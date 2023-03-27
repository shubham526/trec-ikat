#from tqdm import tqdm
import argparse
import sys
import os
import gzip
import json

# Set the size of each chunk to read from the input file
CHUNK_SIZE = 1024 * 1024 * 100  # 100 MB



def analyze_directory(directory, line_counts):
    data_files = sorted(os.listdir(directory))
    for file_name in data_files:
        if os.path.splitext(file_name)[1] == '.gz':
            data_file_name = os.path.join(directory, file_name)
            analyze_single_file(file=data_file_name, line_counts=line_counts)
            print('Analyzed: {}'.format(data_file_name))


def deduplicate(data_dir, save):
    directories = sorted(os.listdir(data_dir))
    # Define a dictionary to store the line hashes and counts
    line_counts = {}

    for directory in directories:
        analyze_directory(directory=os.path.join(data_dir, directory), line_counts=line_counts)

    print('Saving duplicates...')
    save_file = save + '/duplicates.jsonl'
    write_to_file(line_counts, save_file)
    print('Data saved to ==> {}'.format(save_file))


def analyze_single_file(file, line_counts):
    # Open the input file for reading in binary mode
    with gzip.open(file, 'rt', encoding='UTF-8') as f:
        for line in f:
            line_hash = json.loads(line)['URL-hash']
            # Increment the count for this line hash
            line_counts[line_hash] = line_counts.get(line_hash, 0) + 1


def write_to_file(line_counts, save):
    # Open the output file for writing in binary mode
    with open(save, 'wb') as f:
        for line_hash, count in line_counts.items():
            if line_counts.get(line_hash, 0) > 1:
                f.write("%s\n" % line_hash)


def main():
    parser = argparse.ArgumentParser("Deduplicate the ClueWeb22 corpus.")
    parser.add_argument("--data", help='Directory containing text of documents.', required=True)
    parser.add_argument("--save", help='Directory where to save.', required=True)
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    deduplicate(args.data, args.save)


if __name__ == '__main__':
    main()
