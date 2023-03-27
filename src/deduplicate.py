# from tqdm import tqdm
import argparse
import sys
import os
import gzip


def analyze_directory(directory, duplicates, seen):
    data_files = sorted(os.listdir(directory))
    for file_name in data_files:
        if os.path.splitext(file_name)[1] == '.gz':
            data_file_name = os.path.join(directory, file_name)
            analyze_single_file(file=data_file_name, duplicates=duplicates, seen=seen)
            print('Analyzed: {}'.format(data_file_name))


def deduplicate(data_dir, save):
    directories = sorted(os.listdir(data_dir))
    duplicates = set()
    seen = set()

    for directory in directories:
        analyze_directory(directory=os.path.join(data_dir, directory), duplicates=duplicates, seen=seen)

    print('Saving deduplicated data...')
    save_file = save + '/cluweb22.dedup.jsonl'
    write_to_file(seen, save_file)
    print('Data saved to ==> {}'.format(save_file))

    print('Saving duplicates...')
    save_file = save + '/duplicates.jsonl'
    write_to_file(duplicates, save_file)
    print('Data saved to ==> {}'.format(save_file))


def analyze_single_file(file, duplicates, seen):
    with gzip.open(file, 'rt', encoding='UTF-8') as f:
        for line in f:
            if line not in seen:
                seen.add(line)
            else:
                print('Duplicate found.')
                duplicates.add(line)


def write_to_file(data, save):
    with open(save, 'w') as f:
        for d in data:
            f.write("%s\n" % d)


def main():
    parser = argparse.ArgumentParser("Deduplicate the ClueWeb22 corpus.")
    parser.add_argument("--data", help='Directory containing text of documents.', required=True)
    parser.add_argument("--save", help='Directory where to save.', required=True)
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    deduplicate(args.data, args.save)


if __name__ == '__main__':
    main()
