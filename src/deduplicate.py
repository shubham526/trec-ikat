from tqdm import tqdm
import json
import argparse
import sys
import os
import gzip


def deduplicate(data_dir, save):
    data_files = os.listdir(data_dir)
    duplicates = set()
    seen = set()

    for file_name in data_files:
        if os.path.splitext(file_name)[1] == '.gz':
            data_file_name = os.path.join(data_dir, file_name)
            analyze_single_file(file=data_file_name, duplicates=duplicates, seen=seen)
            print('Analyzed: {}'.format(data_file_name))
    print('Saving....')
    write_to_file(seen, save)
    print('File saved to ==> {}'.format(save))


def analyze_single_file(file, duplicates, seen):
    with gzip.open(file, 'rt', encoding='UTF-8') as f:
        for line in tqdm(f):
            if line not in seen:
                seen.add(line)
            else:
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
