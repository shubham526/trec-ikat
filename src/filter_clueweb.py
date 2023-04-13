import json
import argparse
import os
import gzip
import sys

def filter_collection(data_dir):
    directories = sorted(os.listdir(data_dir))
    res = []
    for directory in directories:
        print('Directory: {}'.format(directory))
        directory_path = os.path.join(data_dir, directory)
        data_files = sorted(os.listdir(directory_path))
        for file_name in data_files:
            if os.path.splitext(file_name)[1] == '.gz':
                file_path = os.path.join(directory_path, file_name)
                with gzip.open(file_path, 'rt', encoding='UTF-8') as f:
                    for line in f:
                        d = json.loads(line)
                        if 'https://en.wikipedia.org/' in d['URL']:
                            # print('Found Wiki page: {}'.format(d['URL']))
                            res.append(json.dumps({'idx': d['ClueWeb22-ID'], 'url': d['URL']}))
                    print('Done: {}'.format(file_name))
        print('=======================================================')
    return res

def main():
    parser = argparse.ArgumentParser("Filter ClueWeb22 data.")
    parser.add_argument("--data", help='Directory containing documents.', required=True)
    parser.add_argument("--save", help='File to save.', required=True)
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    print('Filtering collection...')
    res = filter_collection(data_dir=args.data)
    print('[Done].')

    print('Writing to file...')
    with open(args.save, 'w') as f:
        for line in res:
            f.write("%s\n" % line)



if __name__ == '__main__':
    main()

