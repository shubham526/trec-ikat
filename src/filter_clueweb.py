import json
import argparse
import gzip
import sys
from typing import Dict, Set
from tqdm import tqdm

def filter_collection(id2url: Dict[str, str], url_list: Set[str]):
   return [
        json.dumps({'idx': idx, 'url': url})
        for idx, url in tqdm(id2url.items(), total=len(id2url))
        for u in url_list if u in url
    ]

def read_tsv(id2url: str) -> Dict[str, str]:
    with gzip.open(id2url, 'rt', encoding='UTF-8') as f:
        return dict({
            (line.split('\t')[0], line.split('\t')[1])
            for line in tqdm(f, total=200000000)
        })

def main():
    parser = argparse.ArgumentParser("Filter ClueWeb22 data.")
    parser.add_argument("--id2url", help='File containing Id-->URL mappings.', required=True)
    parser.add_argument('--urls', type=str, help='List of URLs to keep.', required=True)
    parser.add_argument("--save", help='File to save.', required=True)
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    print('Loading Id-->URL mappings..')
    id2url: Dict[str, str] = read_tsv(args.id2url)
    print('[Done].')

    print('Filtering collection based on URLs: {}'.split(args.urls))
    res = filter_collection(id2url=id2url, url_list=args.urls.split())
    print('[Done].')

    print('Writing to file...')
    with open(args.save, 'w') as f:
        for line in res:
            f.write("%s\n" % line)
    print('File written to ==> {}'.format(args.save))



if __name__ == '__main__':
    main()

