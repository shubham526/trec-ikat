from tqdm import tqdm
import json
import argparse
import sys
import os
import gzip
import requests

API_URL = "https://rel.cs.ru.nl/api"


def convert(text_dir, outlinks_dir, save_dir):
    text_files = os.listdir(text_dir)
    for file_name in text_files:
        if os.path.splitext(file_name)[1] == '.gz':
            text_file_name = os.path.join(text_dir, file_name)
            outlink_file_name = os.path.join(outlinks_dir, file_name)
            save_file_name = os.path.splitext(file_name)[0].split('.')[0] + '.jsonl'
            save_path = os.path.join(save_dir, save_file_name)
            data = []
            convert_single_file(text_file=text_file_name, outlinks_file=outlink_file_name, data=data)
            print('Converted: {}'.format(file_name))
            print('Saving....')
            write_to_file(data, save_path)
            print('File saved to ==> {}'.format(save_path))


def entity_link(text):
    results = requests.post(API_URL, json={"text": text, "spans": []}).json()
    links = []
    seen = set()
    return [
        {'mention': res[2], 'name': res[3], 'type': res[6]}
        for res in results
    ]


def read_outlink_file(file):
    res = {}
    seen = set()
    with gzip.open(file, 'rt', encoding='UTF-8') as f:
        for line in f:
            if line != '\n':
                d = json.loads(line)
                outlinks = []
                for ol in d['outlinks']:
                    if ol[1] not in seen:
                        seen.add(ol[1])
                        outlinks.append({
                            'url': ol[0],
                            'id': ol[1],
                            'mention': ol[2].replace('\r', ' ').replace('\n', ' ')
                        })
                res[d['ClueWeb22-ID']] = list(outlinks)
    return res


def convert_single_file(text_file, outlinks_file, data):
    outlink_file_dict = read_outlink_file(outlinks_file)
    with gzip.open(text_file, 'rt', encoding='UTF-8') as f:
        for line in tqdm(f):
            d = json.loads(line)
            if d['ClueWeb22-ID'] in outlink_file_dict:
                outlinks = outlink_file_dict[d['ClueWeb22-ID']]
                doc_text = d['Clean-Text'].replace('\r', ' ').replace('\n', ' ')
                rel = entity_link(doc_text)
                if len(rel) != 0:
                    data.append(json.dumps({
                        'id': d['ClueWeb22-ID'],
                        'contents': doc_text,
                        'REL': rel,
                        'outlinks': outlinks
                    }))
            else:
                return


def write_to_file(data, save):
    with open(save, 'w') as f:
        for d in data:
            f.write("%s\n" % d)


def main():
    parser = argparse.ArgumentParser("Convert ClueWeb22 data to Pyserini data format.")
    parser.add_argument("--text", help='Directory containing text of documents.', required=True)
    parser.add_argument("--outlinks", help='Directory containing outlinks of documents.', required=True)
    parser.add_argument("--save", help='Directory where to save.', required=True)
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    convert(args.text, args.outlinks, args.save)


if __name__ == '__main__':
    main()
