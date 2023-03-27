import json
import argparse
import sys
import os
import gzip
from spacy_passage_chunker import SpacyPassageChunker


def convert(data_dir, save_dir, passage_chunker):
    data_files = os.listdir(data_dir)
    for file_name in data_files:
        if os.path.splitext(file_name)[1] == '.gz':
            data_file_name = os.path.join(data_dir, file_name)
            save_file_name = os.path.splitext(file_name)[0].split('.')[0] + '.trecweb'
            save_path = os.path.join(save_dir, save_file_name)
            data = []
            convert_single_file(file=data_file_name, data=data, passage_chunker=passage_chunker)
            print('Converted: {}'.format(file_name))
            print('Saving....')
            write_to_file(data, save_path)
            print('File saved to ==> {}'.format(save_path))


def add_passage_ids(passages) -> str:
    passage_splits = []

    for passage in passages:
        passage_splits.append('<PASSAGE id={}>'.format(passage["id"]))
        passage_splits.append(passage["body"])
        passage_splits.append('</PASSAGE>')

    return '\n'.join(passage_splits)


def create_trecweb_entry(idx: str, url: str, title: str, body: str) -> str:

    content = '<DOC>\n'
    content += '<DOCNO>'
    content += idx
    content += '</DOCNO>\n'
    content += '<DOCHDR>\n'
    content += '</DOCHDR>\n'
    content += '<HTML>\n'
    content += '<TITLE>'
    content += title
    content += '</TITLE>\n'
    content += '<URL>'
    content += url
    content += '</URL>\n'
    content += '<BODY>\n'
    content += body
    content += '</BODY>\n'
    content += '</HTML>\n'
    content += '</DOC>'

    return content


def convert_single_file(file, data, passage_chunker):
    with gzip.open(file, 'rt', encoding='UTF-8') as f:
        for line in f:
            d = json.loads(line)
            doc_text = d['Clean-Text'].replace('\r', ' ').replace('\n', ' ')
            passage_chunker.tokenize_document(doc_text)
            passages = passage_chunker.chunk_document()
            passage_splits = add_passage_ids(passages)
            trecweb_entry = create_trecweb_entry(
                idx=d['ClueWeb22-ID'],
                url=d['URL'],
                title=d['URL-hash'],
                body=passage_splits
            )
            data.append(trecweb_entry)


def write_to_file(data, save):
    with open(save, 'w') as f:
        for d in data:
            f.write("%s\n" % d)


def main():
    parser = argparse.ArgumentParser("Convert ClueWeb22 data to TrecWeb data format.")
    parser.add_argument("--data", help='Directory containing documents.', required=True)
    parser.add_argument("--max-len", help='Maximum size of passage.', default=250, type=int)
    parser.add_argument("--save", help='Directory where to save.', required=True)
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    passage_chunker = SpacyPassageChunker(args.max_len)
    convert(args.data, args.save, passage_chunker)


if __name__ == '__main__':
    main()
