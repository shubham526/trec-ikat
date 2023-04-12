import json
import argparse
import os
import gzip
import sys
from multiprocessing import Process, Manager
from spacy_passage_chunker import SpacyPassageChunker

meta_data = {
    'clueweb': {
        'idx': 'ClueWeb22-ID',
        'body': 'Clean-Text',
        'url': 'URL',
        'title': 'URL-hash'
    },
    'marco': {
        'idx': 'docid',
        'body': 'body',
        'url': 'url',
        'title': 'title'
    }
}


def convert(collection_name, data_dir, save_dir, num_processes, max_len, stride):
    data_files = sorted(os.listdir(data_dir))
    file_queue = Manager().Queue()
    save_queue = Manager().Queue()

    for file_name in data_files:
        if os.path.splitext(file_name)[1] == '.gz':
            file_path = os.path.join(data_dir, file_name)
            save_path = os.path.join(save_dir, os.path.splitext(file_name)[0].split('.')[0] + '.trecweb')
            file_queue.put((file_path, save_path))

    workers = []
    for i in range(num_processes):
        p = Process(target=worker, args=(file_queue, save_queue, max_len, stride, collection_name))
        p.start()
        workers.append(p)

    for p in workers:
        p.join()

    while not save_queue.empty():
        data = save_queue.get()
        save_file(data['data'], data['save_path'])
        print('File saved to ==> {}'.format(data['save_path']))


def worker(file_queue, save_queue, max_len, stride, collection_name):
    passage_chunker = SpacyPassageChunker(max_len, stride)

    while not file_queue.empty():
        file_path, save_path = file_queue.get()
        data = []
        with gzip.open(file_path, 'rt', encoding='UTF-8') as f:
            for line in f:
                d = json.loads(line)
                doc_text = d[meta_data[collection_name]['body']].replace('\r', ' ').replace('\n', ' ')
                # doc_text = d['Clean-Text'].replace('\r', ' ').replace('\n', ' ')
                # doc_text = d['body'].replace('\r', ' ').replace('\n', ' ')
                passage_chunker.tokenize_document(doc_text)
                passages = passage_chunker.chunk_document()
                passage_splits = add_passage_ids(passages)
                # trecweb_entry = create_trecweb_entry(
                #     idx=d['ClueWeb22-ID'],
                #     url=d['URL'],
                #     title=d['URL-hash'],
                #     body=passage_splits
                # )
                trecweb_entry = create_trecweb_entry(
                    idx=d[meta_data[collection_name]['idx']],
                    url=d[meta_data[collection_name]['url']],
                    title=d[meta_data[collection_name]['title']],
                    body=passage_splits
                )
                data.append(trecweb_entry)

        save_queue.put({'data': data, 'save_path': save_path})
        print('Converted: {}'.format(file_path))


def add_passage_ids(passages) -> str:
    passage_splits = []

    for idx, passage in enumerate(passages):
        passage_splits.append('<PASSAGE id={}>'.format(idx))
        passage_splits.append(passage)
        passage_splits.append('</PASSAGE>')

    return '\n'.join(passage_splits)


def create_trecweb_entry(idx: str, url: str, title: str, body: str) -> str:
    return ''.join([
        '<DOC>\n',
        f'<DOCNO>{idx}</DOCNO>\n',
        '<DOCHDR>\n</DOCHDR>\n',
        '<HTML>\n',
        f'<TITLE>{title}</TITLE>\n',
        f'<URL>{url}</URL>\n',
        '<BODY>\n',
        body,
        '</BODY>\n',
        '</HTML>\n',
        '</DOC>'
    ])

def save_file(data, save_path):
    with open(save_path, 'w') as f:
        for d in data:
            f.write("%s\n" % d)


def main():
    parser = argparse.ArgumentParser("Convert ClueWeb22 data to TrecWeb data format.")
    parser.add_argument("--collection", help='Name of collection (clueweb|marco).', required=True)
    parser.add_argument("--data", help='Directory containing documents.', required=True)
    parser.add_argument("--save-dir", help='Directory to save TrecWeb files.', required=True)
    parser.add_argument("--num-workers", help='Number of workers to use for conversion. Default=4', type=int, default=4)
    parser.add_argument('--max-len', default=10, help='Maximum sentence length per passage. Default=10')
    parser.add_argument('--stride', default=5,
                        help='Distance between each beginning sentence of passage in a document. Default=5')
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    print('Corpus: {}'.format(args.collection))
    print('Number of workers = {}'.format(args.num_workers))
    print('UMaximum sentence length per passage = {}'.format(args.max_len))
    print('Distance between each beginning sentence of passage in a document = {}'.format(args.stride))

    convert(args.collection, args.data, args.save_dir, args.num_workers, args.max_len, args.stride)


if __name__ == '__main__':
    main()
