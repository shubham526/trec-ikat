from http.server import HTTPServer

from REL.entity_disambiguation import EntityDisambiguation
from REL.ner import Cmns, load_flair_ner
from REL.server import make_handler


def main():
    wiki_version = "wiki_2019"

    config = {
        "mode": "eval",
        "model_path": "path/to/model",  # or alias, see also tutorial 7: custom models
    }

    model = EntityDisambiguation(base_url, wiki_version, config)

    # Using Flair:
    tagger_ner = load_flair_ner("ner-fast")

    # Alternatively, using n-grams:
    # tagger_ngram = Cmns(base_url, wiki_version, n=5)

    server_address = ("127.0.0.1", 1235)
    server = HTTPServer(
        server_address,
        make_handler(
            base_url, wiki_version, model, tagger_ner
        ),
    )

    try:
        print("Ready for listening.")
        server.serve_forever()
    except KeyboardInterrupt:
        exit(0)


if __name__ == '__main__':
    main()
