from typing import Dict, List
import spacy


class SpacyPassageChunker:

    def __init__(self, max_passage_size, model='en_core_web_sm'):
        self.max_passage_size = max_passage_size
        self.document_sentences = None
        try:
            self.model = spacy.load(model,
                                    exclude=["parser", "tagger", "ner", "attribute_ruler", "lemmatizer", "tok2vec"])
        except OSError:
            print(f"Downloading spaCy model {model}")
            spacy.cli.download(model)
            print(f"Finished downloading model")
            self.model = spacy.load(model,
                                    exclude=["parser", "tagger", "ner", "attribute_ruler", "lemmatizer", "tok2vec"])
        self.model.enable_pipe("senter")
        self.model.max_length = 1500000000  # for documents that are longer than the spacy character limit

    @staticmethod
    def download_spacy_model(model="en_core_web_sm"):
        print(f"Downloading spaCy model {model}")
        spacy.cli.download(model)
        print(f"Finished downloading model")

    @staticmethod
    def load_model(model="en_core_web_sm"):
        return spacy.load(model, exclude=["parser", "tagger", "ner", "attribute_ruler", "lemmatizer", "tok2vec"])

    def tokenize_document(self, document_body) -> None:
        spacy_document = self.model(document_body)
        self.document_sentences = list(spacy_document.sents)

    def chunk_document(self) -> List[Dict]:

        passages = []
        sentence_count = len(self.document_sentences)
        sentences_word_count = [len([token for token in sentence]) for sentence in self.document_sentences]

        current_idx = 0
        current_passage_word_count = 0
        current_passage = ''
        sub_id = 0

        for i in range(sentence_count):

            # 0.67 is used to control passages that may overflow the max passage size
            if current_passage_word_count >= (self.max_passage_size * 0.67):
                passages.append({
                    "body": current_passage,
                    "id": sub_id
                })

                # reset the current passage to an empty string
                current_passage = ''
                current_passage_word_count = 0

                current_idx = i
                sub_id += 1

            current_passage += self.document_sentences[i].text + ' '
            current_passage_word_count += sentences_word_count[i]

        # append the remaining sentences, if any, to a passage
        current_passage = ' '.join(sentence.text for sentence in self.document_sentences[current_idx:])
        passages.append({
            "body": current_passage,
            "id": sub_id
        })

        return passages
