from nltk.stem.snowball import SnowballStemmer
from bs4 import BeautifulSoup
import string
from nltk.tokenize import word_tokenize
import os


class Cleaner:
    def __init__(self, stop_words_file: str, language: str,
                 perform_stop_words_removal: bool, perform_accents_removal: bool,
                 perform_stemming: bool):
        self.set_stop_words = self.read_stop_words(stop_words_file)

        in_table = "áéíóúâêôçãẽõü"
        out_table = "aeiouaeocaeou"
        # altere a linha abaixo para remoção de acentos (Atividade 11)
        self.accents_translation_table = str.maketrans(in_table, out_table)

        self.set_punctuation = set(string.punctuation)

        # flags
        self.perform_stop_words_removal = perform_stop_words_removal
        self.perform_accents_removal = perform_accents_removal
        self.perform_stemming = perform_stemming

    def html_to_plain_text(self, html_doc: str) -> str:
        return BeautifulSoup(html_doc, 'html.parser').get_text()

    def read_stop_words(self, str_file):
        set_stop_words = set()
        with open(str_file, "r") as stop_words_file:
            for line in stop_words_file:
                arr_words = line.split(",")
                [set_stop_words.add(word.replace('\n', '').replace('\'', '').replace(' ', '')) for word in arr_words]
        return set_stop_words

    def is_stop_word(self, term: str):
        return True if term in self.set_stop_words else False

    def word_stem(self, term: str):
        return SnowballStemmer("portuguese").stem(term)

    def remove_accents(self, term: str) -> str:
        return term.translate(self.accents_translation_table)

    def preprocess_word(self, term: str) -> str:
        word = term
        if self.is_stop_word(term) and self.perform_stop_words_removal:
            return None
        if self.perform_accents_removal:
            word = self.remove_accents(term)
        if self.perform_stemming:
            word = self.word_stem(word)
        return word


class HTMLIndexer:
    cleaner = Cleaner(stop_words_file="stopwords.txt",
                      language="portuguese",
                      perform_stop_words_removal=True,
                      perform_accents_removal=True,
                      perform_stemming=True)

    def __init__(self, index):
        self.index = index

    def text_word_count(self, plain_text: str):
        dic_word_count = {}
        for word in word_tokenize(plain_text):
            preprocessed_word = self.cleaner.preprocess_word(word)
            if preprocessed_word and preprocessed_word not in ["?","!",".",",",";",":"]:
                dic_word_count[preprocessed_word] = dic_word_count.get(preprocessed_word, 0) + 1
        return dic_word_count

    def index_text(self, doc_id: int, text_html: str):
        text = self.cleaner.html_to_plain_text(text_html)
        word_count = self.text_word_count(text)

        for k, v in word_count.items():
            self.index.index(k, doc_id, v)

    def index_text_dir(self, path: str):
        for str_sub_dir in os.listdir(path):
            path_sub_dir = f"{path}/{str_sub_dir}"
            for filename in os.listdir(path_sub_dir):
                if filename.endswith(".html"):
                    with open(f"{path_sub_dir}/{filename}") as file:
                        doc_id = int(filename[:-5])
                        self.index_text(doc_id, file)