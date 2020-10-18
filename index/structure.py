from IPython.display import clear_output
from typing import List, Set, Union
from abc import abstractmethod
from functools import total_ordering
from os import path
import os
import pickle
import gc


class Index:
    def __init__(self):
        self.dic_index = {}
        self.set_documents = set()

    def index(self, term: str, doc_id: int, term_freq: int):
        if term not in self.dic_index:
            int_term_id = len(self.set_documents) + 1
            self.set_documents.add(int_term_id)
            self.dic_index[term] = self.create_index_entry(int_term_id)
        else:
            int_term_id = self.get_term_id(term)

        self.add_index_occur(
            self.dic_index[term], doc_id, int_term_id, term_freq)

    @property
    def vocabulary(self) -> List:
        return [term for term in self.dic_index]

    @property
    def document_count(self) -> int:
        return len(self.set_documents)

    @abstractmethod
    def get_term_id(self, term: str):
        raise NotImplementedError(
            "Voce deve criar uma subclasse e a mesma deve sobrepor este método")

    @abstractmethod
    def create_index_entry(self, termo_id: int):
        raise NotImplementedError(
            "Voce deve criar uma subclasse e a mesma deve sobrepor este método")

    @abstractmethod
    def add_index_occur(self, entry_dic_index, doc_id: int, term_id: int, freq_termo: int):
        raise NotImplementedError(
            "Voce deve criar uma subclasse e a mesma deve sobrepor este método")

    @abstractmethod
    def get_occurrence_list(self, term: str) -> List:
        raise NotImplementedError(
            "Voce deve criar uma subclasse e a mesma deve sobrepor este método")

    @abstractmethod
    def document_count_with_term(self, term: str) -> int:
        raise NotImplementedError(
            "Voce deve criar uma subclasse e a mesma deve sobrepor este método")

    def finish_indexing(self):
        pass

    def __str__(self):
        arr_index = []
        for str_term in self.vocabulary:
            arr_index.append(
                f"{str_term} -> {self.get_occurrence_list(str_term)}")

        return "\n".join(arr_index)

    def __repr__(self):
        return str(self)


@total_ordering
class TermOccurrence:
    def __init__(self, doc_id: int, term_id: int, term_freq: int):
        self.doc_id = doc_id
        self.term_id = term_id
        self.term_freq = term_freq

    def write(self, idx_file):
        pass

    def __hash__(self):
        return hash((self.doc_id, self.term_id))

    def __eq__(self, other_occurrence: "TermOccurrence"):
        return False

    def __lt__(self, other_occurrence: "TermOccurrence"):
        return False

    def __str__(self):
        return f"(term_id:{self.term_id} doc: {self.doc_id} freq: {self.term_freq})"

    def __repr__(self):
        return str(self)


# HashIndex é subclasse de Index
class HashIndex(Index):
    def get_term_id(self, term: str):
        return self.dic_index[term][0].term_id

    def create_index_entry(self, term_id: int) -> List:
        return []

    def add_index_occur(self, entry_dic_index: List[TermOccurrence], doc_id: int, term_id: int, term_freq: int):
        entry_dic_index.append(TermOccurrence(doc_id, term_id, term_freq))

    def get_occurrence_list(self, term: str) -> List:
        return self.dic_index[term] if term in self.dic_index else []

    def document_count_with_term(self, term: str) -> int:
        return len(self.dic_index[term]) if term in self.dic_index else 0


class TermFilePosition:
    def __init__(self, term_id: int,  term_file_start_pos: int = None, doc_count_with_term: int = None):
        self.term_id = term_id

        # a serem definidos após a indexação
        self.term_file_start_pos = term_file_start_pos
        self.doc_count_with_term = doc_count_with_term

    def __str__(self):
        return f"term_id: {self.term_id}, doc_count_with_term: {self.doc_count_with_term}, term_file_start_pos: {self.term_file_start_pos}"

    def __repr__(self):
        return str(self)


class FileIndex(Index):

    TMP_OCCURRENCES_LIMIT = 1000000

    def __init__(self):
        super().__init__()

        self.lst_occurrences_tmp = []
        self.idx_file_counter = 0
        self.str_idx_file_name = "occur_idx_file"

    def get_term_id(self, term: str):
        return self.dic_index[term].term_id

    def create_index_entry(self, term_id: int) -> TermFilePosition:
        return TermFilePosition(term_id)

    def add_index_occur(self, entry_dic_index: TermFilePosition,  doc_id: int, term_id: int, term_freq: int):
        self.lst_occurrences_tmp.append(
            TermOccurrence(doc_id, term_id, term_freq))

        if len(self.lst_occurrences_tmp) >= FileIndex.TMP_OCCURRENCES_LIMIT:
            self.save_tmp_occurrences()

    def next_from_list(self) -> TermOccurrence:
        return None

    def next_from_file(self, file_idx) -> TermOccurrence:
            #next_from_file = pickle.load(file_idx)
        bytes_doc_id = file_idx.read(4)
        if not bytes_doc_id:
            return None
        # seu código aqui :)

        return TermOccurrence(doc_id, term_id, term_freq)

    def save_tmp_occurrences(self):

        # ordena pelo term_id, doc_id
        # Para eficiencia, todo o codigo deve ser feito com o garbage
        # collector desabilitado
        gc.disable()

        # ordena pelo term_id, doc_id

        # Abra um arquivo novo faça a ordenação externa: compar sempre a primeira posição
        # da lista com a primeira possição do arquivo usando os métodos next_from_list e next_from_file
        # para armazenar no novo indice ordenado

        gc.enable()

    def finish_indexing(self):
        if len(self.lst_occurrences_tmp) > 0:
            self.save_tmp_occurrences()

        # Sugestão: faça a navegação e obetenha um mapeamento
        # id_termo -> obj_termo armazene-o em dic_ids_por_termo
        dic_ids_por_termo = {}
        for str_term, obj_term in self.dic_index.items():
            pass

        with open(self.str_idx_file_name, 'rb') as idx_file:
            # navega nas ocorrencias para atualizar cada termo em dic_ids_por_termo
            # apropriadamente
            pass

    def get_occurrence_list(self, term: str) -> List:
        return []

    def document_count_with_term(self, term: str) -> int:
        return 0
