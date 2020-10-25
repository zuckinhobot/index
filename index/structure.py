from IPython.display import clear_output
from typing import List, Set, Union
from abc import abstractmethod
from functools import total_ordering
import random
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
            self.dic_index[term] = self.create_index_entry(int_term_id)
        else:
            int_term_id = self.get_term_id(term)
        if not doc_id in self.set_documents:
            self.set_documents.add(doc_id)
        self.add_index_occur(
            self.dic_index[term], doc_id, int_term_id, term_freq)

    @property
    def vocabulary(self) -> List:
        return self.dic_index.keys()

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
        db = [self.term_id, self.doc_id, self.term_freq]
        pickle.dump(db, idx_file)

    def __hash__(self):
        return hash((self.term_id, self.term_id))

    def __eq__(self, other_occurrence: "TermOccurrence"):
        if not type(other_occurrence) == TermOccurrence:
            return False
        return ((self.term_id, self.doc_id) ==
                (other_occurrence.term_id, other_occurrence.doc_id))

    def __lt__(self, other_occurrence: "TermOccurrence"):
        if not type(other_occurrence) == TermOccurrence:
            return False
        return ((self.term_id, self.doc_id) <
                (other_occurrence.term_id, other_occurrence.doc_id))

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
    def __init__(self, term_id: int, term_file_start_pos: int = None, doc_count_with_term: int = None):
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
        self.lst_occurrences_tmp_file = []
        self.idx_file_counter = 0
        self.str_idx_file_name = None

    def get_term_id(self, term: str):
        return self.dic_index[term].term_id

    def create_index_entry(self, term_id: int) -> TermFilePosition:
        return TermFilePosition(term_id)

    def add_index_occur(self, entry_dic_index: TermFilePosition, doc_id: int, term_id: int, term_freq: int):
        self.lst_occurrences_tmp.append(
            TermOccurrence(doc_id, term_id, term_freq))

        if len(self.lst_occurrences_tmp) >= FileIndex.TMP_OCCURRENCES_LIMIT:
            self.save_tmp_occurrences()

    def next_from_list(self) -> TermOccurrence:
        try:
            return self.lst_occurrences_tmp.pop(0)
        except Exception as e:
            print(f"Excecao ao pegar proximo da lista em next_from_list: {e}")
            return None

    def next_from_file(self, file_idx=None) -> TermOccurrence:
        if not self.lst_occurrences_tmp_file and file_idx is not None:
            try:
                self.lst_occurrences_tmp_file = pickle.load(file_idx)
            except Exception as e:
                print(f"Excecao ao abir arquivo em next_from_file: {e}")
                return None

        try:
            return self.lst_occurrences_tmp_file.pop(0)
        except Exception as e:
            print(f"Excecao ao pegar proximo da lista em next_from_file: {e}")
            return None

    def save_tmp_occurrences(self):
        # Para eficiencia, todo o codigo deve ser feito com o garbage
        # collector desabilitado
        gc.disable()
        # ordena pelo term_id, doc_id
        self.lst_occurrences_tmp = sorted(self.lst_occurrences_tmp)
        new_file_name = 'occur_index_{counter}.idx'.format(
            counter=self.idx_file_counter)

        if self.idx_file_counter > 0:
            memory_list = []
            next_from_list = self.next_from_list()
            while next_from_list is not None:
                memory_list.append(next_from_list)
                next_from_list = self.next_from_list()

            file_list = []
            old_file_name = self.str_idx_file_name
            with open(old_file_name, 'rb') as old_file:
                next_from_file = self.next_from_file(old_file)
                while next_from_file is not None:
                    file_list.append(next_from_file)
                    next_from_file = self.next_from_file(old_file)

            self.lst_occurrences_tmp = sorted(file_list + memory_list)

        with open(new_file_name, 'wb') as idx_file:
            pickle.dump(self.lst_occurrences_tmp, idx_file)

        self.str_idx_file_name = new_file_name
        self.lst_occurrences_tmp = []
        self.lst_occurrences_tmp_file = []
        self.idx_file_counter = self.idx_file_counter + 1

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
