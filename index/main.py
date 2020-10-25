from index.structure import *
import pickle


def next(self, file_idx) -> TermOccurrence:
 bytes_doc_id = file_idx.read(4)
 if not bytes_doc_id:
  return None
 try:
  next_from_file = pickle.load(file_idx)
  return self.lst_occurrences_tmp.pop()
 except Exception as e:
  return None
t= [TermOccurrence(1,1,1),
 TermOccurrence(2,1,1),
 TermOccurrence(1,2,2),
 TermOccurrence(2,2,1),
 TermOccurrence(1,3,1),
 TermOccurrence(3,3,1),
 TermOccurrence(1,4,1),
 TermOccurrence(1,5,1),
 TermOccurrence(1,6,1),
 TermOccurrence(2,6,1),
 TermOccurrence(3,7,1),
 TermOccurrence(3,8,1)]


def storeData():
 # initializing data to be stored in db
 Omkar = {'key': 'Omkar', 'name': 'Omkar Pathak',
          'age': 21, 'pay': 40000}
 Jagdish = {'key': 'Jagdish', 'name': 'Jagdish Pathak',
            'age': 50, 'pay': 50000}

 # database
 db1 = {}
 for i in t:
  db = []
  db.append(i.term_id)
  db.append(i.doc_id)
  db.append(i.term_freq)
  dbfile = open('examplePickle', 'ab')
  pickle.dump(db, dbfile)
 dbfile.close()


 # Its important to use binary mode



def loadData():
 # for reading also binary mode is important
 dbfile = open('examplePickle', 'rb')
 db = pickle.load(dbfile)
 for keys in db:
  print(keys, '=>', db[keys])
 dbfile.close()


if __name__ == '__main__':
 storeData()
 loadData()