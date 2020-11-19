import argparse
import pickle
import pandas as pd


def readToFile(file_idx):
    if file_idx is not None:
        try:
            next_from_pickle = []
            while True:
                next_from_pickle.append(pickle.load(file_idx))

        except Exception as e:
            df = pd.DataFrame(next_from_pickle, columns=['term_id', 'doc', 'freq'])
            df.to_csv('translated.csv')
            return None


def readToWrite(file_idx):
    if file_idx is not None:
        try:
            next_from_pickle = []
            while True:
                next_from_pickle.append(pickle.load(file_idx))

        except Exception as e:
            df = pd.DataFrame(next_from_pickle, columns=['term_id', 'doc', 'freq'])
            print(df)
            return None


parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", help="File to translate")
parser.add_argument("-o", "--output", help="File to translate")
args = parser.parse_args()

if args.output == "file":
    with open(args.file, 'rb') as old_file:
        readToFile(old_file)
else:
    with open(args.file, 'rb') as old_file:
        readToWrite(old_file)
