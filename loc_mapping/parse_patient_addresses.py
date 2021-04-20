import pandas as pd
import numpy as np
import argparse
from datetime import datetime

ts_string = datetime.now().strftime("%H%M"+"_"+"%m%d%y")

parser = argparse.ArgumentParser()

parser.add_argument('--data', type=str, help='input filename to parse', required=True)
parser.add_argument('--delimiter', type=str, help='delimiter for columns', required=True)
parser.add_argument('--cols', type=str, help='comma-separated list of column names to select', required=True)
parser.add_argument('--suffix', type=str, help='filename suffix that will be appended to the output file name')

args = parser.parse_args()

delim = args.delimiter
parseList = []
if args.cols:
  parseList = args.cols.split(',')

fileName = str(args.data) + '.' + 'parsed.txt'
if args.suffix:
  fileName = str(args.data) + '.' + args.suffix

# use latin1 encoding because that seems to work, and skip lines with errors
df = pd.read_csv(args.data, encoding="latin1", sep=delim, error_bad_lines=False)
df_filtered = df.loc[:,parseList]
df_filtered.to_csv(fileName, sep='|', index=False)
