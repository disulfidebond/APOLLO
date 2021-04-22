import pandas as pd
import numpy as np
import time
import sys
import collections
import random
from datetime import datetime
from sklearn.cluster import KMeans
import random
import argparse
import math

ts_string = datetime.now().strftime("%H%M"+"_"+"%m%d%y")

parser = argparse.ArgumentParser()


"""
This step selects out IncidentID and Lat/Lon, but I added
address and county fields. It effectively creates the formattedLL file,
although an additional step is needed to select out only IncidentID and Lat/Lon.
"""

# cols are
# IncidentID|Age|Sex|Race|Ethnicity|Address|City|Zip|County|CensusBlock|CensusTract|Latitude|Longitude|
# input file is NLP_Patient file
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

df = pd.read_csv(args.data, encoding="latin1", sep=delim, error_bad_lines=False)
df_filtered = df.loc[:,parseList]
df_filtered.to_csv(fileName, sep='|', index=False)

filterFileName = 'filterFile.formattedLL.' + ts_string + '.txt'
df_filtered = df.loc[:,['IncidentID', 'Latitude', 'Longitude']]
df_filtered.to_csv(filteredFileName, sep='|', index=False)
