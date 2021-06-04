# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 10:39:35 2021

@author: jrcaskey
"""

from formatted import createFormattedLL
from business import createbusdb
import argparse
from datetime import datetime




ts_string = datetime.now().strftime("%H%M"+"_"+"%m%d%y")
parser = argparse.ArgumentParser()
# business list args
parser.add_argument('--data', type=str, help='input short filename to parse', required=True)
parser.add_argument('--outbreak', type=str, help='input filename for outbreak data', required=True)
# formatted args
defaultColString = 'IncidentID,Address,City,Zip,County,Latitude,Longitude'
parser.add_argument('--cases', type=str, help='input filename for case data', required=True)
parser.add_argument('--cols', type=str, help='comma-separated list of column names to select', nargs='?', const=defaultColString, default=defaultColString)
args = parser.parse_args()
# create formattedLL input file
formattedList = createFormattedLL(args.cases, defaultColString)
formattedLL_filename = 'formattedLL.' + ts_string + '.txt'
formattedList.to_csv(formattedLL_filename, index=False, sep='|')

# create business list file
busList = createbusdb(args.outbreak, args.data)
outFileName = 'busList.' + ts_string + '.txt'

with open(outFileName, 'w') as fWrite:
    for i in busList:
        fWrite.write(i + '\n')
