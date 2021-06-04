import pandas as pd
import numpy as np
from datetime import datetime


ts_string = datetime.now().strftime("%H%M"+"_"+"%m%d%y")


'''
select fields from NLP_Patients File
'''
def createFormattedLL(dFile, pList):
    parseList = pList.split(',')
    df = pd.read_csv(dFile, encoding="latin1", sep="|", error_bad_lines=False)
    df_filtered = df.loc[:,parseList]
    df_filtered = df_filtered.dropna(subset=['Address'])
    return df_filtered
