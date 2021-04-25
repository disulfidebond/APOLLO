import pandas as pd
import numpy as np

# import report file
df_orig = pd.read_csv('internalList.GooglePlaces.txt', sep='|', header=None)
df_orig.columns = ['Index', 'NER_term', 'NER_type', 'Iterations', 'NER_Confidence', 'Incident_IDs']
"""Pandas does not recognize the tab delimiters for some reason
otherwise read_csv would work
"""
df_dataList = []
headers = []
with open('parsed_matched_mapped.fixed.formatted.txt') as fOpen:
  for i in fOpen:
    i = i.rstrip('\r\n')
    iSplit = i.split('\t')
    if not headers:
      headers = iSplit
      continue
    df_dataList.append(iSplit)
print(df_dataList[0:2])
print(headers)
df_data = pd.DataFrame(df_dataList, columns=headers)
df_merged = df_orig.merge(df_data, left_index=True, right_index=True)
# output as CSV file
df_merged.to_csv('parsed_merged.txt')
