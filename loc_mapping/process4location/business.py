import pandas as pd
import re
import numpy as np
from datetime import datetime

ts_string = datetime.now().strftime("%H%M"+"_"+"%m%d%y")

def createbusdb(outbreakFile, shortFile):
    # import outbreak text file, drop any missing entries, only select unique entries
    df_outbreaks = pd.read_csv(outbreakFile, delimiter="|", encoding="latin1")
    df_outbreaks = df_outbreaks[df_outbreaks['OutbreakLocation'].notna()].copy()
    df_outbreaks = df_outbreaks.reset_index(drop=True)
    df_outbreaks = df_outbreaks.fillna("UNKNOWN")
    df_filtered = df_outbreaks.loc[:, ['OutbreakLocation','OutbreakLocationAddress','OutbreakLocationJurisdiction']]
    df_dedup = df_filtered.drop_duplicates(subset=['OutbreakLocation'])
    
    # drop entries that are likely residential addresses
    oloc_list = df_dedup['OutbreakLocation'].tolist()
    dropIndex = []
    for idx,itm in enumerate(oloc_list):
      m1 = re.match('\d+', itm)
      m2 = re.match('\s\d+', itm)
      m3 = re.search(', WI', itm)
      if m1:
        dropIndex.append(idx)
      elif m2:
        dropIndex.append(idx)
      elif m3:
        dropIndex.append(idx)
      else:
        pass
    df_dedup = df_dedup.drop(df_dedup.index[dropIndex])
    df_dedup = df_dedup.reset_index(drop=True)
    
    df_dedup_Loc = df_dedup['OutbreakLocation'].tolist()
    df_dedup_Add = df_dedup['OutbreakLocationAddress'].tolist()
    df_dedup_Cty = df_dedup['OutbreakLocationJurisdiction'].tolist()
    
    outbreakAddressList = []
    
    for i in range(len(df_dedup_Loc)):
      locString = df_dedup_Loc[i]
      addressString = df_dedup_Add[i]
      ctyString = df_dedup_Cty[i]
      s = locString + ', ' + addressString + ', ' + ctyString
      outbreakAddressList.append(s)

    # import ShortFields notes file and parse similar to above
    df_pt = pd.read_csv(shortFile, sep="|", encoding="iso-8859-1", error_bad_lines=False, warn_bad_lines=False)

    # verified this actually is a good filter for the relevant columns
    col_list = [[0], [105,117],[127,129],[41],[45],[52],[30,32],[203,218]]
    cols2Select = []
    for i in col_list:
      if len(i) > 1:
        cols2Select.extend([x for x in range(i[0], i[1]+1)])
      else:
        cols2Select.extend(i)
    
    df_pt_filtered = df_pt.iloc[:,cols2Select]
    df_pt_filtered = df_pt_filtered.fillna(-1)
    listValues = df_pt_filtered.values.tolist()
    list_output = []
    for i in listValues:
      v = list(filter(lambda x: x != -1,i))
      list_output.append(v)
    
    # parse the file into a list of [incidentID, [addressList]]
    shortFieldsNotes = []
    shortFieldsNotes_output = []
    # prev = None
    for i in list_output:
      if not i:
        continue
      if re.search(r'[A-Za-z]+', str(i[0])):
        continue
      if re.search(r'[-|/]', str(i[0])):
        continue
      if len(i) != 1:
        s_list = [str(x) for x in i]
        s_incident = ''
        try:
          s_incident = s_list[0]
          s_incident = float(s_incident)
          s_incident = str(s_incident)
        except IndexError:
          print('IndexError parsing line')
          print(i)
          continue
        except ValueError:
          print('ValueError parsing line')
          print(i)
          continue
        s_data = s_list[1:]
        shortFieldsNotes.append((s_incident, s_data))
        s = '|'.join(s_data)
        s = s_incident + ',' + s
        shortFieldsNotes_output.append(s)
    
    # optional code block: write to output file
    '''
    outFileName = 'shortFields.addresses.' + ts_string + '.txt'
    with open(outFileName, 'w') as fWrite:
      for i in shortFieldsNotes_output:
        fWrite.write(i + '\n')
    '''
    # end optional code block
    
    def checkText(s):
      m = re.match('\d+', s)
      m1 = re.match('\s\d+', s)
      if m:
        return False
      elif m1:
        return False
      else:
        return True
    
    shortFieldsNotes_notesOnly = []
    for i in shortFieldsNotes:
      shortFieldsNotes_notesOnly.append(i[1])
    
    unsorted_results = []
    for i in shortFieldsNotes_notesOnly:
      if not i:
        continue
      if len(i) == 1:
        b = checkText(i[0])
        if b:
          unsorted_results.append(i[0])
      else:
        for val in i:
          b = checkText(val)
          if b:
            unsorted_results.append(val)
    
    unsorted_merged_results = unsorted_results + outbreakAddressList
    '''
    outFileName = 'parsed_addresses.' + ts_string + '.txt'

    with open(outFileName, 'w') as fWrite:
      for i in unsorted_merged_results:
        fWrite.write(i + '\n')
    '''
    return unsorted_merged_results
