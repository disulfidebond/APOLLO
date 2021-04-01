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

parser.add_argument('--data', type=str, help='input filename for weekly data', required=True)
parser.add_argument('--patients', type=str, help='filename for formatted patient lat/lon coordinates', required=True)
args = parser.parse_args()


nameCol = []
typeCol = []
iterationsCol = []
scoreCol = []
incidentsCol = []
outbreakCol = []
rowCt = 0
with open(args.data) as fOpen:
  header = True
  for i in fOpen:
    rowCt += 1
    i = i.strip('\r\n')
    iSplit = i.split(',')
    nameCol.append(iSplit[0])
    typeCol.append(iSplit[1])
    iterationsCol.append(iSplit[2])
    scoreCol.append(iSplit[3])
    if header:
      header = False
      incidentsCol.append([])
      outbreakCol.append([])
      continue
    iAppend = iSplit[4:]
    iAppend = [x.replace(' ','') for x in iAppend]
    iAppend = [x.replace("'","") for x in iAppend]
    iAppend = [x.replace('"', '') for x in iAppend]
    iAppend = [x.replace('[', '') for x in iAppend]
    iAppend = [x.replace(']', '') for x in iAppend]
    outbreakID_list = []
    try:
      outbreakIDs = list(filter(lambda x: x.startswith('2020-'), iAppend))
      trimIdx = iAppend.index(outbreakIDs[0])
      iAppend = iAppend[:trimIdx]
      outbreakID_list = iAppend[trimIdx:]
    except IndexError:
      pass
    incidentsCol.append(iAppend)
    outbreakCol.append(outbreakID_list)

# codeblock 1: look for duplicated entries
# TODO: create additional filter to select types
# then add to vocab search list 
dup_items = []
itemsList = []
for i in range(1, len(incidentsCol)):
  if i == 1:
    itemsList.append(incidentsCol[i])
  else:
    if incidentsCol[i] not in itemsList:
      itemsList.append(incidentsCol[i])
    else:
      d = [x for x in range(len(incidentsCol)) if incidentsCol[x] in [incidentsCol[i]]]
      dup_items.extend(d)
dup_items = list(set(dup_items))
dup_items.sort()
flagList1 = []
flagList_incidents = []
for i in dup_items:
  flagList1.append((incidentsCol[i], typeCol[i], nameCol[i]))
  flagList_incidents.append(incidentsCol[i])

flagList1_parsed = []
flagList1_ids = []
for i in flagList1:
  iList = i[0]
  if len(iList[-1]) == 0:
    iList = iList[:-1]
  iListStr = '_'.join(iList)
  flagList1_parsed.append((iListStr, i[1], i[2]))
  flagList1_ids.append(iListStr)
flagList1_ids = list(set(flagList1_ids))

duplicatedIds = []
duplicatedId_names = []
for i in flagList1_ids:
  res = list(filter(lambda x: x[0] == i, flagList1_parsed))
  types_res = [x[1] for x in res]
  names_res = [x[2] for x in res]
  names_res_str = ','.join(names_res)
  iSplit = i.split('_')
  # iSplit = [int(x) for x in iSplit]
  duplicatedIds.append(iSplit)
  duplicatedId_names.append(names_res_str)
  nm_str = i.replace('_',',')
  # print(nm_str + ',' + names_res_str)
# end code block

results_lol = []
filteredCount = 0
unfilteredCount = 0
for i in range(0, len(incidentsCol)):
  if typeCol[i] == 'Miscellaneous' or typeCol[i] == 'Organization':
    iList = incidentsCol[i]
    if len(iList[-1]) == 0:
      iList = iList[:-1]
    # if len(iList) > 1:
    x = (nameCol[i], typeCol[i], iterationsCol[i], scoreCol[i], iList)
    results_lol.append(x)
    filteredCount += 1
  else:
    unfilteredCount += 1
    continue
print('total ' + str(rowCt) + ' rows.')
print('found ' + str(filteredCount) + ' Misc or Org rows, and skipped ' + str(unfilteredCount) + ' rows.')

# import dataset to pull lat/lng coordinates
df = pd.read_csv(args.patients, sep='|', dtype={'IncidentID': 'int', 'Latitude': 'float', 'Longitude': 'float'})
df_incidents = df.loc[:,'IncidentID'].tolist()
df_incidents = [str(x) for x in df_incidents]
df_lat = df.loc[:,'Latitude'].tolist()
df_lng = df.loc[:,'Longitude'].tolist()
formatted_results = []
resList = []
resNames = []
dup_Ct = 0

internalListFile = 'internalList.GooglePlaces.' + ts_string + '.txt'
with open(internalListFile, 'a') as fWrite:
  for i in range(0, len(results_lol)):
    itm = results_lol[i]
    incList = ','.join(itm[4])
    # print(str(i) + '|' + itm[0] + '|' + itm[1] + '|' + itm[2] + '|' + itm[3] + '|' + incList)
    fWrite.write(str(i) + '|' + itm[0] + '|' + itm[1] + '|' + itm[2] + '|' + itm[3] + '|' + incList + '\n')

for i in results_lol:
  iList = i[4]
  rList = []
  nm = i[0]
  for itm in iList:
    # skipping this for now, it can return patient names as words to add 
    # dupIdxList = [(i, f.index(itm)) for i, f in enumerate(duplicatedIds) if itm in f]
    # nms_add = duplicatedId_names[dupIdxList[0][0]]
    # if itm in (item for subl in duplicatedIds for item in subl):
    if itm in df_incidents:
      fIdx = df_incidents.index(itm)
      rList.append([df_lat[fIdx],df_lng[fIdx]])
      # if len(dupIdxList) > 1:
      #  dupCt += 1
      #  nm = nm + ',' + dCheck[0][1]
    else:
      print('warning, incident ID ' + str(itm) + ' not found!')
  rand_scramble = random.uniform(-0.02,0.02)
  lat_coord = float()
  lng_coord = float()
  if len(rList) == 1:
    if np.isnan(rList[0]).any() == True:
      print('No Lat/Lon coordinates for IncidentID ' + str(nm))
      resList.append((nm, (-1, -1)))
      continue
    else:
      lat_coord = rList[0][0]
      lng_coord = rList[0][1]
  else:
    rList1 = list(filter(lambda x: np.isnan(x).any() != True, rList))
    if len(rList1) != len(rList):
      print('warning, missing Lat/Lon in ' + str(nm) + ' !')
      rList = rList1
    coords_arr = np.asarray(rList, dtype=np.float64)
    km = KMeans(n_clusters = 1, init='random', max_iter=300, random_state=42)
    km.fit(coords_arr)
    centroids = km.cluster_centers_
    lat_coord = centroids[0][0]
    lng_coord = centroids[0][1]
  if rand_scramble < 0:
    lat_coord = lat_coord - rand_scramble
  else:
    lat_coord = lat_coord - rand_scramble
  resList.append((nm, (lat_coord, lng_coord)))
ct = 0

list4GooglePlacesFile = 'list4GooglePlaces.' + ts_string + '.txt'
for i in resList:
  # print(str(ct) + ',' +str(i[0]) + ',' + str(i[1][0]) + ',' + str(i[1][1]))
  with open(list4GooglePlacesFile, 'a') as fWrite:
    fWrite.write(str(ct) + ',' +str(i[0]) + ',' + str(i[1][0]) + ',' + str(i[1][1]) + '\n')
  ct += 1
