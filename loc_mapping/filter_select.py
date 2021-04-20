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


def calcClustering(l, elbow_cutoff = None):
  """
  input list must be in the format [[float(lat),float(lng)],[float(lat),float(lng)],...]
  """
  distortions = []
  if elbow_cutoff is None:
      elbow_cutoff = 16
  coords_arr = np.asarray(l, dtype=np.float64)
  kMax = len(l)
  for i in range(1, kMax):
    km = KMeans(n_clusters = i, init='random', max_iter=300, random_state=42)
    km.fit(coords_arr)
    distortions.append(km.inertia_)
  distortions_list = [(x+1, distortions[x]) for x in range(0, len(distortions))]
  """
  find distortions slopes
  """
  distortions_slopes = []
  for i in range(0, (len(distortions_list) - 1)):
    data2 = distortions_list[i+1]
    x1 = distortions_list[i][0]
    y1 = distortions_list[i][1]
    x2 = data2[0]
    y2 = data2[1]
    distortions_slopes.append((x1,((y2-y1)/(x2-x1))))
  """
  if elbowPoint < threshold, return that k, otherwise return k == 1
  """
  elbowPoint = 1
  for i in range(0, len(distortions_slopes) - 1):
    next_slope = distortions_slopes[i+1]
    curr_slope = distortions_slopes[i]
    try:
      if (curr_slope[1]/next_slope[1])**2 < elbow_cutoff:
        elbowPoint = curr_slope[0]
        break
    except ZeroDivisionError:
      print(l)
      sys.exit()
  return elbowPoint

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


def createDataList(nCol, tCol, inCol, itCol, sCol, filterList=[]):
  filteredCount = 0
  unfilteredCount = 0
  results_lol = []
  for i in range(1, len(iCol)):
    if not filterList:
      iList = list(set(iCol[i]))
      iList = list(filter(lambda x: x != '', iList))
      x = (nCol[i], tCol[i], iCol[i], sCol[i], iList, (i,i))
      results_lol.append(x)
    else:
      # untested
      if typeCol[i] in filterList:
        # COMMENT: This prevents clustering errors
        # from duplicated entries but does not preserve order.
        iList = list(set(iCol[i]))
        iList = list(filter(lambda x: x != '', iList))
        x = (nCol[i], tCol[i], iCol[i], sCol[i], iList, (i,filteredCount))
        results_lol.append(x)
        filteredCount += 1
      else:
        unfilteredCount += 1
        continue
  if filterList:
    print('total ' + str(rowCt) + ' rows.')
    print('found ' + str(filteredCount) + ' Misc or Org rows, and skipped ' + str(unfilteredCount) + ' rows.')
  return results_lol

results_lol = createDataList(nCol=nameCol, tCol=typeCol, inCol=incidentsCol, itCol=iterationsCol, sCol=scoreCol, filterList=[])


# import dataset to pull lat/lng coordinates
df = pd.read_csv(args.patients, sep='|', dtype={'IncidentID': 'int', 'Latitude': 'float', 'Longitude': 'float'})
print(df.head())
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
    # print(itm)
    # print(str(itm[5][0]) + ',' + str(itm[5][1]) + '|' + itm[0] + '|' + itm[1] + '|' + itm[2] + '|' + itm[3] + '|' + incList)
    fWrite.write(str(i) + '|' + itm[0] + '|' + itm[1] + '|' + itm[2] + '|' + itm[3] + '|' + incList + '\n')

for i in results_lol:
  iList = i[4]
  rList = []
  nm = i[0]
  for itm in iList:
    if itm in df_incidents:
      fIdx = df_incidents.index(itm)
      rList.append([df_lat[fIdx],df_lng[fIdx]])
    else:
      print('warning, incident ID ' + str(itm) + ' in ' + str(nm) + ' not found!')
  """
  if number of incident IDs < 3 skip running clustering and elbow detection
  """
  rList_filtered = list(filter(lambda x: np.isnan(x).any() != True, rList))
  rand_scramble = random.uniform(-0.02,0.02)
  if len(rList_filtered) == 0:
    print('No Lat/Lon coordinates for IncidentID ' + str(nm))
    coords = [(-1, -1)]
    resList.append((nm, coords))
  elif len(rList_filtered) == 1:
    lat_coord = rList[0][0]
    lng_coord = rList[0][1]
    if rand_scramble < 0:
      lat_coord = lat_coord - rand_scramble
    else:
      lat_coord = lat_coord - rand_scramble
    coords = [(lat_coord, lng_coord)]
    resList.append((nm, coords))
  elif len(rList_filtered) == 2:
    coords_arr = np.asarray(rList_filtered, dtype=np.float64)
    km = KMeans(n_clusters = 1, init='random', max_iter=300, random_state=42)
    km.fit(coords_arr)
    centroids = km.cluster_centers_
    lat_coord = centroids[0][0]
    lng_coord = centroids[0][1]
    if rand_scramble < 0:
      lat_coord = lat_coord - rand_scramble
    else:
      lat_coord = lat_coord - rand_scramble
    coords = [(lat_coord, lng_coord)]
    resList.append((nm, coords))
  else:
    """
    else run clustering and elbow detection
    """
    k = calcClustering(rList_filtered, elbow_cutoff = 16)
    coords_arr = np.asarray(rList_filtered, dtype=np.float64)
    km = KMeans(n_clusters = k, init='random', max_iter=300, random_state=42)
    km.fit(coords_arr)
    centroids = km.cluster_centers_
    lat_coords = []
    lng_coords = []
    if rand_scramble < 0:
        lat_coords = [x[0] - rand_scramble for x in centroids]
        lng_coords = [x[1] for x in centroids]
    else:
        lat_coords = [x[0] + rand_scramble for x in centroids]
        lng_coords = [x[1] for x in centroids]
    coords = list(zip(lat_coords, lng_coords))
    resList.append((nm, coords))


ct = 0
list4GooglePlacesFile = 'list4GooglePlaces.' + ts_string + '.txt'
for i in resList:
  # print(str(ct) + ',' +str(i[0]) + ',' + str(i[1][0]) + ',' + str(i[1][1]))
  ll_string = ''
  if len(i[1]) > 1:
    ll_list = [str(x[0]) + ',' + str(x[1]) for x in i[1]]
    ll_string = '|'.join(ll_list)
  else:
    ll_string = str(i[1][0][0]) + ',' + str(i[1][0][1]) + '|'
  # print(str(ct) + ',' + str(i[0]) + ',' + ll_string)
  with open(list4GooglePlacesFile, 'a') as fWrite:
    fWrite.write(str(ct) + ',' + str(i[0]) + ',' + ll_string + '\n')
  ct += 1
