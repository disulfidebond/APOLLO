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

df_data = pd.read_csv(args.data)

df_data_nameList = df_data['Name'].replace(np.nan, '', regex=True)
nameCol = df_data_nameList.tolist()
df_data_typeList = df_data['Type'].replace(np.nan, '', regex=True)
typeCol = df_data_typeList.tolist()
df_data_iterList = df_data['Iterations'].replace(np.nan, 0.0)
iterationsCol = df_data_iterList.tolist()
df_data_scoreList = df_data['Score'].replace(np.nan, 0.0)
scoreCol = df_data_scoreList.tolist()
df_data_incdList = df_data['Incidents'].replace(np.nan, '', regex=True)
incidentsCol_data = df_data_incdList.tolist()

# parse incidents col into list of lists
incidentsCol = []
for i in incidentsCol_data:
  iAppend = i.split(',')
  iAppend = [x.replace(' ','') for x in iAppend]
  iAppend = [x.replace("'","") for x in iAppend]
  iAppend = [x.replace('"', '') for x in iAppend]
  iAppend = [x.replace('[', '') for x in iAppend]
  iAppend = [x.replace(']', '') for x in iAppend]
  incidentsCol.append(iAppend)

df_data_OutbList = df_data['Outbreaks'].replace(np.nan, '', regex=True)
outbreakCol = df_data_OutbList.tolist()
df_data_OutbIDList = df_data['OutbreakIDs'].replace(np.nan, 0)
outbreakIDCol = df_data_OutbIDList.tolist()
df_data_OutbLocList = df_data['Outbreak Locations'].replace(np.nan, '', regex=True)
outbreakLocCol = df_data_OutbLocList.tolist()
df_data_OutbProcStList = df_data['Outbreak Process Statuses'].replace(np.nan, '', regex=True)
outbreakProcStCol = df_data_OutbProcStList.tolist()

def createDataList(nCol, tCol, inCol, itCol, sCol, filterList=[]):
  filteredCount = 0
  unfilteredCount = 0
  results_lol = []
  for i in range(0, len(inCol)):
    if not filterList:
      iList = list(set(inCol[i]))
      iList = list(filter(lambda x: x != '', iList))
      x = (nCol[i], tCol[i], itCol[i], sCol[i], iList, (i,i))
      results_lol.append(x)
    else:
      # untested
      if typeCol[i] in filterList:
        # COMMENT: This prevents clustering errors
        # from duplicated entries but does not preserve order.
        iList = list(set(inCol[i]))
        iList = list(filter(lambda x: x != '', iList))
        x = (nCol[i], tCol[i], itCol[i], sCol[i], iList, (i,filteredCount))
        results_lol.append(x)
        filteredCount += 1
      else:
        unfilteredCount += 1
        continue
  if filterList:
    print('total ' + str(rowCt) + ' rows.')
    print('found ' + str(filteredCount) + ' Misc or Org rows, and skipped ' + str(unfilteredCount) + ' rows.')
  return results_lol


# createDataList(nCol, tCol, inCol, itCol, sCol, filterList=[])
results_lol = createDataList(nCol=nameCol, tCol=typeCol, inCol=incidentsCol, itCol=iterationsCol, sCol=scoreCol, filterList=[])

# import dataset to pull lat/lng coordinates
df = pd.read_csv(args.patients, sep='|', dtype={'IncidentID': 'int', 'Latitude': 'float', 'Longitude': 'float'})
df = df.dropna()
df_incidents = df.loc[:, 'IncidentID'].tolist()
df_incidents = [str(x) for x in df_incidents]
df_lat = df.loc[:,'Latitude'].tolist()
df_lng = df.loc[:,'Longitude'].tolist()
df_zip = df.loc[:,'Zip'].tolist()
df_cty = df.loc[:,'County'].tolist()
formatted_results = []
resList = []
resZip = []
resCty = []
resNames = []
dup_Ct = 0


internalListFile = 'internalList.GooglePlaces.' + ts_string + '.txt'
with open(internalListFile, 'a') as fWrite:
  for i in range(0, len(results_lol)):
    itm = results_lol[i]
    incList = ','.join(itm[4])
    fWrite.write(str(i) + '|' + itm[0] + '|' + itm[1] + '|' + str(itm[2]) + '|' + str(itm[3]) + '|' + incList + '\n')

for i in results_lol:
  iList = i[4]
  rList = []
  zList = []
  cList = []
  nm = i[0]
  for itm in iList:
    if itm in df_incidents:
      fIdx = df_incidents.index(itm)
      rList.append([df_lat[fIdx],df_lng[fIdx]])
      # modification to add zips
      zList.append(df_zip[fIdx])
      cList.append(df_cty[fIdx])
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
  zipString = ','.join(zList)
  # remove duplicated counties
  resCtyList = list(set(cList))
  ctyString = ','.join(resCtyList)
  resZip.append((nm, zipString))
  resCty.append((nm, ctyString))

zips4GooglePlacesFile = 'zipsCounties4GooglePlaces.' + ts_string + '.txt'
with open(zips4GooglePlacesFile, 'w') as fWrite:
  for i in range(0, len(resZip)):
    s = resZip[i][0] + '\t' + resZip[i][1] + '\t' + resCty[i][0] + '\t' + resCty[i][1]
    fWrite.write(s + '\n')

ct = 0
list4GooglePlacesFile = 'list4GooglePlaces.noZips.' + ts_string + '.txt'
for i in resList:
  ll_string = ''
  if len(i[1]) > 1:
    ll_list = [str(x[0]) + ',' + str(x[1]) for x in i[1]]
    ll_string = '|'.join(ll_list)
  else:
    ll_string = str(i[1][0][0]) + ',' + str(i[1][0][1])
  with open(list4GooglePlacesFile, 'a') as fWrite:
    fWrite.write(str(ct) + ',' + str(i[0]) + ',' + ll_string + '\n')
  ct += 1
