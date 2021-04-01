import requests, sys
import re
import time
import random
import argparse
import hashlib
import time
import datetime
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


t_stamp = time.time()
t_stamp_string = datetime.datetime.fromtimestamp(t_stamp).strftime('%Y%m%d_%H%M%S')
# apikey = 'AIzaSyBG_Js3Fv3zJrCCZErdWLRoQnGqram8RG0'
# Part 0: Functions

def extractName(l):
    r = []
    if len(l) > 3:
        print('Warning, invalid type or structure!')
    for i in l:
        iSplit = i.split(',')
        r.append(iSplit[0])
    return r

def fuzzyMatch(list_matchStrings, matches_NER):
    returnList = []
    for x in range(len(list_matchStrings)):
        mString = list_matchStrings[x].upper()
        tString = extractName(matches_NER[x])
        tString = [v.upper() for v in tString]
        tString_s = list(set(tString))
        chainStore = False
        if len(tString_s) == 1:
            chainStore = True
        v = process.extract(mString,tString)
        if v[0][1] >= 80:
            if chainStore:
                returnList.append(matches_NER[x])
            else:
                returnList.append(matches_NER[x][0])
            # v_filter = list(filter(lambda x: int(x[1]) >= 80, v))
            # v_check = v_filter[0][0]
            # v_filtered = list(filter(lambda x: x[0] == v_check, v_filter))
            # returnList.append((matches_NER[x],v_filtered))
        else:
            returnList.append(None)
    return returnList

def mapDaneCountyLocation(apikey, location_lat, location_lng, radius_in_meters, debug_mode=False, next_page_token=None, locString=None):
    urlString_root = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
    # example: location_lat = '43.0778771802915' ; location_lng = '-89.3823592'
    location_lat = str(location_lat)
    location_lng = str(location_lng)
    radius_in_meters = str(radius_in_meters)
    location_string = 'location=' + location_lat + ',' + location_lng + '&radius=' + radius_in_meters + '&name=' + locString
    urlString_apikey = '&key=' + apikey
    urlQuery = urlString_root + location_string + urlString_apikey
    if next_page_token is not None:
        urlQuery = urlQuery + '&pagetoken=' + next_page_token
    if debug_mode:
        print(urlQuery)
        print(next_page_token)
    r = requests.get(urlQuery, headers={ "Content-Type" : "application/json"})
    if not r.ok:
        print('Error with:')
        print(urlQuery)
        print(r)
        return None
    else:
        decoded = r.json()
        return decoded
def scanResults_regex(val, r):
    s_split = r.split(' ')
    r_str = s_split[0]
    foundMatch = False
    m = re.search(r_str, val, re.IGNORECASE)
    if m:
        return True
    else:
        return False
def filterMatches(s, l):
    foundResults = []
    namedResults = []
    returnedResults = []
    for i in l['results']:
        nm = i['name']
        address = i['vicinity']
        pl_id = i['place_id']
        bus_status = None
        try:
            bus_status = i['business_status']
        except KeyError:
            bus_status = 'NOTBUSINESS'
        namedResults.append((nm, address, pl_id, bus_status))
    names = [x[0] for x in namedResults]
    addresses = [x[1] for x in namedResults]
    # place_ids = [x[2] for x in namedResults]
    b_status = [x[3] for x in namedResults]
    for idx,val in enumerate(names):
        res = scanResults_regex(val, s)
        if not res:
            continue
        else:
            foundResults.append((val, addresses[idx], b_status[idx]))
    stopCt = 0
    for i in range(len(foundResults)):
        if stopCt > 2:
            break
        if foundResults[i][2] != 'CLOSED_PERMANENTLY':
            returnedResults.append((foundResults[i][0], foundResults[i][1]))
        stopCt += 1
    return returnedResults


parser = argparse.ArgumentParser()

parser.add_argument('--apikey', type=str, help='API Key for Google Places', required=True)
parser.add_argument('--input', type=str, help='Input file with Index,NER String,Lat/Lon Coordinates', required=True)
parser.add_argument('--output', type=str, help='Output file name, default is ts_string.txt')
parser.add_argument('--moption1', nargs='?', const=True, type=bool, help='run fuzzy string match for Google Places matches')

args = argparse.ArgumentParser()

skipFuzzyMatch = False
if args.moption1 == False:
    skipFuzzyMatch = True

outFileName = 'outputFile.' + t_stamp_string + '.txt'
if args.output:
    outFileName = args.output

placesList = args.input

# import coords list
coords_tupleList = []
with open(placesList) as fOpen:
    for i in fOpen:
        i = i.rstrip('\r\n')
        iSplit = i.split(',')
        if len(iSplit) < 4:
            continue
        coords_tupleList.append((iSplit[0], iSplit[1], iSplit[2], iSplit[3]))

returnedResults = []
ct = 0
for i in coords_tupleList:
    vocab = str(i[1])
    if float(i[2]) == -1.0 or float(i[2]) == -1.0:
        returnedResults.append((ct, vocab, ['NoLocation_Provided']))
        ct += 1
        continue
    lat_c = str(i[2])
    lng_c = str(i[3])
    mappingResults = mapDaneCountyLocation(apikey=apikey, location_lat=lat_c, location_lng=lng_c, radius_in_meters=20000, debug_mode=True, next_page_token=None, locString=vocab)
    if len(mappingResults['results']) == 0:
        returnedResults.append((ct, vocab, ['NoResults']))
        ct += 1
        continue
    filteredRes = filterMatches(vocab, mappingResults)
    if len(filteredRes) == 0:
        print(str(vocab) + ' failed regex filter, blindly selecting the top 3')
        x_ct = 0
        x_res = []
        for val in mappingResults['results']:
            try:
                if val['business_status'] == 'CLOSED_PERMANENTLY':
                    continue
            except KeyError:
                continue
            if x_ct > 2:
                break
            nm = val['name']
            a = val['vicinity']
            x_res.append((nm, a))
            x_ct += 1
        if len(x_res) == 0:
            returnedResults.append((ct, vocab, ['NoResults']))
        else:
            returnedResults.append((ct, vocab, x_res))
    else:
        returnedResults.append((ct, vocab, filteredRes))
    ct += 1

k = 0
with open(outFileName, 'w') as fWrite:
    for i in returnedResults:
        s_out = str(i[0]) + '|' + str(i[1]) + '|'
        if i[2][0] == 'NoResults' or i[2][0] == 'NoLocation_Provided':
            print(str(k) + ',' + s_out + 'NoResults')
            fWrite.write(str(k) + ',' + s_out + 'NoResults' + '\n')
            k += 1
            continue
        s_out_list = [str(x[0]) + ',' + str(x[1]) for x in i[2]]
        # s_out_list = ['(' + str(x+1) + ') ' + s_out_list[x] for x in range(len(s_out_list))]
        s_out_list_str = '|'.join(s_out_list)
        fWrite.write(str(k) + ',' + s_out + s_out_list_str + '\n')
        print(str(k) + ',' + s_out + s_out_list_str)
        k += 1

list_NER = []
matches_NER = []
with open(outFileName) as fOpen:
    for i in fOpen:
        i = i.rstrip('\r\n')
        iSplit = i.split('|')
        ner_name = iSplit[1]
        g_matches = iSplit[2:]
        if len(g_matches) == 1:
            g_matches.append('None')
            g_matches.append('None')
        elif len(g_matches) == 2:
            g_matches.append('None')
        else:
            g_matches = g_matches
        matches_NER.append(g_matches)
        list_NER.append(ner_name)

fuzzyResults = fuzzyMatch(list_matchStrings, matches_NER)
matchedResults = []
for x in range(len(fuzzyResults)):
    if fuzzyResults[x] is not None:
        # print(fuzzyResults[x])
        matchedResults.append([fuzzyResults[x]])
        # matchedResults.append(fuzzyResults[x])
    else:
        
        tmp = [matches_NER[x][0]]
        tmp.append('None')
        tmp.append('None')
        matchedResults.append(matches_NER[x])
ct = 0
for i in range(len(matchedResults)):
    str_out = matchedResults[i][0]
    if len(matchedResults[i][0]) < 4:
        str_out = ','.join(matchedResults[i][0])
    print(str(ct),list_matchStrings[i],str_out)
    ct += 1               
