import requests, sys
import re
import time
import random
import argparse
import hashlib
import time
import datetime
import json

t_stamp = time.time()
t_stamp_string = datetime.datetime.fromtimestamp(t_stamp).strftime('%Y%m%d_%H%M%S')
apikey = 'AIzaSyBG_Js3Fv3zJrCCZErdWLRoQnGqram8RG0'
# Part 0: Functions

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

    # import coords list
    coords_tupleList = []
    with open('coords_list.txt') as fOpen:
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

                for i in returnedResults:
                    s_out = str(i[0]) + '|' + str(i[1]) + '|'
                    if i[2][0] == 'NoResults':
                        print(s_out + 'NoResults')
                        continue
                    s_out_list = [str(x[0]) + ',' + str(x[1]) for x in i[2]]
                    # s_out_list_str = ','.join(s_out_list)
                    s_out_list = ['(' + str(x+1) + ') ' + s_out_list[x] for x in range(len(s_out_list))]
                    s_out_list_str = ','.join(s_out_list)
                    with open('outputFile.02202021.txt', 'a') as fWrite:
                        fWrite.write(s_out + s_out_list_str + '\n')
                    print(s_out + s_out_list_str)                                
