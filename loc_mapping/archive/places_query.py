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
# Part 0: Functions

t_stamp = time.time()
t_stamp_string = datetime.datetime.fromtimestamp(t_stamp).strftime('%Y%m%d_%H%M%S')
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

def parseResults(l):
    nm = l['name']
    address = l['vicinity']
    pl_id = l['place_id']
    bus_status = None
    lat = l['geometry']['location']['lat']
    lng = l['geometry']['location']['lng']
    try:
        bus_status = l['business_status']
    except KeyError:
        bus_status = 'NOTBUSINESS'
    return (nm, address, pl_id, bus_status, lat, lng)

def fuzzyMatch(parsedResults, fuzzy_NERterm):
    fMatches_all = [x[1] for x in parsedResults]
    fMatches = []
    fuzzy_results = []
    for i in fMatches_all:
        fMatches.append([x[0] for x in i])
    for idx in range(len(fMatches)):
        mString = fuzzy_NERterm[idx]
        mString = mString.upper()
        if len(fMatches[idx]) == 0:
            fuzzy_results.append((fuzzy_NERterm[idx], (None, None, None)))
        elif len(fMatches[idx]) == 1:
            m = [x.upper() for x in fMatches[idx]]
            v = process.extract(mString, m)
            m_name = parsedResults[idx][1][0][0]
            m_address = parsedResults[idx][1][0][1]
            m_url = parsedResults[idx][1][0][2]
            m_url = 'https://www.google.com/maps/place/?q=place_id:' + m_url
            fuzzy_results.append((fuzzy_NERterm[idx], [(m_name, m_address, v[0][1], m_url)]))
        else:
            m = [x.upper() for x in fMatches[idx]]
            vals = process.extract(mString, m)
            m_results = []
            for v in vals:
                vIdx = m.index(v[0])
                vScore = v[1]
                m_name = parsedResults[idx][1][vIdx][0]
                m_address = parsedResults[idx][1][vIdx][1]
                m_url = parsedResults[idx][1][vIdx][2]
                m_url = 'https://www.google.com/maps/place/?q=place_id:' + m_url
                m_results.append((m_name, m_address, vScore, m_url))
            fuzzy_results.append((fuzzy_NERterm[idx], m_results))
    return fuzzy_results


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
placesList = 'list4GooglePlaces.0021_040921.txt'
coords_tupleList = []
with open(placesList) as fOpen:
    for i in fOpen:
        i = i.rstrip('\r\n')
        iSplit = i.split(',')
        if len(iSplit) < 4:
            continue
        iValues = iSplit[2:]
        s = ','.join(iValues)
        coordsList = s.split('|')
        coordsList = list(filter(lambda x: x != '', coordsList))
        listedValues = [x.split(',') for x in coordsList]
        coords_tupleList.append((iSplit[0], iSplit[1], listedValues))

unparsedResults = []
tmp = []
ct = -1
for i in coords_tupleList:
    vocab = str(i[1])
    ct += 1
    if len(i[2]) == 1:
        lat_c = str(i[2][0][0])
        lng_c = str(i[2][0][1])
        mappingResults = mapDaneCountyLocation(apikey=apikey, location_lat=lat_c, location_lng=lng_c, radius_in_meters=20000, debug_mode=True, next_page_token=None, locString=vocab)
        if len(mappingResults['results']) == 0:
            print('nothing found in radius, expanding search to max')
            mappingResults = mapDaneCountyLocation(apikey=apikey, location_lat=lat_c, location_lng=lng_c, radius_in_meters=30000, debug_mode=True, next_page_token=None, locString=vocab)
        unparsedResults.append((vocab,[mappingResults]))
    else:
        multiple_coords = i[2]
        m_res = []
        for m in multiple_coords:
            lat_c = str(m[0])
            lng_c = str(m[1])
            mappingResults = mapDaneCountyLocation(apikey=apikey, location_lat=lat_c, location_lng=lng_c, radius_in_meters=20000, debug_mode=True, next_page_token=None, locString=vocab)
            if len(mappingResults['results']) == 0:
                print('nothing found in radius, expanding search to max')
                mappingResults = mapDaneCountyLocation(apikey=apikey, location_lat=lat_c, location_lng=lng_c, radius_in_meters=30000, debug_mode=True, next_page_token=None, locString=vocab)
            m_res.append(mappingResults)
        unparsedResults.append((vocab,m_res))

parsedResults = []
ct = -1
for unparsed in unparsedResults:
    ct += 1
    res = []
    for r in unparsed[1][0]['results']:
        # res = parseResults(r)
        res.append(parseResults(r))
    parsedResults.append((unparsed[0],res))

# only select up to 3 results
fuzzy_NERterm = [x[0] for x in parsedResults]
res = fuzzyMatch(parsedResults, fuzzy_NERterm)
res_filtered = []
for i in res:
    if len(i[1]) > 3:
        res_filtered.append((i[0],i[1][0:3]))
    else:
        res_filtered.append(i)

# Output results as CSV file
with open('parsed_matched_mapped.txt', 'w') as fWrite:
    fWrite.write('NER_term' + '\t' + 
                 'address1' + '\t' + 'confidence1' + '\t' + 'URL1' + '\t' + 
                 'address2' + '\t' + 'confidence2' + '\t' + 'URL2' + '\t' + 
                 'address3' + '\t' + 'confidence3' + '\t' + 'URL3' + '\n')
    for r in res_filtered:
        s = ''
        s_fill = '\t' + '' + '\t' + '' + '\t' + '' + '\t' + ''
        if len(r[1]) == 1:
            s = r[0] + '\t' + str(r[1][0][0]) + '\t' + str(r[1][0][1]) + '\t' + str(r[1][0][2]) + '\t' + str(r[1][0][3]) + s_fill + s_fill
        elif len(r[1]) == 2:
            l_joined = [str(x[0]) + ' ' + str(x[1]) + str("\t") + str(x[2]) + '\t' + str(x[3]) for x in r[1]]
            s = r[0] + '\t' + '\t'.join(l_joined) + s_fill
        else:
            if r[1][0] is None:
                s = r[0] + s_fill + s_fill + s_fill
            else:
                l_joined = [str(x[0]) + ' ' + str(x[1]) + str("\t") + str(x[2]) + '\t' + str(x[3]) for x in r[1]]
                s = r[0] + '\t' + '\t'.join(l_joined)
        # print(s)
        fWrite.write(s + '\n')
        
# Validate hits
# This code block is temporary, and will be replaced with NLP
def validateBusinessEntry(s, l, df, cutoff=90):
    busNames_list = df['OutbreakLocation'].tolist()
    busNames_address = df['OutbreakLocationAddress'].tolist()
    if s in busNames_list:
        s_idx = busNames_list.index(s)
        print('exact match found')
        return True
    busEntries = [x.upper() for x in l]
    s_fz = s.upper()
    v = process.extract(s_fz, busEntries)
    filtered_res = [x[1] for x in v]
    filtered_res = list(filter(lambda x: x > cutoff, filtered_res))
    if filtered_res:
        return True
    else:
        return False

bus_list = []
with open('parsed_addresses.04192021.csv') as fOpen:
    for i in fOpen:
        i = i.rstrip('\r\n')
        bus_list.append(i)    
# fuzzy match
validatedList = []
notValidatedList = []
for i in res_filtered:
    validatedEntry = False
    if i[1][0] is None:
        print('skipping ' + str(i[0]))
        continue
    if len(i[1]) == 1:
        address2Check = i[1][0][0] + ' ' + i[1][0][1]
        address2Check = address2Check.replace('.','')
        checkedEntry = validateBusinessEntry(address2Check, bus_list, parsed_df_bus_addresses)
        if checkedEntry:
            validatedEntry = True
    else:
        for v in i[1]:
            address2Check = v[0] + ' ' + v[1]
            address2Check = address2Check.replace('.','')
            checkedEntry = validateBusinessEntry(address2Check, bus_list, parsed_df_bus_addresses)
        if checkedEntry:
            validatedEntry = True
    if validatedEntry:
        validatedList.append(i)
    else:
        notValidatedList.append(i)
print('Validation Scan Complete')
print(str(len(validatedList)) + ' entries were validated, ' + str(len(notValidatedList)) + ' entries were not validated.')

# End code block
