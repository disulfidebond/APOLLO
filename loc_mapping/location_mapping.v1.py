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
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import string


t_stamp = time.time()
t_stamp_string = datetime.datetime.fromtimestamp(t_stamp).strftime('%Y%m%d_%H%M%S')
apikey = 'AIzaSyBoCLw1n0Sf060AxmaoilSIRP14ffqEqks'
# Part 0: Functions and variable names

outFileName = 'parsedOutputFromGoogle.08092021.hexagonMapping.txt'
busListFile = 'busList.1142_080921.txt'
googlePlacesList = 'list4GooglePlaces.noZips.1146_080921.txt'


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

def fuzzyMatch(res_list, ner_term, bus_list):
    print('scanning ' + str(ner_term))
    print('\n')
    fuzzy_results = []
    fzPartialRatio_results = []
    nString = ner_term.upper()
    # part1: get confidence using name string match between NER term and Google Results
    # only select values with cosine similarity confidence GT|E 0.50
    namesList = [x[0] for x in res_list]
    for idx in range(len(namesList)):
        mString = namesList[idx]
        mString = mString.upper()
        mString = mString.translate(str.maketrans('','', string.punctuation))
        v = process.extract(mString, [nString])
        m_name = res_list[idx][0]
        m_address = res_list[idx][1]
        m_url = res_list[idx][2]
        m_url = 'https://www.google.com/maps/place/?q=place_id:' + m_url
        fuzzy_results.append((m_name, m_address, v[0][1], m_url))
    # part2: match NER terms to business name db, do not include if 
    # fuzzy similarity confidence is below threshold
    for f in fuzzy_results:
        fz_name_match = f[0] + ' ' + f[1]
        fz_name_match = fz_name_match.translate(str.maketrans('', '',string.punctuation))
        # does not seem to make a difference here
        # fz_name_match = fz_name_match.translate(str.maketrans('', '', string.punctuation))
        fz_ratio = [fuzz.partial_ratio(fz_name_match, x) for x in bus_list]
        if len(fz_ratio) > 0:
            fz_max = max(fz_ratio)
            idx_list = [idx for idx,val in enumerate(fz_ratio) if val == fz_max]
            fz_ratio_vals = [val for idx,val in enumerate(bus_list) if idx in idx_list]
            fzPartialRatio_results.append((f[0], (fz_max,fz_ratio_vals)))
        else:
            print('no matches, skipping step')
    results_cosSim = []
    # only take top result, not all results
    fzPartialRatio_scores = [x[1][0] for x in fzPartialRatio_results]
    max_score = max(fzPartialRatio_scores)
    fzPartialRatio_results = list(filter(lambda x: x[1][0] == max_score, fzPartialRatio_results))
    for i in fzPartialRatio_results:
        if i[1][0] == 0:
            results_cosSim.append((i, ('FILTERED', 'FILTERED')))
            continue
        tgt_string = i[0]
        matchString = i[1][1]
        matchStrings = [x.translate(str.maketrans('', '',string.punctuation)) for x in matchString]
        tfidf_vectorizer = TfidfVectorizer()
        sparse_matrix = tfidf_vectorizer.fit_transform(matchStrings)
        doc_term_matrix = sparse_matrix.toarray()
        tgt_transform = tfidf_vectorizer.transform([tgt_string]).toarray()
        tgt_cosine = cosine_similarity(doc_term_matrix,tgt_transform)
        maxIdx = np.argmax(tgt_cosine)
        results_cosSim.append((i, (i[1][1][maxIdx], tgt_cosine[maxIdx])))
    ct = -1
    results_return = []
    # finally, use cosine similarity again as a final indicator if the NER Term/Google Result 
    # matches to an entry in the business name db
    for i in results_cosSim:
        ct += 1
        if i[1][1][0] < 0.45:
            results_return.append(['', '', '', fuzzy_results[ct][3], 0])
        else:
            results_return.append([fuzzy_results[ct][0], fuzzy_results[ct][1], fuzzy_results[ct][2], fuzzy_results[ct][3], 1])
    return results_return

      
def filterStreet(s):
    iString = s.replace(' ','')
    # add exceptions here
    if s == 'UW':
        return True
    elif s == 'BP':
        return True
    else:
        # filtering section
        # first remove short strings
        if len(iString) < 4:
            return False
        else:
            # then remove street-type strings
            m_place = re.search('Place$', s)
            m_pl = re.search('Pl$', s)
            m_st = re.search('St$', s)
            m_street = re.search('Street$', s)
            m_ave = re.search('Ave$', s)
            m_avenue = re.search('Avenue$', s)
            m_dr = re.search('Dr$', s)
            m_drive = re.search('Drive$', s)
            m_rd = re.search('Rd$', s)
            m_road = re.search('Road$', s)
            m_ct = re.search('Ct$', s)
            m_court = re.search('Court$', s)
            m_way = re.search('Way$', s)
            if m_place:
                return False
            elif m_pl:
                return False
            elif m_st:
                return False
            elif m_street:
                return False
            elif m_ave:
                return False
            elif m_avenue:
                return False
            elif m_dr:
                return False
            elif m_drive:
                return False
            elif m_rd:
                return False
            elif m_road:
                return False
            elif m_ct:
                return False
            elif m_court:
                return False
            elif m_way:
                return False
            else:
                # keep everything else
                return True
def filterStopW(s,l):
    if s == 'UW Health':
        return False
    scores = [fuzz.ratio(s, x) for x in l]
    scan_scores = list(filter(lambda x: x > 50, scores))
    if len(scan_scores) == 0:
        return True
    else:
        return False
def filterOther(s):
    m = re.search('\[|\]', s)
    if m:
        return False
    else:
        return True

def parseOutputItems(l, sep='|'):
    outS = ''
    for i in l:
        if i[-1] != 0:
            outS += str(i[0]) + ' ' + str(i[1]) + sep + str(i[2]) + sep + str(i[3]) + sep
        else:
            outS += '' + sep + '' + sep + '' + sep
    outS = outS[:-1]
    return outS

def formatOutputItems(l, sep='|'):
    fItems = ''
    if len(l) == 1:
        fItems = parseOutputItems(l, sep)
        fItems = fItems + sep + '' + sep + '' + sep + '' + sep + '' + sep + '' + sep + '' 
    elif len(l) == 2:
        fItems = parseOutputItems(l, sep)
        fItems = fItems + sep + '' + sep + '' + sep + ''
    else:
        l_out = l
        if len(l) > 3:
            l_out = l[0:3]
        fItems = parseOutputItems(l_out, sep)
    return fItems

# ensure file name variables are present
if not outFileName:
    print('Warning! Variable outFileName has not been assigned')
    sys.exit()
if not busListFile:
    print('Warning! Variable busListFile has not been assigned')
    sys.exit()
if not googlePlacesList:
    print('Warning! Variable googlePlacesList has not been assigned')
    sys.exit()

# hexagon mapping step functions

def haversineDistance(lat1, lon1, lat2, lon2):
    # credit: https://stackoverflow.com/a/4913653
    from math import radians, cos, asin, sqrt, atan2, sin
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6373.0 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def haversinePoint(lat1, lon1, d, bearing):
    # credit: https://www.edwilliams.org/avform147.htm#Dist 
    # and https://dtcenter.org/sites/default/files/community-code/met/docs/write-ups/gc_simple.pdf
    from math import radians, cos, asin, sqrt, pi, atan2, sin
    lon1, lat1, bearing = map(radians, [lon1, lat1, bearing])
    d = d/6373.0
    lat2 = asin(sin(lat1)*cos(d) + cos(lat1)*sin(d)*cos(bearing))
    lon2 = None
    if (cos(lat1) == 0):
        lon2 = lon1
    else:
        lon2=((lon1-asin(sin(bearing)*sin(d)/cos(lat1))+pi)%(2*pi))-pi
    # alternative but equivalent
    # dlon=atan2(sin(bearing)*sin(d)*cos(lat1),cos(d)-sin(lat1)*sin(lat2))
    # lon2=((lon1-dlon+pi)%(2*pi))-pi
    lat2 = lat2 * (180/pi)
    lon2 = lon2 * (180/pi)
    lat1 = lat1 * (180/pi)
    lon1 = lon1 * (180/pi)
    return (lat2,lon2)

def pullItems(d, itm):
    for k,v in d.items():
        if v == itm:
            return k
def convertValue(v):
    result = v - 90
    if result < 0:
        v_res = 90 - v
        result = 360 - v_res
        return result
    else:
        return result

def convertAngle(a):
    result = a + 180
    if a <= 360:
        return result
    else:
        mod_a = a - 180
        return mod_a


# ###
# layers section
# last angle is only to validate
# layer 0
l0 = [270, 330, 30, 90, 150, 210] 
# layer 1
l1 = [
     150,
     210,
     270,
     210,
     270,
     330,
     270,
     330,
     30,
     330,
     30,
     90,
     30,
     90,
     150,
     90,
     150,
     210
     ]
# layer 2
l2 = [150,210,150,210,270,210,270,210,270,330,270,330,270,330,30,330,30,330,30,90,30,90,30,90,150,90,150,90,150,210]
l0_conv = []
for i in l0:
    l0_conv.append(convertValue(i))
l1_conv = []
for i in l1:
    l1_conv.append(convertValue(i))
l2_conv = []
for i in l2:
    l2_conv.append(convertValue(i))
# end layers section

# hexagon mapping section
def hexagonMapping(lat_origin, lon_origin, l0_conv = l0_conv, l1_conv = l1_conv):
    # first angle: 60 degrees, offset = 90
    # first angle to first hexagon vertex in quadrant II == 60 degrees
    # update: this places the origin as the center of the layer 0 hexagon
    lat1, lon1 = haversinePoint(lat1=lat_origin, lon1=lon_origin, d=30, bearing=60) # 90 == W, 270 == E
    # print('Starting Lat,Lon is', str(lat1), str(lon1))
    d = haversineDistance(lat1=lat_origin,lon1=lon_origin, lat2=lat1, lon2=lon1)
    # print('Distance is',str(d))
    
    # layer 0
    lat_lon_layer0 = []
    lat_start = lat1
    lon_start = lon1
    lat_t = None
    lon_t = None
    lat_end = None
    lon_end = None
    ct = 0
    for b in l0_conv:
        ct += 1
        if lat_t is None:
            lat_t = lat_start
            lon_t = lon_start
            lat_lon_layer0.append((lat_t, lon_t))
        lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=b)
        lat_lon_layer0.append((lat_t, lon_t))
        if ct == (len(l0_conv)-1):
            lat_end = lat_t
            lon_end = lon_t

    # debug code block
    # print('layer 0 coordinates are:')
    # for itm in lat_lon_layer0:
    #    print(itm)
    # print('Starting Latitude,Longitude is ',str(lat_start),str(lon_start),'\nEnding Latitude,Longitude is ', str(lat_t), str(lon_t))
    # print('Error Distance is ', haversineDistance(lat_start, lon_start, lat_t, lon_t))
    # print('Radial Distance from Origin Point is ', haversineDistance(lat_origin, lon_origin, lat_t, lon_t))
    # end debug code block

    # layer 1
    lat_lon_layer1 = []
    lat_start = lat_end 
    lon_start = lon_end
    lat_start1, lon_start1 = haversinePoint(lat1=lat_start, lon1=lon_start, d=30, bearing=0)
    lat_t = None
    lon_t = None
    lat_end = None
    lon_end = None
    ct = 0
    center_ct = 1
    centerPointList = []
    centerPointAngle = None
    hexPointList = []
    for b in l1_conv:
        ct += 1
        center_ct += 1
        if lat_t is None:
            lat_t = lat_start1
            lon_t = lon_start1
            lat_lon_layer1.append((lat_t, lon_t))
        if center_ct == 2:
            centerPointAngle = b + 180
            centerPointAngle = convertAngle(centerPointAngle)
        if center_ct == 3:
            lat_center, lon_center = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=centerPointAngle)
            centerPointList.append((lat_center, lon_center))
            center_ct = 0
        lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=b)
        # print(lat_t, lon_t)
        lat_lon_layer1.append((lat_t, lon_t))
        if ct == (len(l0_conv)-1):
            lat_end = lat_t
            lon_end = lon_t

    # debug code block
    # print('layer 1 coordinates are:')
    # for itm in lat_lon_layer1:
    #    print(itm)
    #print('center points:')
    #for itm in centerPointList:
    #    print(itm)
    # end debug code block
    returnList = lat_lon_layer0 + lat_lon_layer1 + centerPointList
    return returnList
# end hexagon mapping section

# business list
bus_list = []
with open(busListFile) as fOpen:
    for i in fOpen:
        i = i.rstrip('\r\n')
        m = re.search('\d+', i)
        if m:
            bus_list.append(i)
bus_list_unformatted = [x.translate(str.maketrans('', '',string.punctuation)) for x in bus_list]
bus_list = [x.replace('  ',' ') for x in bus_list_unformatted]

# import coords list
placesList = googlePlacesList
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
for i in coords_tupleList:
    print(i)

# 
# optional, use if reruns are needed
skipTermList = ['VeronaI', 'BOSTO', 'MilwaukeeI', 'UHSHankes', 'P . O . Box Topeka', 'HARVE H', 'W No']

parsedResults = []
unparsedResults = []
ct = -1
stopX = 0
for i in coords_tupleList:
    res_filtered = []
    vocab = str(i[1])
    # optional debug block for rerunning
    # skip queries with no API results
    if vocab in skipTermList:
        res_results = []
        print('skipping query for ' + str(vocab) + ' reason: KNOWNSKIP\n')
        res_results.extend(['FILTERED', 5])
        unparsedResults.append((vocab, res_results))
        continue
    # end optional debug block
    print('querying API for ' + str(vocab))
    ct += 1
    check1 = filterStreet(i[1])
    check3 = filterOther(i[1])
    if not check1:
        print('skipping result ' + vocab + ', Reason 2')
        unparsedResults.append((vocab, ['FILTERED', 2]))
    elif str(i[2][0][0]) == '-1':
        print('skipping result ' + vocab + ', Reason 1')
        unparsedResults.append((vocab, ['FILTERED', 1]))
    elif not check3:
        print('skipping result ' + vocab + ', Reason 4')
        unparsedResults.append((vocab, ['FILTERED', 4]))
    else:
        multiple_coords = i[2]
        m_res = []
        noMatchBool = False
        for m in multiple_coords:
            lat_c = str(m[0])
            lng_c = str(m[1])
            mappingResults = mapDaneCountyLocation(apikey=apikey, location_lat=lat_c, location_lng=lng_c, radius_in_meters=20000, debug_mode=True, next_page_token=None, locString=vocab)
            if len(mappingResults['results']) == 0:
                print('nothing found in radius, expanding search to max')
                mappingResults = mapDaneCountyLocation(apikey=apikey, location_lat=lat_c, location_lng=lng_c, radius_in_meters=30000, debug_mode=True, next_page_token=None, locString=vocab) 
            if len(mappingResults['results']) == 0:
                hexagonSearch = hexagonMapping(float(lat_c), float(lng_c))
                print('running hexagon search for ' + str(vocab))
                for h in hexagonSearch:
                    r = mapDaneCountyLocation(apikey=apikey, location_lat=h[0], location_lng=h[1], radius_in_meters=20000, debug_mode=True, next_page_token=None, locString=vocab)
                    # stop at first result
                    if r['status'] == 'OK':
                        mappingResults = r
                        break
            if len(mappingResults['results']) == 0:
                res_filtered.append(['FILTERED', 5])
                noMatchBool = True
                continue
            res = mappingResults['results'][0:3]
            p_res = [parseResults(x) for x in res]
            m_res.extend(p_res)
        if not noMatchBool:
            res_filtered.append((vocab,m_res))
        res_results = []
        for u in res_filtered:
            if u[0] == 'FILTERED':
                print('Skipping check for ' + str(u[0]) + ' reason ' + str(u[1]))
                res_results.extend(['FILTERED', 5])
            else:
                fzMatch_res = fuzzyMatch(u[1], u[0], bus_list)
                res_results.extend(fzMatch_res)
        unparsedResults.append((vocab, res_results))

# output section
sep = '\t'
with open(outFileName, 'w') as fWrite:
    fWrite.write('Name\tAddress1\tConfidence1\tURL1\tAddress2\tConfidence2\tURL2\tAddress3\tConfidence3\tURL3\n')
    for i in unparsedResults:
        s = ''
        if i[1][0] == 'FILTERED':
            s = str(i[0]) + sep + 'FILTERED' + sep + '' + sep + '' + sep + '' + sep + '' + sep + '' + sep + '' + sep + '' + sep + ''
        else:
            sList = i[1]
            # sort lists so that Address1 is never empty
            sList.sort(key=lambda x: x[4], reverse=True)
            s = formatOutputItems(sList, sep='\t')
            # if all results are empty, add 'FILTERED
            check_s = s.split(sep)
            f_s = list(filter(lambda x: x != '', check_s))
            if len(f_s) == 0:
                check_s[0] = 'FILTERED'
                s = sep.join(check_s)
            s = str(i[0]) + sep + s
        print(s)
        fWrite.write(s + '\n')
