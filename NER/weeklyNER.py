import csv
from datetime import datetime, timedelta
import numpy as np
import statistics
import NERPipeline
from typing import List, Dict
from pathlib import Path
import re
from tqdm import tqdm
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import logging


logging.basicConfig(format='%(levelname)s :: %(filename)s :: %(funcName)s :: %(asctime)s :: %(message)s', level=logging.DEBUG)
logging.getLogger('NERPipeline').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


def weekfinder(day):
    """
    given a date, return it's year and week number
    :param: day: string in format "YYYY-MM-DD" or "YYYY-MM-DD <anything_else>"
    :return: tuple (int(YYYY), int(1-52))
    """
    try:
        d=day.split(" ")
        d=d[0]
        d=(datetime.strptime(d,'%Y-%m-%d'))
        d=d.date()
        year, week, weekday=d.isocalendar()
        return(year, week)
        
    except:
        print(f"couldn't convert date: {day}")
        return(0,0)
   
   
def findweekIDs():
    """
    open patient file directly and get all incident IDS that are:
        in the week specified  
        in Dane county
        for confirmed cases
    :return: dict of relevant incident ids from patient file {incident_id:1}
    """
    file_path=""  # Add WEDSS patient file absolute path
    f=open(file_path,encoding='ISO-8859-1')
    f.seek(0)
    lines=f.readlines()

    # establish year and week number of interest
    d=(input("PLEASE ENTER A Day in Your Target Week Number: YYYY-MM-DD:  "))
    w=weekfinder(d)
    print(f'year week conversion: {w}')    
   
    # for every line in patient file (not including header row), where:
      # EpisodeDate not empty string and in specified week
      # county=Dane
      # resolutionStatus contains 'Confirmed' or 'Probable'
    # add that to list of incident ids
    confirmed_counter = 0
    any_counter = 0
    ids={}
    count = 0
    usable_resolution_statues = ["Confirmed", "Probable"]
    for line in lines:
        count+=1
        ls=line.split("|")
        incidentID = ls[0]
        episode_date = ls[18]
        county = ls[8]
        resolution_status = ls[33]
        
        if count!=1 \
            and episode_date!='' \
            and weekfinder(episode_date) == w \
            and county=="Dane":
                
            any_counter += 1
            if resolution_status.strip() in usable_resolution_statues:       
                ids[incidentID]=1
                confirmed_counter+=1
                
    print(f'total rows, in Dane county, in week of specified date:{any_counter}')            
    print(f'total confirmed cases, in Dane county, in week of specified date:{confirmed_counter}')
    return(ids)


def readfields(ids):
    """
    This function gets the list of incidentIDs and concatenates 
    all related fields in all files for each of them
    :param ids: dict of {incidentID:1}
    :return: output dict {incidentID:"all.values.associated.with.that.incident.in.all.files"}
    """
    output={}
    headers={}
    incidents={}
    file_folder=""  # Add WEDSS data folder path
    files=["WEDSS_file_name_1","WEDSS_file_name_2","WEDSS_file_name_2","WEDSS_file_name_2",]  # Add WEDSS file names

    # get all column names (that will be used for NLP)
    with open("NLPFields.csv","r") as csvfile:
        csvreader=csv.reader(csvfile,delimiter=",")
        for row in csvreader:
            if row[0]!="Fields": #"Fields" is first column in header row
                headers[row[0]]=1  # headers dict structure {'column_name':1)
    
    headers_keys = list(headers.keys())
    print(f'read in {len(headers)} headers to search for, e.g. {headers_keys[0]}')
    
    # iterate over all files in files list, 
    print('reading in WEDSS files data...')
    for file in tqdm(files):
        f=open(file_folder+file,encoding='ISO-8859-1')
        f.seek(0)
        lines=f.readlines()
        counter=0
        hdic={}
        
        # iterate over every line in a file
        for line in lines:
            ls=line.split("|")
            counter+=1
            
            # determine which columns are relevant in this file, make dict hdic
            # for the first line in a file add all values to hdic {'header val':1}
            # IF those header values in the NLPFields.csv file, or contain "_Sec".
            if counter==1:
                for i in range(0,len(ls)):
                    # Needs to be modified to cover exact fields?
                    if ls[i] in headers or "_Sec" in ls[i]:
                        hdic[i]=1
                        
            # for non-header/first file rows, if row has id in list of ids passed to fn
            # concatenate all values of all columns in that file that were in the NLPFields.csv file
            # concatenated into a string separated by '.'. 
            else:
                tmp=''
                if ls[0] in ids:
                    for key in hdic:
                        try:
                            tmp=tmp+'.'+ls[key]
                        except:
                            # ignore if error reading
                            t=0
                    
                    # ls[0] is indicidentID, if its not in output dir already, append
                    if ls[0] not in output:
                        output[ls[0]]=tmp
                    else:
                        output[ls[0]]+=tmp    
    return(output)

    
def saveoutputtexts(output):
    """
    write all the collected text fields across all files for each incident ID to CSV
    :param output: dict {indcidentID:text}
    :return: N/A
    """
    with open("weeksincidents.csv", "w") as csvfile:
        csvwriter=csv.writer(csvfile,delimiter=",")
        csvwriter.writerow(["IncidentID","Text"])
        for key in output:
            csvwriter.writerow([key,output[key]])


def chunk_out_text(text:str, chunk_size:int = 510) -> List:
    """
    :param text: any string
    :param chunk_size: any positive int
    split up test a list of 0-to-n chunk_size strings and one string with the remaining < chunk_size chunk
    510 since tokenzier will add start and end tokens, model max is 512.
    """
    i = 0
    chunks = []
    while i < len(text):
        if i+chunk_size < len(text):
            chunks.append(text[i:i+chunk_size])
        else:
            chunks.append(text[i:len(text)])
        i += chunk_size
    return chunks


def ner_over_chunks(actual_text:str) -> List:
    """
    Take a string of any length, cut it into 512 char sections, run NER on each section then reassemble the
    results into one list.
    :param actual_text: any string - here all concatenated text associated with an incident_id_column_name
    :return: List of three lists [[entities], [entity types], [scores]]
    """
    chunks = chunk_out_text(actual_text)
    aggregated_results = [[],[],[]]
    chunk_ners = []
    for c in chunks:
        # NERPipeline returns 3 value list
        # chunk_ners is a list of three value lists
        res = NERPipeline.nerfunc(c)
        chunk_ners.append(res)
    
    for c in chunk_ners:
        for j in range(3):
            aggregated_results[j].extend(c[j]) 
    
    return aggregated_results
  
  
def BERTNER(output:Dict) -> Dict:
    """
    run BERT NER pipeline on text for every incident id - this is distributed via dask.distributed
    :param output: dict {incidentID: "all text from NLPFields.csv fields concatenated"}
    :return: dict {incidentID: [[Names], [Types], [Scores]]}
    """
    out={}
    print('kick off NER processing...')
    for key, actual_text in tqdm(output.items()): 
        out[key] = ner_over_chunks(actual_text)
    return(out)
    
    
def saveNER(out:Dict):
    """
    write BERT NER results to a csv file
    :param out:  dict {incidentID: [[Names], [Types], [Scores]]}
    """
    with open("NERResults.csv", "w") as csvfile:
        csvwriter=csv.writer(csvfile,delimiter=",")
        # write header row
        csvwriter.writerow(["IncidentID","Names","Types","Scores"])
        
        # for each incidentID, write NER output
        for key, tmp in out.items():
            # tmp=out[key]
            csvwriter.writerow([key,tmp[0],tmp[1],tmp[2]])


def find_associated_known_outbreak_ids(namedic:Dict) -> Dict:
    """
    for a given dictionary of NER entities and associated incidentIDs find all known outbreak IDs associated with that key and incidentIDs
    :param namedic: Dict {NERentity: List of IncidentIDs}
    :return: {NERentity: outbreakIDs}
    """
    # relevant column names in outbreak_id file
    incident_id_column_name = 'IncidentID' 
    outbreak_id_column_name = 'Outbreak#'
    outbreak_location_column_name = 'OutbreakLocation'
    incident_id_index = None
    outbreak_id_index = None
    location_name_index = None
    # existing outbreakIDs file.
    outbreak_file_path = ''  # Add WEDSS outbreak file absolute path
    incidents_to_outbreaks = {}
    outbreaks_to_ners = {}
    # get all outbreaks in outbreak file
    with open(outbreak_file_path, "r") as csvfile:
        csvreader=csv.reader(csvfile, delimiter=",")
        for row_counter, row in enumerate(csvreader):
            if row_counter == 0:  # if header row get indexes for the two columns we want
                try:
                    logger.info('opened outbreak_id file, header row: %s', str(row))
                    incident_id_index = row.index(incident_id_column_name)
                    outbreak_id_index = row.index(outbreak_id_column_name)
                    location_name_index = row.index(outbreak_location_column_name)
                except (ValueError):
                    logger.error('Failed reading outbreak file %s: one of %s, %s, %s, not found in %s', outbreak_file_path, incident_id_column_name, outbreak_id_column_name, outbreak_location_name, str(row))
            else:
                # map incidentID to outbreakID
                incident_id = row[incident_id_index]
                outbreak_id = row[outbreak_id_index]
                incidents_to_outbreaks[incident_id] = outbreak_id
                
                # map outbreakID to pre-NER string incorporating outbreak location information - typically organization name
                outbreak_location = row[location_name_index]
                preprocessed_outbreak_id = outbreak_id.replace('-',' ').title().split()    
                preprocessed_outbreak_id.insert(2, '.')
                preprocessed_outbreak_id = ' '.join(preprocessed_outbreak_id) + '. ' + outbreak_location
                outbreaks_to_ners[outbreak_id] = preprocessed_outbreak_id
        
    # replaced preprocessed_outbreaks_ids with NER results
    outbreaks_to_ners = BERTNER(outbreaks_to_ners)
    # limit NER results to just the entities, remove the types and scores.
    for outbreak_id, ner_result in outbreaks_to_ners.items():
        outbreaks_to_ners[outbreak_id] = [x for x in outbreaks_to_ners[outbreak_id][0] if ((x!="") and (x[0]!="#"))]


    # for all entities found by the NER pipeline look at WEDDS data
    outbreak_ids_by_entity = {}  # key:[list of outbreak ids]
    for key in namedic:
        if key!="" and key[0]!="#":
            # set up empty list to append to
            outbreak_ids_by_entity[key] = []
            # get all incident ids associated with each NER entity/key
            incident_id_strs = [str(x).strip() for x in namedic[key]]
            for incident_id in incident_id_strs:
                try:
                    # if an outbreak NER entity associated with an incident ID matches the key - store that outbreak ID
                    for entity in outbreaks_to_ners[incidents_to_outbreaks[incident_id]]:
                        if key == entity: 
                            outbreak_ids_by_entity[key].append(incidents_to_outbreaks[incident_id])
                            break
                            
                except (KeyError):
                    # logger.debug('entity %s not found in outbreak id search process', key)
                    pass
                        
        
    return outbreak_ids_by_entity


def get_stop_entities(path:Path, entity_col:str, type_col:str, frequency_col:str, cutoff:float) -> Dict:
    """
    load csv file with list of 'stop entities'
    load all rows for columns entity_col and type_col to a dict {entity_col:type:col}
    :param path: Path to file
    :param entity_col: column in file with entities
    :param type_col: column in file with entity types
    :param frequency_col: column in file with frequency as percentage
    :param cutoff: frequency at and above which stop entities will be used
    :return: stop_entities Dict {entity_col: type_col}
    """
    stop_entities = {}
    entity_col_index = None
    type_col_index = None
    frequency_col_index = None 
    logger.debug('opening stop entites file %s', str(path))
    with open(path, "r") as csvfile:
        csvreader=csv.reader(csvfile, delimiter=",")
        for row_counter, row in enumerate(csvreader):
            if row_counter == 0:  # if header row get indexes for the two columns we want
                try:
                    logger.info('opened outbreak_id file, header row: %s', str(row))
                    entity_col_index = row.index(entity_col)
                    type_col_index = row.index(type_col)    
                    frequency_col_index = row.index(frequency_col)
                except (ValueError):
                    logger.error('Failed reading stop entities file %s: one of %s, %s, not found in %s', path, entity_col, type_col, str(row))
            else:
                if float(row[frequency_col_index]) >= cutoff:
                    stop_entities[row[entity_col_index]] = row[type_col_index]
    return stop_entities
    
    
def fuzzy_match_entities(all_keys:Dict, match_cutoff:int=70) -> Dict:
    """
    fuzzy match all keys in output dict then return collated results in a dict
    :param all_keys: Dict, structure {'entity to fuzzy match':'list of associated values'}
    :param match_cutoff: fuzzywuzzy token_sort_ratio minimum value for match - fine tune for application
    :return: Dict {'matched entities (str)': List of Lists of collated values}
    """
    key_list = list(all_keys.keys())
    for key in key_list:
        matched_keys_scores = process.extract(key, all_keys.keys(), scorer=fuzz.token_sort_ratio)
        matched_keys = [key for key, score in matched_keys_scores if score >= match_cutoff]

        if matched_keys:
            collected_entities = []
            collected_values = []
            new_entity = ''
            for entity in matched_keys:
                collected_entities.append(entity)
                collected_values.append(all_keys[entity])
                new_entity = ','.join(collected_entities)

                # remove matched entities from orig dict so it's not added more than once
                del all_keys[entity]

            new_value = [[] for x in collected_values[0]]

            for single_entity_result in collected_values:
                for i, result_datum in enumerate(single_entity_result):
                    new_value[i].append(result_datum)

            all_keys[new_entity] = new_value
    return all_keys


def findpopular(out):
    """
    take NERPipeline output (per incident) and convert to per entity output:
        for every entity: entity name, entity type, number of times found, 
        average score, list of incidentID where it was found
    write results to CSV
    also output a list of unqiue incidient IDs in the file
    also check for any existing outbreakids associated with every entity and incidentID
        if they exist, append list per entity
    :param out:  dict {incidentID: [[Names], [Types], [Scores]]}
    """
    countdic={}
    namedic={}
    typedic={}
    scoredic={}
    # for all incidentIDs (e.g. relevant text at this point), get entities, types and scores
    # for every entity
    #    add entity to typedic with {entity:type}
    #    add score to scoredic with {entity:[score, score..]]
    #       if entity already present - append score to existing scores 
    for key in out:
        tmp=out[key]
        entity=tmp[0]
        entity_type=tmp[1] 
        score=tmp[2]
        for i in range(0,len(tmp[0])):
            if entity[i] not in typedic:
                typedic[entity[i]]=entity_type[i]
            if entity[i] not in scoredic:
                #print(score[i])
                scoredic[entity[i]]=[float(score[i])]
            else:
                m=scoredic[entity[i]]
                m.append(float(score[i]))
                scoredic[entity[i]]=m

    # for all incidentIDs (relevant text)
    # get all entities
    #   add every entity to namedic {entity:[IncidentID]}
    #   add every entity to countdic {entity:1}
    #   each time an existing entity is found:
    #       increment value for entity in countdic 
    #       append incidentID to entity key list in namedic 
    for key in out:
        tmp=out[key]
        tmp=set(tmp[0])
        for item in tmp:
            if item not in namedic:
                namedic[item]=[key]
                countdic[item]=1
            else:
                countdic[item]+=1
                l=namedic[item]
                l.append(key)
                namedic[item]=l

    stop_entities = get_stop_entities(path = Path(''),  # Add WEDSS stop entities file - generate from running whole pipeline on all data
                                      entity_col = '\ufeffName',
                                      type_col = 'Type',
                                      frequency_col = 'Frequency',
                                      cutoff = 1.6)

    outbreak_ids_by_key = find_associated_known_outbreak_ids(namedic)
    
    # capture all unique incident ids 
    every_outbreak_incident_id = []
    
    # collate all results into output format: {entity:[type, count, score, incidentids, outbreakids] }
    all_keys = {}
    for key in namedic:
        if key!="" \
           and key[0]!="#"\
           and typedic[key] != 'Person':  #\
           # and key not in stop_entities:  # TODO: no stop entities while defining them!
            # collect every incidentID to facilitate search
            incident_id_strs = [str(x).strip() for x in namedic[key]] 
            every_outbreak_incident_id.extend(incident_id_strs)
            
            # collect all entities and associated information
            all_keys[key] = [typedic[key],
                            countdic[key],
                            sum(scoredic[key])/len(scoredic[key]),
                            incident_id_strs,
                            outbreak_ids_by_key[key]]
    
    all_keys = fuzzy_match_entities(all_keys)
    write_final_results_to_csv(all_keys)
    write_all_unique_incidentids_to_csv(every_outbreak_incident_id)
    
    
def flatten(lol:List) -> List:
    """
    take any list of lists (of lists), return a single list with all values
    :param lol: List of lists
    :return: a flat list
    """
    if isinstance(lol, list):
        return [a for i in lol for a in flatten(i)]
    else:
        return [lol]
 
 
def write_final_results_to_csv(final_results:Dict):
    """
    Write final results to .txt file (pipe separated)
    :param final_results: Dict {entity: str: List of Lists of values ["Type","Iterations","Score", "Incidents", "Outbreaks"] 
    """
    now = datetime.strftime(datetime.now(), '%Y%m%d_%H_%M')
    with open(f"FinalResults_{now}.txt","w") as csvfile:
        csvwriter=csv.writer(csvfile,delimiter="|")
        # write header row
        csvwriter.writerow(["Name","Type","Iterations","Score", "Incidents", "Outbreaks"])
        
        # sort results by total combined iterations count
        final_results = {k: v for k,v in sorted(final_results.items(), 
                                               key=lambda item: sum(flatten(item[1][1])), 
                                               reverse=True)}
        # for every named entity write: entity, type, count, average score, incidentIDs list
        for entity, results in final_results.items():
                csvwriter.writerow([entity,
                                    results[0],
                                    results[1],
                                    results[2],
                                    results[3],
                                    results[4]])


def write_all_unique_incidentids_to_csv(every_outbreak_incident_id: List):
    """
    output single csv column fo all unique incident ids in final output
    :param every_outbreak_incident_id: List 
    """
    now = datetime.strftime(datetime.now(), '%Y%m%d_%H_%M')
    # write all unique incident ids out to a separate file
    with open(f"every_outbreak_incident_id_from_NERPipeline_{now}.csv", 'w') as csvfile:
        csvwriter=csv.writer(csvfile,delimiter=',')
        every_outbreak_incident_id = list(set(every_outbreak_incident_id))
        csvwriter.writerow(["incidentID"])
        for i in every_outbreak_incident_id:
            csvwriter.writerow([i])

    
if __name__=="__main__":
    ids=findweekIDs()
    output=readfields(ids)
    saveoutputtexts(output)
    out = BERTNER(output)
    saveNER(out)
    findpopular(out)



