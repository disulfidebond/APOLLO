#!/usr/bin/python env
from sklearn.feature_extraction.text import TfidfVectorizer
from pathlib import Path
from tqdm import tqdm
import csv
from datetime import datetime, date, timedelta
import calendar
from collections import Counter
import string


from NERPipeline import NERPipeline
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import statistics
import pandas as pd


from typing import List, Dict

import logging

logging.basicConfig(format='%(levelname)s :: %(filename)s :: %(funcName)s :: %(asctime)s :: %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# TODO: batched NER processing
# TODO: distributed processing

class ApolloDetector(object):
    """
    Detect frequent NERS in WEDSS data extracts per incident and report likely 
    outbreaks
    
    Public Methods:
    run_pipeline - configure and execute entire APOLLO NER Pipeline
    generate_stop_entities - configure and execute APOLLO NER to produce 
    stop entities

    Attributes:
    file_paths: dict of all WEDDS files being processed 
    output_path: directory to write all output files
    nlp_fields: Dictionary specifying WEDDS columns names to collect
    stop_entities: list of all NER entities to ignore due to commonality
    ids: a list of all incidentIDs being examined
    date_range: a list of dates that will be used in determining if an 
    incidentID in the patients file is valid for analysis.
    raw_wedss_text: dictionary of incidentID:all WEDDS text from NLP fields 
    separated by .'s.
    ner_results: {incidentID:[[all ners], [all ner types], [all ner scores]]}
    outbreaks: dictionaru of outbreak dictionaries by incidentID {incidentID:{'outbreakID ':###, 'OutbreakLocation':###, etc)
    """
    def __init__(self, data_folder:str, 
                 output_path:str,
                 nlp_fields_file:str,
                 stop_entities_file:str=False,
                 file_prefix:str='',
                 stop_entities_run=False):

        if not stop_entities_run:
            assert Path(data_folder).is_dir(), f'{self.data_folder.absolute()} is not a valid directory'
            self.file_paths = self.get_file_names(data_folder)
            
            self.output_path = Path(output_path)
            assert self.output_path.is_dir(), f'{self.output_path.absolute()} is not a valid directory'        
            
            nlp_fields_file = Path(nlp_fields_file)
            self.nlp_fields = self.get_nlp_fields(nlp_fields_file)
            
            if stop_entities_file:
                stop_entities_file = Path(stop_entities_file)
                assert stop_entities_file.exists(), f'{stop_entities_file.absolute()} does not exist'
                self.stop_entities = self.get_stop_entities_from_file(stop_entities_file)
            
            assert isinstance(file_prefix, str), f'{file_prefix} is not a str'
            # TODO: assert no char in file_prefix == ['<','>',':','\"','\\','/','|','?','*'] 
            if file_prefix:
                self.file_prefix = file_prefix + '_'
            else:
                self.file_prefix = file_prefix
            
            self.nlp = NERPipeline()

        if stop_entities_run:
            raise NotImplementedError()
 
        
    @staticmethod
    def get_file_names(data_folder: str) -> Dict:
        """
        gets list of all filepaths, plus patient file path separately, given directory of paths
        :param data_folder: str, path to data files
        :return: patient file path (Path object), list of all file paths
        """
        data_folder = Path(data_folder)
        assert data_folder.is_dir(), f'{data_folder.absolute()} is not a valid directory'
        all_file_paths = list(data_folder.glob(r'NLP_*.txt'))

        file_patterns = {
            'patient':'NLP_Patient.*\.txt',
            'outbreak':'NLP_Outbreak.*\.txt'
        }

        file_paths = {}
        for file, pattern in file_patterns.items():
            file_paths[file] = [x for x in all_file_paths if re.search(pattern, x.name)][0]
        
        file_paths['all'] = []
        for path in all_file_paths:
            file_paths['all'].append(path)
        logger.info(f"WEDDS data patient path: {file_paths['patient']}")
        return file_paths
    
    
    @staticmethod
    def get_nlp_fields(file_path:Path) -> Dict:
        """
        get all column names to collect WEDSS text from
        
        :param file_path: location of single column csv with column names
        :type file_path: Path
        :returns: {'header name':1}
        :type returns:
        """
        unique_field_names = {}
        with file_path.open("r", encoding='ISO-8859-1') as csvfile: 
            csvreader=csv.reader(csvfile,delimiter=",")
            for row_counter, row in enumerate(csvreader):
                field_name = row[0]
                if row_counter!=0:
                    unique_field_names[field_name]=1
        
        logger.info(f'read in {len(unique_field_names)} WEDSS column names to search for, e.g. {list(unique_field_names.keys())[0]}')
        return unique_field_names


    @staticmethod
    def get_stop_entities_from_file(entities_file:Path, 
                                    entity_col:str='Name',  #'\ufeffName', 
                                    freq_col:str='Frequency', 
                                    freq_limit:float=1.378) -> Dict:  # 1.7
        """
        load csv file with list of 'stop entities'
        load all rows for columns entity_col and freq_col to a dict {entity_col:freq_col}
        :param entities_file: Path to file
        :param entity_col: column in file with entities
        :param freq_col: column in file with entity frequencies
        :return: stop_entities Dict {entity_col: freq_col}
        """
        stop_entities = {}
        entity_col_index = None
        freq_col_index = None
        logger.debug(f'opening stop entities file {entities_file}')
        with entities_file.open("r", encoding='ISO-8859-1') as csvfile:
            csvreader=csv.reader(csvfile, delimiter=",")
            for row_counter, row in enumerate(csvreader):
                if row_counter == 0:  # if header row get indexes for the two columns we want
                    try:
                        logger.debug('stop entities file header row: %s', str(row))
                        entity_col_index = row.index(entity_col)
                        freq_col_index = row.index(freq_col)    
                    except (ValueError):
                        logger.error('Failed reading stop entities file %s: one of %s, %s, not found in %s', entities_file, entity_col, freq_col, str(row))
                else:
                    if float(row[freq_col_index].strip()) >= freq_limit:
                        stop_entities[row[entity_col_index]] = row[freq_col_index]
        logger.debug(f'loaded {len(stop_entities)} stop entities')
        stop_entities = list(stop_entities.keys())
        return stop_entities


    def dict_to_csv(self, d:Dict, 
                    columns:List, 
                    file_name:str, 
                    sep:str=',', 
                    add_date:bool = True, 
                    ext:str='.csv'):
        """
        use this func to write all output - will require dict transformation to key:List before use every time
        :param d: dictionary to write
        :param folder: path to write to
        :param file_name: name of file to write
        :param columns: list of column names to write in output
        :param sep: separator to use default=','
        :param date: include date between filename and extension default true
        :ext: file suffix default .csv but choose what you want e.g. .txt
        """       
        date_string = ''
        if add_date:
            date_string = '_'+datetime.strftime(datetime.now(),'%Y-%m-%d_%H_%M')
            
        write_path = self.output_path / (self.file_prefix + file_name + date_string + ext)
   
        with write_path.open("w") as csvfile:
            csvwriter=csv.writer(csvfile,delimiter=sep)
            # write header row
            csvwriter.writerow(columns)
            # write data
            for k, v in d.items():
                output_list = [k]                    
                if hasattr(v, '__iter__') and not isinstance(v,str):
                    for val in v:
                        output_list.append(val)
                else:
                    output_list.append(v)
                csvwriter.writerow(output_list)


    def set_date_range(self, day, period) -> List:
        """
        given a date, return it's year and week number
        :param: day: string in format "YYYY-MM-DD" or "YYYY-MM-DD <anything_else>"
        :param period: string specifying content of a list of valid date values (days) to return:
                            'week': all the dates in the calendar week for param day
                            'trailing_seven_days': the six days leading up to 'day' plus that day.
                            'month': all the days in the month of 'day'
                            'all': returns string 'all' rather than list of dates - date checker function 'date_in_range' knows what that means .
        :return: self.date_range = List of datetimes
        """
        try:
            d=day.split(" ")
            d=d[0]
            d=(datetime.strptime(d,'%Y-%m-%d'))
            d=d.date()
            logger.debug(f'day: {day}, period:{period}')
            if period == 'week':
                day_of_week = d.isocalendar()[2]-1
                start_date = d - timedelta(days=day_of_week)
                self.date_range = [start_date + timedelta(days=i) for i in range(7)]
            if period == 'trailing_seven_days':
                start_date = d
                self.date_range = [start_date - timedelta(days=i) for i in range(7)]  
            if period == 'month':
                number_of_days = calendar.monthrange(d.year,d.month)[1]
                self.date_range = [date(d.year, d.month, day) for day in range(1, number_of_days+1)]
            if period == 'all':
                self.date_range = 'all'                
            if period not in ['week','month','all','trailing_seven_days']:
                raise ValueError("period type must be 'week', 'month', trailing_seven_days' or 'all'")
        except (IndexError, ValueError):
            logger.error(f"Couldn't set date range with {type(d)}:{d} and {type(period)}:{period}")
           
            
    def date_in_range(self, date:str) -> bool:
        """
        helper function compare date to list of dates in range, return True if
        in range, else False. If period is 'all', always return True.
        :param date: plain text date in format "YYYY-MM-DD <stuff>"
        :return: True if date in range, else False. If can't convert date return False.
        """
        if self.date_range == 'all':
            return True
        try:
            d=date.split(" ")
            d=d[0]
            d=(datetime.strptime(d,'%Y-%m-%d'))
            d=d.date()
            if d in self.date_range:
                return True
            else:
                return False
                
        except (IndexError,ValueError):
            logger.warning(f"not a valid date value: {d}")
            return False


    def get_ids(self, target_date:str='', period:str='week', search_county:str='Dane',
                ids:List=None, scale_factor:int=None):
        """
        open patient file directly and get all incident IDS that are:
            in the week specified  
            in Dane county
            for confirmed cases
        :param target_date: date to use to define the month or week to pull ids from
        :param period: string, either 'week', 'month' or 'all', the period to pull dates around the target_date
        :param search_county: county to search in for incidents
        :param ids: list of ids to search for directly, skip looking for criteria matches in patient file.
        :param scale_factor: sub sample data - keeping on every nth patient row.
        :return: self.ids - dict of relevant incident ids {incident_id:1}
        """
        if ids:
            id_dict = {}
            for incidentID in ids:
                id_dict[incidentID] = 1
            self.ids = id_dict
            
        if not ids:
            with self.file_paths['patient'].open('r', encoding='ISO-8859-1') as f: 
                f.seek(0)
                lines=f.readlines()
                logger.debug(f'target_date: {target_date}')
                logger.debug(f'period: {period}')
                # establish dates of interest
                self.set_date_range(target_date, period)
                logger.debug(f'searching from {self.date_range[0]} to {self.date_range[-1]}')
                
                # for every line in patient file (not including header row), where:
                  # EpisodeDate not empty string and in specified week
                  # county=Dane
                  # resolutionStatus contains 'confirmed'
                # add that to list of incident ids
                confirmed_counter = 0
                any_counter = 0
                ids={}
                usable_resolution_statues = ["Confirmed", "Probable"]
                for count, line in enumerate(lines):
                    header_row = False
                    if count == 0:
                        header_row = True
                    ls=line.split("|")
                    incidentID = ls[0]
                    episode_date = ls[18]
                    county = ls[8]
                    resolution_status = ls[33]
                    
                    if not header_row\
                        and episode_date!=''\
                        and self.date_in_range(episode_date)\
                        and county==search_county:
                            
                        any_counter += 1
                        
                        if resolution_status.strip() in usable_resolution_statues:       
                            ids[incidentID]=1
                            confirmed_counter+=1
                if scale_factor:
                    original_len = len(ids)
                    ids = {k:v for k,v in ids.items() if k in list(ids.keys())[::scale_factor]}
                    scaled_len = len(ids)
                    print(f'scaled ids from {original_len} to {scaled_len}')
                print(f'total any status cases, in Dane county, in week of specified date:{any_counter}')            
                print(f'total confirmed cases, in Dane county, in week of specified date:{confirmed_counter}')
                self.ids=ids

                   
    def get_text_from_wedds_files(self):
        """
        This function gets the list of incidentIDs and concatenates 
        all related fields in all files for each of them
        :param ids: dict of {incidentID:1}
        :return: self.raw_wedss_text dict {incidentID:"all.values.associated.with.that.incident.in.all.files"}
        """
        self.raw_wedss_text={}
       
        # iterate over all files in files list, 
        logger.info(f"reading in WEDSS files: {self.file_paths['all']}")
        for file in tqdm(self.file_paths['all']):
            with file.open('r', encoding='ISO-8859-1') as f:
                f.seek(0)
                lines=f.readlines()
                hdic={}
                
                # iterate over every line in a file
                for counter, line in tqdm(enumerate(lines)):
                    header_row = False
                    if counter == 0:
                        header_row=True
                    ls=line.split("|")
                    incident_id = ls[0]
                    
                    # determine which columns are relevant in this file, make dict hdic
                    # for the first line in a file add all values to hdic {'header val':1}
                    # IF those header values in the NLPFields.csv file, or contain "_Sec".
                    if header_row:
                        for i in range(0,len(ls)):
                            if ls[i] in self.nlp_fields or "_Sec" in ls[i]:
                                hdic[i]=1
                                
                    # for non-header/first file rows, if row has id in list of ids passed to fn
                    # concatenate all values of all columns in that file that were in the NLPFields.csv file
                    # concatenated into a string separated by '.'. 
                    else:
                        tmp=''
                        if incident_id in self.ids:
                            for key in hdic:
                                try:
                                    tmp=tmp+'|'+ls[key]
                                except:
                                    pass
                            
                            # if incidentID its not in self.raw_wedss_text dict already, 
                            # append it and associated text, else just extend text.
                            if incident_id not in self.raw_wedss_text:
                                self.raw_wedss_text[incident_id]=tmp
                            else:
                                self.raw_wedss_text[incident_id]+=tmp 
                                
        self.dict_to_csv(self.raw_wedss_text, 
                         columns=["IncidentID","Text"], 
                         file_name="wedds_text_per_incident")
        
    def wedss_text_filter(self) -> Dict:
        """
        filter meaningless input before NER pipeline
        filters out incidents with no text, WEDSS timestamps where there is 
        text 
        :return: filtered raw_wedss_text Dict
        """
        processed_wedss_text = {}
        for k, v in self.raw_wedss_text.items():
            # skip incidents with no text
            if not re.match(r"^[|]+(\r\n|\r|\n)[|]+$", v):
                # remove WEDSS timestamps where there is text
                v = re.sub(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d?\d (19|20)\d\d \d\d:\d\d:\d\d GMT-0\d00 \(Central (Standard|Daylight) Time\)", 
                           '', 
                           v)
                # remove |Y(|Y|Y|Y...) from input text
                v = re.sub(r"(\|[Y])+", '', v)
                
                # remove contact tracers names: ORGLast, First 
                v = re.sub(r"MAXIM\w+, \w+,", '', v)
                v = re.sub(r"UHS\w+, \w+,", '', v)
                processed_wedss_text[k] = v
        
        return processed_wedss_text


    def BERTNER(self, output:Dict, stats_output=False) -> Dict:
        """
        run BERT NER pipeline on text for every incident id
        :param output: dict {incidentID: "all text from NLPFields.csv fields concatenated"}
        :return: dict {incidentID: [[Names], [Types], [Scores]]}
        """
        out={}
        print('kick off NER processing...')
        for key, actual_text in tqdm(output.items()):
            out[key] = self.nlp.ner_over_chunks(actual_text)
            # progress(out)
        
        if stats_output:
            self.dict_to_csv(self.nlp.get_token_stats(),
                             columns=['stat','value'],
                             file_name='token_statistics')
       
        # # return(dask.compute(out)[0]) 
        return(out)
 
    
    def process_text_for_entities(self):
        """
        process raw text per incident ID to NER results per incidentID
        """
        processed_wedss_text = self.wedss_text_filter()
        self.ner_results = self.BERTNER(processed_wedss_text, stats_output=True)
        self.dict_to_csv(self.ner_results,
                         columns = ["IncidentID","Names","Types","Scores"],
                         file_name="ner_results")

    def load_all_outbreak_data(self):
        """
        load all outbreak data for any outbreak row with incidentID in report 
        incidentIDs list
        create self.outbreaks attribute - dict of dicts:
        {'IncidentID':{'OutbreakID ':###,'Outbreak#':##,'OutbreakLocation':##,
                       'OutbreakLocationType':##,'OutbreakLocationAddress': ##,
                       'OutbreakCreateDate':# ,'OutbreakLocationJurisdiction': #,
                       'OutbreakProcessStatus': #,'OutbreakResolutionStatus': #}}
        """
        # TODO: basic data integrity checks - column names, row counts
        incident_id_column_name = 'IncidentID' 
        incident_id_index = None
        self.outbreaks = {}

        with self.file_paths['outbreak'].open('r', encoding='ISO-8859-1') as csvfile:
            csvreader=csv.reader(csvfile, delimiter='|')
            name_to_index = {}
            for row_counter, row in enumerate(csvreader):
                if row_counter == 0:
                    try:
                        logger.info('opened outbreak_id file, header row: %s', str(row))
                        incident_id_index = row.index(incident_id_column_name)
                        column_names = [x for x in row if x != incident_id_column_name]
                        for name in column_names:
                            name_to_index[name] = row.index(name) 

                    except (ValueError):
                        logger.error(f"failed loading outbreak file: {self.files_paths['outbreaks']}: column_names:{column_names})")

                else:
                    incident_id = row[incident_id_index]                    
                    if (incident_id != '') and (incident_id in self.ids):
                        self.outbreaks[incident_id] = {}
                        for column_name in column_names:
                            self.outbreaks[incident_id][column_name] = row[name_to_index[column_name]]
                    
                    
    def find_associated_known_outbreak_ids(self, namedic:Dict) -> Dict:
        """
        for a given dictionary of NER entities and associated incidentIDs find all known outbreak IDs associated with that key and incidentIDs
        :param namedic: Dict {NERentity: [incident_id, incident_id, ...}
        :type namedic: Dict
        :return: {NERentity: [outbreak_id, outbreak_id, ...}
        :type return: Dict
        """
        self.load_all_outbreak_data()  # populates: self.outbreaks
        # relevant column names in outbreak_id file
        incident_id_column_name = 'IncidentID' 
        outbreak_id_column_name = 'Outbreak#'
        outbreak_location_column_name = 'OutbreakLocation'
        
        incident_id_index = None
        outbreak_id_index = None
        location_name_index = None

        incidents_to_outbreaks = {}
        outbreaks_to_ners = {}
        
        self.outbreak_stats = {}
        
        outbreak_match_ratio = 66

        # get all outbreaks in outbreak file
        # TODO: refactor with self.outbreaks from self.load_all_outbreak_data() to avoid reading outbreaks file twice
        with self.file_paths['outbreak'].open("r", encoding='ISO-8859-1') as csvfile:
            csvreader=csv.reader(csvfile, delimiter="|")
            for row_counter, row in enumerate(csvreader):
                if row_counter == 0:  # if header row get indexes for the two columns we want
                    try:
                        logger.info('opened outbreak_id file, header row: %s', str(row))
                        incident_id_index = row.index(incident_id_column_name)
                        outbreak_id_index = row.index(outbreak_id_column_name)
                        location_name_index = row.index(outbreak_location_column_name)
                    except (ValueError):
                        logger.error('Failed reading outbreak file %s: one of %s, %s, %s, not found in %s', self.file_paths['outbreak'], incident_id_column_name, outbreak_id_column_name, outbreak_location_column_name, str(row))
                else:
                    # map incidentID to outbreakID - only for incident IDs that we're looking at (in out Dict)
                    incident_id = row[incident_id_index]
                    if incident_id in self.ids:
                        outbreak_id = row[outbreak_id_index]
                        incidents_to_outbreaks[incident_id] = outbreak_id  # this is essentially the self.outbreaks dict!
                        
                        # map outbreakID to pre-NER string incorporating outbreak location information - typically organization name
                        # id structure typically all caps YEAR-COUNTY BUSINESS NAME | split into: year County. Business Name. Location
                        # TODO: drop year County here - never informative.
                        outbreak_location = row[location_name_index]
                        preprocessed_outbreak_id = outbreak_id.replace('-',' ').title().split()    
                        preprocessed_outbreak_id.insert(2, '.')
                        preprocessed_outbreak_id = ' '.join(preprocessed_outbreak_id) + '. ' + outbreak_location
                        outbreaks_to_ners[outbreak_id] = preprocessed_outbreak_id
                    else:
                        pass
           
        # replaced preprocessed_outbreaks_ids with NER results
        # dict of outbreakbreakidL NERS for htat outbreak - for all outbreaks that can be linked to a current/relevant incidentID
        outbreaks_to_ners = self.BERTNER(outbreaks_to_ners)

        # limit NER results to just the entities, remove the types and scores.
        # TODO: confirm filtering still necessary here if ((x!="") and (x[0]!="#"), move into NERPipeline
        for outbreak_id, ner_result in outbreaks_to_ners.items():
            outbreaks_to_ners[outbreak_id] = [x for x in outbreaks_to_ners[outbreak_id][0] if ((x!="") and (x[0]!="#"))]
        
        self.outbreak_stats['associated outbreak count'] = len(outbreaks_to_ners)
        
        # for all entities found by the NER pipeline look at WEDDS outbreak data
        # outbreak_ids_by_entity = {}  #{ entity:[list of outbreak ids]}
        outbreak_data_by_entity = {}  #{ entity: [lsit of outbrek dicts]}
        for key in namedic:
            if key!="" and key[0]!="#":
                # set up empty list to append to
                # outbreak_ids_by_entity[key] = []
                outbreak_data_by_entity[key] = []
                # get all incident ids associated with each NER entity/key
                incident_id_strs = [str(x).strip() for x in namedic[key]]
                for incident_id in incident_id_strs:
                    try:
                        # if an outbreak NER entity associated with an incident ID matches the key - store that outbreak ID
                        for entity in outbreaks_to_ners[incidents_to_outbreaks[incident_id]]:
                            # if key.strip() == entity.strip():
                            if fuzz.token_sort_ratio(entity, key) >= outbreak_match_ratio:
                                # logger.debug(f"key: {key}, incident_id:'{incident_id}'")
                                # outbreak_ids_by_entity[key].append(incidents_to_outbreaks[incident_id])
                                if self.outbreaks[incident_id]:
                                    outbreak_data_by_entity[key].append(self.outbreaks[incident_id]) # [{}, {}, {}] i.e. append a dictionary.
                                break
                            else:
                                pass
                                # outbreak_ids_by_entity[key].append('no match')
                                
                    except (KeyError):
                        # logger.debug('entity %s not found in outbreak id search process', key)
                        # outbreak_ids_by_entity[key].append('no match')
                        pass
        
        # for outbreak in outbreak_ids_by_entity:
       
        # reporting stats for outbreak matching
        # unique_outbreaks = {}

        # for entity, outbreak_ids in outbreak_ids_by_entity.items():
        #     for outbreak in outbreak_ids:
        #         if outbreak in unique_outbreaks:
        #             unique_outbreaks[outbreak] += 1
        #         else:
        #             unique_outbreaks[outbreak] = 1
        
        unique_outbreaks_data = {}
        for entity, outbreak_data in outbreak_data_by_entity.items():
            for outbreak in outbreak_data:
                outbreak_id = outbreak['Outbreak#']
                if outbreak_id in unique_outbreaks_data:
                    unique_outbreaks_data[outbreak_id] += 1
                else:
                    unique_outbreaks_data[outbreak_id] = 1

        # self.outbreak_stats['unique outbreaks matched to NERS'] = len(unique_outbreaks)
        self.outbreak_stats['unique outbreaks matched to NERS from data_dict'] = len(unique_outbreaks_data)
        print(self.outbreak_stats)
        
        self.dict_to_csv(outbreaks_to_ners, ['outbreak_id','NERs'], 'outbreaks_to_ners')
        self.dict_to_csv(unique_outbreaks_data, ['outbreak_id', 'count'], 'unique_outbreaks')
        self.dict_to_csv(self.outbreak_stats, ['outbreak_stat', 'value'], 'outbreak_stats')
        return outbreak_data_by_entity  # outbreak_ids_by_entity, 

    
    def fuzzy_match_entities(self, all_keys:Dict, 
                             scorer=fuzz.token_sort_ratio,
                             match_cutoff:int=70) -> Dict:
        """
        Recursively match all keys agains each other until no more matches.
        fuzzy match all keys in output dict then return collated results in a dict
        :param all_keys: Dict, structure {'entity to fuzzy match':'list of associated values'}
                        all_keys[key] = [typedic[key],
                                countdic[key],
                                sum(scoredic[key])/len(scoredic[key]),
                                incident_id_strs,
                                outbreak_ids_by_key[key]]
        :type all_keys: Dict
        :param match_cutoff: fuzzywuzzy token_sort_ratio minimum value for match, default = 70
        :type match_cutoff: int
        :return: Dict {'matched entities (str)': List of Lists of collated values}
        """
        matched_keys={}
        key_list = list(all_keys.keys())
        for key in key_list:
            # for each entity in all_keys
            # fuzzy match that entity against all other entities
            # for those matches above threshold
            # collect those together
           
            # print(f'fuzzy_match key: {key}')
            matched_keys_scores = process.extract(key, 
                                                  all_keys.keys(), 
                                                  scorer=scorer)
            # print(f'matched_keys_scores: {matched_keys_scores}')
            matched_keys[key] = [matched_key 
                                 for matched_key, score 
                                 in matched_keys_scores 
                                 if score >= match_cutoff]
            # print(f'matched_keys:{matched_keys}')
        
        
        # for each matched entity in that list:
        #    {key: [match, match, ...]}
        # collect results together
        # remove that entity from the overall list    
        # logger.debug(f'fuzzy matched keys: {matched_keys}')

            
        # create new dict that key is all matched entities: 
        #        {[bab, bac, bad]: [[],[],[]],[[],[],[]],..}
        # for all matched entities aggregate dict contents
        # add all entities to a collected list - new matches can't be in that list
        already_matched = []        
        collected_entities = {}
        for entity, matches in matched_keys.items():
            if entity not in already_matched:
                new_key = ','.join(matches)  # first match is always == the key.
                collected_entities[new_key] = []
                for match in matches:
                    collected_entities[new_key].append(all_keys[match])
                already_matched.extend(matches)
        # print(f'already matched: {already_matched}')
        # print(f'collected_entities: {collected_entities}')

        # 'e': [
        #     ['Location', 1, 0.09659047722816468, ['10845169'], []], 
        #     ['Location', 2, 0.16888469060262043, ['10930216', '10622584'], []], 
        #     ['Location', 4, 0.17528001649383168, ['10930216', '10622584', '10536812', '10845169'], []], 
        #     ['Location', 2, 0.16757280040871014, ['10930216', '10622584'], []]
        # ],

        # # create empty list of lists to append to for aggregating entities
        # collected_entities_key = list(collected_entities.keys())[0]
        # new_value = [[] for x in collected_entities[collected_entities_key][0]]
        # print(f'new_value: {new_value}')
        
        
        # for all matched entities, merge their types, iterations, scores,
        # and outbreaks into one list of lists.
        collated_entities = {}
        for key, results in collected_entities.items():
            collator = []
            for j, single_entity_result in enumerate(results):
                if j == 0:
                    collator = [[x] for x in single_entity_result]
                else:
                    for i, result_datum in enumerate(single_entity_result):
                        collator[i].append(result_datum)
            collated_entities[key] = collator
        return collated_entities
    
    
    def break_out_ner_results_per_entity(self):
        """
        create four dictionaries from self.ner_results
        countdic a count of every entity 
        namedic every entity and each incidentID it is named in
        typedic every entity and its type (Org, Person, etc)
        scoredic every entity and it's score in every incidentID it was found in

        :return:  typedic, scoredic, countdic, namedic
        :return type: Dict, Dict, Dict, Dict
        """
        countdic={}  # {entity: count}
        namedic={}  # {entity: [incidentID, IncidentID, ...]}
        typedic={}  # {entity: ner_type}
        scoredic={}  # {entity:[score, score..]}
        # for all incidentIDs (e.g. relevant text at this point), get entities, types and scores
        # self.ner_results = {incident_id:[[entities], [types], [scores]]}
        # for every entity
        #    add entity to typedic with {entity: ner_type}
        #    add score to scoredic with {entity:[score, score..]]
        #       if entity already present - append score to existing scores
        for key, bertner_output in self.ner_results.items():
            entities=bertner_output[0]
            entity_type=bertner_output[1] 
            score=bertner_output[2]
            for i in range(0,len(entities)):
                tidy_entity = entities[i].strip()
                if tidy_entity not in typedic:
                    typedic[tidy_entity]=entity_type[i]
                if tidy_entity not in scoredic:
                    scoredic[tidy_entity]=[float(score[i])]
                else:
                    m=scoredic[tidy_entity]
                    m.append(float(score[i]))
                    scoredic[tidy_entity]=m

        # for all incidentIDs (relevant text)
        # get all entities
        #   add every entity to namedic {entity:[IncidentID]}
        #   add every entity to countdic {entity:1}
        #   each time an existing entity is found:
        #       increment value for entity in countdic 
        #       append incidentID to entity key list in namedic 
        for key, bertner_output in self.ner_results.items():
            for item in set(bertner_output[0]):
                tidy_item = item.strip()
                if tidy_item not in namedic:
                    namedic[tidy_item]=[key]
                    countdic[tidy_item]=1
                else:
                    countdic[tidy_item]+=1
                    l=namedic[tidy_item]
                    l.append(key)
                    namedic[tidy_item]=l

        return typedic, scoredic, countdic, namedic
    
        
    def collate_entities_into_output_format(self, typedic, scoredic, countdic, namedic, outbreak_data_by_key) -> Dict:  #outbreak_ids_by_key
        """
        collate all results into output format: {entity:[type, count, score, incidentids, outbreakids] }
        :param countdic: {entity: count}
        :type  countdic: Dict
        :param namedic: {entity: [incidentID, IncidentID, ...]}
        :type namedic: Dict
        :param typedic:  {entity: ner_type}
        :type typedic: Dict
        :param scoredic: {entity:[score, score..]}
        :type typedic: Dict
        :param outbreak_ids_by_key: {entity: [outbreak_id, outbreak_id, ...}
        :type outbreak_ids_by_key: Dict
        :param outbreak_data_by_key: {entity: {outbreak_data}, {outbreak_data}, ...}
        :type outbreak_data_by_key: Dict
        :return: {entity:[type, count, score, incidentids, outbreakids] 
        :type return: Dict
        """
        # TODO: move Person entity type filter into the NERPipeline
        all_keys = {}
        
        namedic = self.apply_stop_entities_rules(namedic)
        
        for key in namedic:
            if key!="" \
               and key[0]!="#" \
               and typedic[key] != 'Person' \
               and key.strip() not in self.stop_entities:

                incident_id_strs = [str(x).strip() for x in namedic[key]] 
                
                # collect all entities and associated information
                # print(f"outbreak_data_by_key[{key}]:{outbreak_data_by_key[key]}")
                all_keys[key] = [
                    typedic[key], # entity type
                    countdic[key], # entity count
                    sum(scoredic[key])/len(scoredic[key]),
                    incident_id_strs,
                    # outbreak_ids_by_key[key],
                    [x['Outbreak#'] for x in outbreak_data_by_key[key]],  # outbreak_ids
                    [x['OutbreakID '] for x in outbreak_data_by_key[key]],
                    [x['OutbreakLocation'] for x in outbreak_data_by_key[key]],
                    [x['OutbreakProcessStatus'] for x in outbreak_data_by_key[key]]
                ]
        return all_keys
    
            
    def create_report(self, test_incident_entity_fuzzy_match_configs=False):
        """
        take NERPipeline output (dict {incidentID: [[Names], [Types], [Scores]]}) and convert to per entity output:
            for every entity: entity name, entity type, number of times found, 
            average score, list of incidentID where it was found
        write results to CSV
        also output a list of unqiue incidient IDs in the file
        also check for any existing outbreakids associated with every entity and incidentID
            if they exist, append list per entity
        """
        typedic, scoredic, countdic, namedic = self.break_out_ner_results_per_entity()       
        outbreak_data_by_key = self.find_associated_known_outbreak_ids(namedic) 

        all_keys = self.collate_entities_into_output_format(typedic, scoredic, countdic, namedic, outbreak_data_by_key)
        all_keys = self.fuzzy_match_entities(all_keys, scorer=fuzz.token_set_ratio, match_cutoff=70)
        all_keys = self.tidy_fuzzy_matched_results(all_keys)

        self.dict_to_csv(all_keys,
                  columns=["Name","Type","Iterations","Score", "Incidents", 
                           "Outbreaks", "OutbreakIDs", "Outbreak Locations", 
                           "Outbreak Process Statuses"],
                  file_name="final_report")

        if test_incident_entity_fuzzy_match_configs:
            match_cutoffs = range(60,81,5)
            print(f'match_cutoff: {list(match_cutoffs)}')
            scorers = [{'name':'ratio', 'scorer':fuzz.ratio},
                       {'name':'partial_ratio', 'scorer':fuzz.partial_ratio},
                       {'name':'token_sort_ratio', 'scorer':fuzz.token_sort_ratio},  # was using this one originally at 70
                       {'name':'token_set_ratio', 'scorer':fuzz.token_set_ratio}]
            
            results_dict = {'scorer_name':[], 
                            'match_cutoff':[],
                            'number_of_keys':[],
                            'mean_key_len':[],
                            'median_key_len':[],
                            'min_key_len':[],
                            'max_key_len':[]}
            for scorer in scorers:
                for cutoff in match_cutoffs:
                    
                    matched_keys = self.fuzzy_match_entities(all_keys, 
                                                             scorer=scorer['scorer'], 
                                                             match_cutoff=cutoff)
                   
                    results_len = len(matched_keys)
                    key_lens =  []
                    for k in matched_keys:
                        # k is all matched keys
                        key_lens.append(len(k))
                    
                    mean_key_len = sum(key_lens)/len(key_lens)
                    median_key_len = statistics.median(key_lens)
                    min_key_len = min(key_lens)
                    max_key_len = max(key_lens)
                    results_dict['scorer_name'].append(scorer['name'])
                    results_dict['match_cutoff'].append(cutoff)
                    results_dict['number_of_keys'].append(results_len)
                    results_dict['mean_key_len'].append(mean_key_len)
                    results_dict['median_key_len'].append(median_key_len)
                    results_dict['min_key_len'].append(min_key_len)
                    results_dict['max_key_len'].append(max_key_len)
       
                    
                    self.dict_to_csv(matched_keys,
                                     columns=["Name","Type","Iterations","Score", "Incidents", "Outbreaks"],
                                     file_name=f"final_report_scorer_{scorer['name']}_mc_{cutoff}")
            
            df = pd.DataFrame.from_dict(results_dict)
            df.to_csv(self.output_path / "scorer_performance_analysis.csv", index=False)


    def flatten(self, lol:List) -> List:
        """
        take any list of lists (of lists), return a single list with all values
        :param lol: List of lists
        :return: a flat list
        """
        if isinstance(lol, list):
            return [a for i in lol for a in self.flatten(i)]
        else:
            return [lol]
   
    
    def tidy_fuzzy_matched_results(self, fuzzy_matches:Dict) -> Dict:
        """
        combine results to make them more readable
        turn list of entities in to just entity with highest iteration count in data
        turn list of Types into just the type of the entity with the highest iteration count
        turn list of iterations into sum of all iterations for those/that entity
        turn list of scores into mean of all scores for those entities
        turn list of incidents lists into a flat list of unique incident ids
        turn list of outbreaks into a flat list of unique outbreaks
        :param fuzzy_matches: {'entities list as string':
                               [list of types, 
                                list of entity iterations, 
                                list of entity average scores, 
                                list of incidents,  
                                list of outbreaks]}
        :type fuzzy_matches:Dict
        :return: {entity as key:[main_type, summed_iterations, average_score, incidents, outbreaks]}
        :type return: Dict
        """
        processed_output = {}
        for k,v in fuzzy_matches.items():
            # set up variable names
            # outbreaks=None
            entities = k.split(',')
            types = self.flatten(v[0])
            iterations = self.flatten(v[1])
            scores = self.flatten(v[2])
            # incidents=list(set(self.flatten(v[3])))
            incidents=self.flatten(v[3])
            # try:
            #     # outbreaks=list(set(self.flatten(v[4])))
            #     outbreaks=self.flatten(v[4])
            # except(IndexError):
            #     outbreaks = []
            try:
                outbreak_names=self.flatten(v[4])
            except(IndexError):
                outbreak_names = None
            try:
                outbreak_ids=self.flatten(v[5])
            except(IndexError):
                outbreak_ids = None
            try:
                outbreak_locations=self.flatten(v[6])
            except(IndexError):
                outbreak_locations = None
            try:
                outbreak_process_statuses=self.flatten(v[7])
            except(IndexError):
                outbreak_process_statuses = None
                
            # get most common entity index
            max_iteration = max(iterations)
            max_index = iterations.index(max_iteration)
            
            # get each value corresponding with most common entity, sum iterations, avg scores
            main_entity = entities[max_index]
            main_type = types[max_index]
            summed_iterations = sum(iterations)
            average_score = sum(self.flatten(scores))/len(self.flatten(scores))
            incidents = self.flatten(incidents)

            # check if outbreak locations is just empty strings
            empty_count = 0           
            for x in outbreak_locations:
                if not x:
                    empty_count +=1
            if empty_count == len(outbreak_locations):
                outbreak_locations = None
            
            processed_output[main_entity] = [
                main_type, summed_iterations, average_score, incidents,  # outbreaks
                outbreak_names, outbreak_ids, outbreak_locations,
                outbreak_process_statuses
                ]
        
        # sort dictionary to order by iterations largest to smallest.
        processed_output = {k: v for k, v in sorted(processed_output.items(),
                                                   key=lambda item: sum(self.flatten(item[1][1])),
                                                   reverse=True)}
        # only return results if the count is greater than 1.
        processed_output = {k: v for k, v in processed_output.items() if v[1]>1}
        return processed_output
    
    @staticmethod
    def apply_stop_entities_rules(entities: Dict) -> Dict:
        """
        apply rules for entities processing to Dict of entities.
        no entities 3 chars or less
        no entities that are street names like 'word St' or 'token Drive'
        """
        updated_entities = {k: v for k, v in entities.items() if len(k) > 3}
        updated_entities = {k: v for k, v in updated_entities.items()
                            if not re.match(r"\w+ (Road|Rd|Street|St|Avenue|Ave|Drive|Dr)$", ' '.join(k.split()[-2:]))}
        return updated_entities
    
    def create_stop_entities(self, min_df:float=5, specific_terms:List=None):
        """
        define stop words by document frequency
        
        :param min_df: minimum doc frequency in percent for an entity to be counted as a stop word
        :param type: float
        """
        # if no stop entities file loaded, create them from the text data
        if not self.stop_entities:
            # get all entities from all incident ids
            just_entities = {k:list(set(v[0])) for k,v in self.ner_results.items()}
            # print(f'just_entities: {just_entities}')
            entities = self.flatten(list(just_entities.values()))
            # print(f'entities: {entities}')
            document_count = len(just_entities)
            # print(f'doc_count: {document_count}')
            # get counts with collections
            entity_counts = Counter(entities)
            # print(f'entity_counts: {entity_counts}')
            # change counts to frequencies (%)
            entities_by_doc_frequency = {k:v/document_count*100 for k,v in entity_counts.items()}
            # print(f'entities_by_doc_frequency: {entities_by_doc_frequency}')
            # filter by min_df
            entities_by_doc_frequency = {k:v for k,v in entities_by_doc_frequency.items() if v >= min_df}
            # print(f'filtered entities_by_doc_frequency: {entities_by_doc_frequency}')
            # order by frequency descending
            entities_by_doc_frequency = {k: v for k,v in sorted(entities_by_doc_frequency.items(),
                                               key=lambda item: item[1],
                                               reverse=True)}
            # print(f'sorted entities_by_doc_frequency: {entities_by_doc_frequency}')
            # dump to dict
            self.dict_to_csv(entities_by_doc_frequency, 
                             ['Name', 'Frequency'], 
                             f'stop_entities_list_min_df_{min_df}_pc')
            
            stop_entities = list(entities_by_doc_frequency.keys())
            # print(f'stop_entities: {stop_entities}')
            
            self.stop_entities = stop_entities
            
        # exclude list of specific terms in all cases
        if not specific_terms:
            wi_cities = [
                'Abbotsford','Adams','Algoma','Alma','Altoona','Amery','Antigo','Appleton','Arcadia','Ashland','Augusta','Baraboo','Barron','Bayfield','Beaver Dam','Beloit','Berlin','Black River Falls','Blair','Bloomer','Boscobel','Brillion','Brodhead','Brookfield','Buffalo City','Burlington','Cedarburg','Chetek','Chilton','Chippewa Falls','Clintonville','Colby','Columbus','Cornell','Crandon','Cuba City','Cudahy','Cumberland','Darlington','Delafield','Delavan','De Pere','Dodgeville','Durand','Eagle River','Eau Claire','Edgerton','Elkhorn','Elroy','Evansville','Fennimore','Fitchburg','Fond du Lac','Fort Atkinson','Fountain City','Fox Lake','Franklin','Galesville','Gillett','Glendale','Glenwood City','Green Bay','Green Lake','Greenfield','Greenwood','Hartford','Hayward','Hillsboro','Horicon','Hudson','Hurley','Independence','Janesville','Jefferson','Juneau','Kaukauna','Kenosha','Kewaunee','Kiel','La Crosse','Ladysmith','Lake Geneva','Lake Mills','Lancaster','Lodi','Loyal','Madison','Manawa','Manitowoc','Marinette','Marion','Markesan','Marshfield','Mauston','Mayville','Medford','Mellen','Menasha','Menomonie','Mequon','Merrill','Middleton','Milton','Milwaukee','Mineral Point','Mondovi','Monona','Monroe','Montello','Montreal','Mosinee','Muskego','Neenah','Neillsville','Nekoosa','New Berlin','New Holstein','New Lisbon','New London','New Richmond','Niagara','Oak Creek','Oconomowoc','Oconto','Oconto Falls','Omro','Onalaska','Oshkosh','Osseo','Owen','Park Falls','Peshtigo','Pewaukee','Phillips','Pittsville','Platteville','Plymouth','Port Washington','Portage','Prairie du Chien','Prescott','Princeton','Racine','Reedsburg','Rhinelander','Rice Lake','Richland Center','Ripon','River Falls','St. Croix Falls','St. Francis','Schofield','Seymour','Shawano','Sheboygan','Sheboygan Falls','Shell Lake','Shullsburg','South Milwaukee','Sparta','Spooner','Stanley','Stevens Point','Stoughton','Sturgeon Bay','Sun Prairie','Superior','Thorp','Tomah','Tomahawk','Two Rivers','Verona','Viroqua','Washburn','Waterloo','Watertown','Waukesha','Waupaca','Waupun','Wausau','Wautoma','Wauwatosa','West Allis','West Bend','Westby','Weyauwega','Whitehall','Whitewater','Wisconsin Dells','Wisconsin Rapids'
                ]
            wi_counties = [
                'Adams','Ashland','Barron','Bayfield','Brown','Buffalo','Burnett','Calumet','Chippewa','Clark','Columbia','Crawford','Dane','Dodge','Door','Douglas','Dunn','Eau Claire','Florence','Fond du Lac','Forest','Grant','Green','Green Lake','Iowa','Iron','Jackson','Jefferson','Juneau','Kenosha','Kewaunee','La Crosse','Lafayette','Langlade','Lincoln','Manitowoc','Marathon','Marinette','Marquette','Menominee','Milwaukee','Monroe','Oconto','Oneida','Outagamie','Ozaukee','Pepin','Pierce','Polk','Portage','Price','Racine','Richland','Rock','Rusk','Sauk','Sawyer','Shawano','Sheboygan','St. Croix','Taylor','Trempealeau','Vernon','Vilas','Walworth','Washburn','Washington','Waukesha','Waupaca','Waushara','Winnebago','Wood County'
                ]
            us_states = [
                'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware','Florida',
                'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana','Iowa','Kansas','Kentucky','Louisiana','Maine','Maryland',
                'Massachussetts','Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire',
                'New Jersey','New Mexico','New York','North Carolina','Norht Dakota','Ohio','Oklahoma','Oregon','Pennsylvania',
                'Rhode Island','South Carolina','South Dakota','Tennessee','Texas','Utah','Vermont','Virginia','Washington',
                'West Virginia','Wisconsin','Wyoming'
                ]

            
            # any terms two chars or less
            all_single_chars_lower = list(string.ascii_lowercase)
            all_single_chars_upper = list(string.ascii_uppercase)
            all_two_chars_lower = [y + z for y in all_single_chars_lower 
                                             for x in all_single_chars_lower
                                             for z in all_single_chars_lower]
            all_two_chars_upper = [x.upper() for x in all_two_chars_lower]
            all_two_chars_title = [x.title() for x in all_two_chars_lower]
            all_two_chars = all_single_chars_lower + all_single_chars_upper +\
                            all_two_chars_lower + all_two_chars_upper +\
                            all_two_chars_title
      
            specific_terms = wi_cities + wi_counties + us_states + all_two_chars
                    
      
        # print(f'specific terms: {specific_terms}')
        self.stop_entities.extend(specific_terms)
        # print(f'stop_entities: {stop_entities}')        
        
        
        if not self.stop_entities:
            print('NO STOP ENTITIES FOUND!')
            stop_entities = ['|']
        
        
    @staticmethod
    def identity_tokenizer(pre_tokenized_text):
        return pre_tokenized_text
    
           
    def filter_entities_by_tfidf_score(self, threshold=0.5):
        """
        filter {incidentID: [entity, entity, entity]} for high scoring entities by TFIDF
        # use tfidf - for each doc (incident text), keep only terms that exceed threshold
        # dump terms that don't meet threshold of 'interest' to a dict per incident
        # decode each qualifying term (entity) from the feature vector
        # use those as the entities for that incident
        :param ner_results: dicitonary of entities to filter
        :type ner_results: Dict
        :param threshold: tfidf score to filter - keep entities scoring higher
        :type threshold: float (0.0-1.0)
        :return: {incidentID: [entity, entity, entity]}
        :type return: Dict, Dict`
        """
        just_entities = {k:v[0] for k,v in self.ner_results.items()}
        just_types = {k:v[1] for k,v in self.ner_results.items()}
        just_scores = {k:v[2] for k,v in self.ner_results.items()}
        
        keepers = {}
        stoppers = {}
        types = {}
        scores = {}
        
        tv = TfidfVectorizer(tokenizer=self.identity_tokenizer,
                             lowercase=False,
                             stop_words=None)
        tfidf_matrix = tv.fit_transform(just_entities.values())
        feature_matrix = tfidf_matrix.todense()
        feature_names = tv.get_feature_names()
        key_list = list(self.ner_results.keys())
        rows_and_keys = list(zip(range(len(key_list)), key_list))
        for row, k in rows_and_keys:
            fmr = self.flatten(feature_matrix[row].tolist())
            scores_and_features = list(zip(fmr, feature_names))
            doc_keepers = []
            doc_stoppers = []
            doc_types = []
            doc_scores = []
            for score, feature in scores_and_features:
                if score >= threshold:
                    doc_keepers.append(feature)
                    lookup_index = just_entities[k].index(feature)
                    doc_types.append(just_types[k][lookup_index])
                    doc_scores.append(just_scores[k][lookup_index])
                if (score < threshold) and (score > 0):
                    doc_stoppers.append(feature)
                
            keepers[k] = doc_keepers
            stoppers[k] = doc_stoppers
            types[k] = doc_types
            scores[k] = doc_scores
        
        final_filtered = {k:[keepers[k], types[k], scores[k]] for k,v in self.ner_results.items()}
        self.dict_to_csv(stoppers,
                         ['incidentID', 'entities_filtered_out'],
                         f'entities_filtered_out_threshold_{threshold}')
        self.dict_to_csv(final_filtered,
                         ['incidentID', 'entities_filtered_in', 'types', 'scores'],
                         f'entities_filtered_in_threshold_{threshold}')
                
        self.ner_results = final_filtered
        
 
    def run_pipeline(self, target_date:str, period:str='trailing_seven_days'):
        """
        Run APOLLO detector with one call.
        
        :param target_date: date string in format 'YYYY-MM-DD'. used to specify
        which month or week to pull data from
        :type target_date: str
        :param period: 'trailing_seven_days' (default), 'monthly' or 'week', 
        period of WEDSS data to examine for outbreaks
        :type period: str
        :returns: None.
        """
        self.get_ids(target_date, period=period)
        self.get_text_from_wedds_files()
        self.process_text_for_entities()
        self.create_stop_entities()
        self.create_report()     
        
    def generate_stop_entities(self, period):
        self.stop_entities=None
        self.get_ids('1900-01-01', period='all')
        self.get_text_from_wedds_files()
        self.process_text_for_entities()
        self.create_stop_entities(min_df=0)
        
        
def main():
    """run APOLLO"""
    detector = ApolloDetector('/project/WEDSS/NLP/', 
                          output_path = 'output_dir', 
                          nlp_fields_file = 'supporting_data/nlp_fields.csv',
                          stop_entities_file='supporting_data/stop_entities_example.csv')
    detector.run_pipeline(target_date='2020-10-01', period='week')
    
if __name__=="__main__":
    main()