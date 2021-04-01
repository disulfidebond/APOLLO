from tqdm import tqdm
import csv
from datetime import datetime, date, timedelta
import calendar
import statistics

import NERPipeline

from typing import List, Dict
from pathlib import Path
import re

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import logging

logging.basicConfig(format='%(levelname)s :: %(filename)s :: %(funcName)s :: %(asctime)s :: %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# TODO: set up a stop entities run config
# TODO: batched NER processing
# TODO: distributed processing

class ApolloDetector(object):
    """
    Detect frequent NERS in WEDSS data extracts per incident and report likely outbreaks
    Public Methods
    
    
    Attributes
    .file_paths dict of all WEDDS files being processed 
    .output_path directory to write all output files
    .nlp_fields - Dictionary specifying WEDDS columns names to collect
    .stop_entities - list of all NER entities to ignore due to commonality
    .ids - a list of all incidentIDs being examined
    .date_range - a list of dates that will be used in determining if an incidentID in the patients file is valid for analysis.
    .raw_wedss_text - dictionary of incidentID:all WEDDS text from NLP fields separated by .'s.
    .ner_results - dictionary {incidentID:[[all ners], [all ner types], [all ner scores]]}
    
    """
    def __init__(self, data_folder:str, 
                 output_path:str,
                 nlp_fields_file:str,
                 stop_entities_file:str, 
                 stop_entities_run=False):

        # TODO: set a string to prefix all output files e.g. brittany ids in dict_to_csv.
        if not stop_entities_run:
            assert Path(data_folder).is_dir(), f'{self.data_folder.absolute()} is not a valid directory'
            self.file_paths = self.get_file_names(data_folder)
            
            self.output_path = Path(output_path)
            assert self.output_path.is_dir(), f'{self.output_path.absolute()} is not a valid directory'        
            
            nlp_fields_file = Path(nlp_fields_file)
            assert nlp_fields_file.exists(), f'{nlp_fields_file.absolute()} does not exist'
            self.nlp_fields = self.get_nlp_fields(nlp_fields_file)
            
            stop_entities_file = Path(stop_entities_file)
            assert stop_entities_file.exists(), f'{stop_entities_file.absolute()} does not exist'
            self.stop_entities = self.get_stop_entities(stop_entities_file)

        if stop_entities_run:
            raise NotImplementedError
        
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
    def get_stop_entities(entities_file:Path, 
                          entity_col:str='Name',  #'\ufeffName', 
                          freq_col:str='Frequency', 
                          freq_limit:float=1.7) -> Dict:
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
            csvreader=csv.reader(csvfile, delimiter="\t")
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
            
        write_path = self.output_path / (file_name + date_string + ext)
   
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
        :param period: range of values to return, a list of days a week or a month long
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
            if period == 'month':
                number_of_days = calendar.monthrange(d.year,d.month)[1]
                self.date_range = [date(d.year, d.month, day) for day in range(1, number_of_days+1)]
            else:
                print("period type must be 'week' or 'month'")
                raise
        except (IndexError, ValueError):
            logger.error(f"Couldn't set date range with {d}")
           
    def date_in_range(self, date:str) -> bool:
        """
        :param date: plain text date in format "YYYY-MM-DD <stuff>"
        :return: True if date in range, else False. If can't convert date return False.
        """
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

    def get_ids(self, target_date:str='', period:str='week', ids:List=None, scale_factor:int=0):
        """
        open patient file directly and get all incident IDS that are:
            in the week specified  
            in Dane county
            for confirmed cases
        :param target_date: date to use to define the month or week to pull ids from
        :param period: string, either 'week' or 'month', the period to pull dates around the target_date
        :param ids: list of ids to search for directly, skip looking for criteria matches in patient file.
        :param scale_factor: sub sample data - keeping on every nth patient row.
        :return: self.ids - dict of relevant incident ids {incident_id:1}
        """
        # TODO: set county as variable
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
                count = 0
                usable_resolution_statues = ["Confirmed", "Probable"]
                # TODO: reimplement scale factor
                for count, line in enumerate(lines):
                    ls=line.split("|")
                    incidentID = ls[0]
                    episode_date = ls[18]
                    county = ls[8]
                    resolution_status = ls[33]
                    
                    if count!=0\
                        and episode_date!=''\
                        and self.date_in_range(episode_date)\
                        and county=="Dane":
                            
                        any_counter += 1
                        
                        if resolution_status.strip() in usable_resolution_statues:       
                            ids[incidentID]=1
                            confirmed_counter+=1
                            
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
        for file in self.file_paths['all']:
            with file.open('r', encoding='ISO-8859-1') as f:
                f.seek(0)
                lines=f.readlines()
                hdic={}
                
                # iterate over every line in a file
                for counter, line in enumerate(lines):
                    ls=line.split("|")
                    incident_id = ls[0]
                    
                    # determine which columns are relevant in this file, make dict hdic
                    # for the first line in a file add all values to hdic {'header val':1}
                    # IF those header values in the NLPFields.csv file, or contain "_Sec".
                    if counter==0:
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
                                    tmp=tmp+'.'+ls[key]
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

    @staticmethod
    def _chunk_out_text(text:str, chunk_size:int = 512) -> List:
        """
        :param text: any string
        :param chunk_size: any positive int
        split up test a list of 0-to-n chunk_size strings and one string with the remaining <chunk_size chunk
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

    def _ner_over_chunks(self, actual_text:str) -> List:
        """
        Take a string of any length, cut it into 512 char sections, run NER on each section then reassemble the
        results into one list.
        :param actual_text: any string - here all concatenated text associated with an incident_id_column_name
        :return: List of three lists [[entities], [entity types], [scores]]
        """
        chunks = self._chunk_out_text(actual_text)
        aggregated_results = [[],[],[],[]]
        chunk_ners = []
        for c in chunks:
            # NERPipeline returns 3 value list
            # chunk_ners is a list of three value lists
            res = NERPipeline.nerfunc(c)
            chunk_ners.append(res)
        
        for c in chunk_ners:
            for j in range(4):
                aggregated_results[j].extend(c[j]) 
        
        return aggregated_results
    
    def BERTNER(self, output:Dict, stats_output=False) -> Dict:
        """
        run BERT NER pipeline on text for every incident id - this is distributed via dask.distributed
        :param output: dict {incidentID: "all text from NLPFields.csv fields concatenated"}
        :return: dict {incidentID: [[Names], [Types], [Scores]]}
        """
        out={}
        # print('starting parallel NER processing...')
        print('kick off NER processing...')
        for key, actual_text in tqdm(output.items()):
            actual_text = re.sub(r"[.]+", ". ", actual_text)  # replace series of periods with just one and a space.
            out[key] = self._ner_over_chunks(actual_text)
            # progress(out)
            
        # collect all tokens for metric calcs
        document_lengths = []
        doc_tokens = []
        for k,v in out.items():
            tokens = v[3]
            document_lengths.append(len(tokens))
            doc_tokens.append(tokens)
        
        if stats_output:
            token_stats={}
            token_stats['total_doc_count'] = len(document_lengths)
            token_stats['total tokens'] = sum(document_lengths)
            token_stats['total unique tokens'] = len(set(self.flatten(doc_tokens)))
            median = statistics.median(document_lengths)
            LQ = statistics.median([x for x in document_lengths if x <= median])
            UQ = statistics.median([x for x in document_lengths if x >= median])
            token_stats['token count median'] = median
            token_stats['token count lower quartile'] = LQ
            token_stats['token count upper quartile'] = UQ
            token_stats['token count IQR'] = UQ-LQ
            self.dict_to_csv(token_stats,
                        columns=['stat','value'],
                        file_name='token_statistics')

        # strip tokens
        for k,v in out.items():
            out[k] = (v[0], v[1], v[2])

        # return(dask.compute(out)[0]) 
        return(out)
 
    def process_text_for_entities(self):
        """
        process raw text per incident ID to NER results per incidentID
        """
        self.ner_results = self.BERTNER(self.raw_wedss_text, stats_output=True)
        self.dict_to_csv(self.ner_results,
                         columns = ["IncidentID","Names","Types","Scores"],
                         file_name="ner_results")

    def find_associated_known_outbreak_ids(self, namedic:Dict) -> Dict:
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

        incidents_to_outbreaks = {}
        outbreaks_to_ners = {}
        
        self.outbreak_stats = {}
        
        outbreak_match_ratio = 66

        # get all outbreaks in outbreak file
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
                        incidents_to_outbreaks[incident_id] = outbreak_id
                        
                        # map outbreakID to pre-NER string incorporating outbreak location information - typically organization name
                        # id structure typically all caps YEAR-COUNTY BUSINESS NAME | split into: year County. Business Name. Location
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
        for outbreak_id, ner_result in outbreaks_to_ners.items():
            outbreaks_to_ners[outbreak_id] = [x for x in outbreaks_to_ners[outbreak_id][0] if ((x!="") and (x[0]!="#"))]
        
        self.outbreak_stats['associated outbreak count'] = len(outbreaks_to_ners)
        
        # for all entities found by the NER pipeline look at WEDDS outbreak data
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
                            # if key.strip() == entity.strip():
                            if fuzz.token_sort_ratio(entity, key) >= outbreak_match_ratio:
                                outbreak_ids_by_entity[key].append(incidents_to_outbreaks[incident_id])
                                break
                                
                    except (KeyError):
                        # logger.debug('entity %s not found in outbreak id search process', key)
                        pass
                            
        unique_outbreaks = {}
        for entity, outbreak_ids in outbreak_ids_by_entity.items():
            for outbreak in outbreak_ids:
                if outbreak in unique_outbreaks:
                    unique_outbreaks[outbreak] += 1
                else:
                    unique_outbreaks[outbreak] = 1
        self.outbreak_stats['unique outbreaks matched to NERS'] = len(unique_outbreaks)
        
        print(self.outbreak_stats)
        
        self.dict_to_csv(outbreaks_to_ners, ['outbreak_id','NERs'], 'outbreaks_to_ners')
        self.dict_to_csv(unique_outbreaks, ['outbreak_id', 'count'], 'unique_outbreaks')
        self.dict_to_csv(self.outbreak_stats, ['outbreak_stat', 'value'], 'outbreak_stats')
        return outbreak_ids_by_entity


    
    @staticmethod
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

    def create_report(self):
        """
        take NERPipeline output (dict {incidentID: [[Names], [Types], [Scores]]}) and convert to per entity output:
            for every entity: entity name, entity type, number of times found, 
            average score, list of incidentID where it was found
        write results to CSV
        also output a list of unqiue incidient IDs in the file
        also check for any existing outbreakids associated with every entity and incidentID
            if they exist, append list per entity
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
        for key, bertner_output in self.ner_results.items():
            entity=bertner_output[0]
            entity_type=bertner_output[1] 
            score=bertner_output[2]
            for i in range(0,len(bertner_output[0])):
                if entity[i] not in typedic:
                    typedic[entity[i]]=entity_type[i]
                if entity[i] not in scoredic:
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
        for key, bertner_output in self.ner_results.items():
            for item in set(bertner_output[0]):
                if item not in namedic:
                    namedic[item]=[key]
                    countdic[item]=1
                else:
                    countdic[item]+=1
                    l=namedic[item]
                    l.append(key)
                    namedic[item]=l

        outbreak_ids_by_key = self.find_associated_known_outbreak_ids(namedic)  # TODO: uncomment for non-stop entity runs
        
        # collate all results into output format: {entity:[type, count, score, incidentids, outbreakids] }
        all_keys = {}
        for key in namedic:
            if key!="" \
               and key[0]!="#" \
               and typedic[key] != 'Person' \
               and key not in self.stop_entities: # TODO: uncomment for non-stop entity runs
                # collect every incidentID to facilitate search
                incident_id_strs = [str(x).strip() for x in namedic[key]] 
                
                # collect all entities and associated information
                all_keys[key] = [typedic[key],
                                countdic[key],
                                sum(scoredic[key])/len(scoredic[key]),
                                incident_id_strs,
                                outbreak_ids_by_key[key]]  # TODO: put htis back for non-stop entity collection runs
        
        all_keys = self.fuzzy_match_entities(all_keys) # TODO: uncomment for non-stop entity runs
        # all_keys = self.tidy_fuzzy_matched_results(all_keys)
        # sort dictionary to order by max iterations to least.
        all_keys = {k: v for k,v in sorted(all_keys.items(),
                                           key=lambda item: sum(self.flatten(item[1][1])),
                                           reverse=True)}
        self.dict_to_csv(all_keys,
                         columns=["Name","Type","Iterations","Score", "Incidents", "Outbreaks"],
                         file_name="final_report")

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
            outbreaks=None
            entities = k.split(',')
            types = self.flatten(v[0])
            iterations = self.flatten(v[1])
            scores = self.flatten(v[2])
            incidents=list(set(self.flatten(v[3])))
            try:
                outbreaks=list(set(self.flatten(v[4])))
            except(IndexError):
                pass
            # get most common entity index
            max_iteration = max(iterations)
            max_index = iterations.index(max_iteration)
            
            #get each value corresponding with most common entity, sum iterations, avg scores
            main_entity = entities[max_index]
            main_type = types[max_index]
            summed_iterations = sum(iterations)
            average_score = sum(self.flatten(scores))/len(self.flatten(scores))
            incidents = self.flatten(incidents)
            
            processed_output[main_entity] = [main_type, summed_iterations, average_score, incidents, outbreaks]
            
        return processed_output

    def run_pipeline(self, target_date:str, period:str='week'):
        """
        Run APOLLO detector with one call.
        
        :param target_date : date string in format 'YYYY-MM-DD'. used to specify
        which month or week to pull data from
        :type target_date: str
        :param period: 'monthly' or 'week' (the default) period of WEDSS data
        to examine for outbreaks
        :type period: str
        :returns: None.
        """
        self.get_ids(target_date, period=period)
        self.get_text_from_wedds_files()
        self.process_text_for_entities()
        self.create_report()

        
def main():
    """run APOLLO"""
    detector = ApolloDetector('/project/WEDSS/NLP/', 
                          output_path = 'output_dir', 
                          nlp_fields_file = 'supporting_data/nlp_fields.csv',
                          stop_entities_file='supporting_data/stop_entities_example.csv')
    detector.run_pipeline('2020-10-01')
    
if __name__=="__main__":
    main()