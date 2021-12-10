# -*- coding: utf-8 -*-
"""
Created on Thu Jul 29 21:39:12 2021

@author: imcconnell2
"""
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from abc import ABC, abstractmethod
import errno
import os
from typing import List, Dict, Generator, Any
from random import randrange
import json
import click
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.ERROR)

class DateMismatchError(Exception):
    pass


class FileHandler(ABC):
    """interface for file loaders"""
    
    @abstractmethod
    def load_data() -> Dict[datetime, Dict]:
        """define {date: dict{}} of relevant data per file"""
        pass

    
class BaseFileHandler(FileHandler):
    """template for all file loaders"""
    def __init__(self, filepath):
        self.filepath = Path(filepath)
        if not self.filepath.exists(): 
            raise FileNotFoundError(errno.ENOENT, 
                                    os.strerror(errno.ENOENT), 
                                    filepath)
        if not self.filepath.is_dir():
            raise NotADirectoryError(f'{self.filepath} is not a directory')
        
        self.data = {}
        self.date_range = []
    
    def __get_file_list__(self) -> List:
        """define all files to load in dir filepath e.g.
        return [x for x in self.filepath.glob('*.csv')]"""
        pass
    
    def __get_data__(self) -> Dict[date, Any]:
        """define date:dict of data per file type"""
        pass
    
    def __get_date_range__(self) -> List:
        """return list of dates associated with each file"""
        self.date_range = sorted([k for k in list(self.data.keys())])
        
    def load_data(self) -> Dict[date, Any]:
        self.__get_data__()
        self.__get_date_range__()
        return self.data


class OutbreakStatsFileHandler(BaseFileHandler):
    def __get_file_list__(self) -> Dict:
        """return list of all files in dir filepath"""
        return [x for x in self.filepath.glob('*outbreak_stats*.json')]
    
    def __get_data__(self) -> Dict[date, Dict]:
        """get dict of {datetime:Dict}"""
        datapoints_to_load = [
            'report_date',
            "unique_known_outbreaks_for_all_incident_ids",
            "unique outbreaks matched to NERS from data_dict",
            "unique known outbreaks NOT matched to NERs"
            ]
        for f in self.__get_file_list__():
            with f.open('r') as j:
                raw_data = json.load(j)
                data_to_keep = {}
                for dp in datapoints_to_load:
                    data_to_keep[dp] = raw_data[dp]
                report_date = datetime.strptime(data_to_keep['report_date'],
                                                '%Y-%m-%d')
                self.data[report_date] = data_to_keep


class FinalReportFileHandler(BaseFileHandler):
    """load Final Report files - return {date: pd.DataFrame}"""
    def __get_file_list__(self) -> List:
        return [x for x in self.filepath.glob('*APOLLO-Report*.txt')]
   
    def __get_data__(self) -> Dict[date, pd.DataFrame]:
        """get dict of report_date:report_dataframe"""
           
        dfs = [pd.read_csv(x, delimiter='|')
               for x in self.__get_file_list__()]
        
        logging.info(f'{len(dfs)=}')
        
        date_col = None
        for k in ['DateStamp', 'Datestamp', 'TimeEnd']:
            if k in dfs[0].columns.to_list():
                date_col = k
                break
            
        if not date_col:
            raise ValueError('Final Reports Date Stamp not defined')
        
        cols = [date_col,'Name','Iterations','Incidents','Outbreaks','OutbreakIDs',
                'Address1']
        
        self.data = {date.fromisoformat(
                        x[date_col][0]).replace(day=1):  # handle month end vs start kludge 
                     x[cols] 
                     for x in dfs}


class OutbreakChunkFileHandler(BaseFileHandler):
    """load parsed and chunked outbreak files
    :return: {date: List[str]}"""  
    def __get_file_list__(self) -> List:
        return [x for x in self.filepath.glob('*outbreak-chunk-*.txt')]
        
    def __get_data__(self) -> Dict[date, List[str]]:
        """ get dict of outbreak_chunk_month: [outbreak#]"""

        for file in self.__get_file_list__():
            file_date = date.fromisoformat(file.stem[:7] + '-01')
            logging.debug(f'file_date: {file_date}')
            with open(file, 'r') as f:
                df = pd.read_csv(f, index_col=False, sep='|')
                self.data[file_date] = list(set(df['Outbreak#'].to_list())) 


class FinalReportProcessor():
    """
    load one or more final APOLLO reports and process for outbreak detection \
    stats for each final report (time period)
    i.e. get recall and precision for each month of a year
    
    First you must:
    run ApolloDetector Pipeline (with all output files enabled) for each month
    or other time period for a range like a year. Those results must include
    John's geolocation pipeline (used to define TP, FN etc)
    
    Run outbreak_chunker to split up outbreak file into period-length chunks
    
    get two files per point/period in date range:
        1: Final Report files with John's geolocaiton pipeline data
        2: outbreak chunk
    
    From |-delimited chunked outbreak files:
        list all known outbreaks in that period for FN calc.

    From final report |-delimited .txt file:
        everythign else

    new outbreaks: outbreaks detected by APOLLO, not in known outbreaks, BUT have a detected address
    true postives: outrbeaks detected by APOLLO linked to a known outbreak
    false positives: outbreaks detected by APOLLO, not in known outbreaks, and no address detected
    false negatives: known outbreaks in NLP data for a report period not detected by APOLLO
    true negatives: we can't know these!
    recall: TP/TP+FN
    precision: TP/TP+FP
    f1_score: (2*TP)/(2*TP)+FP+FN
    
    """
    def __init__(self, FinalReports:FileHandler, OutbreakChunks: FileHandler):        
        self.final_report_data = FinalReports.load_data()
        self.outbreak_chunks = OutbreakChunks.load_data()
        if OutbreakChunks.date_range != FinalReports.date_range:
                        raise DateMismatchError(
                f"outbreak chunk dates don't match final report dates\n"\
                f"outbreak chunks:{OutbreakChunks.date_range}\n"\
                f"final reports:{FinalReports.date_range}"
                )
            
        self.date_range = FinalReports.date_range
        self.statistics = {}
        
    @staticmethod
    def randomly_sample_df(df: pd.DataFrame) -> Generator:
        """return a generator for random samples of a df. Assumes df index is
        incrementing integer
        :param df: dataframe to sample
        :return: infinite generator"""

        while True:
            indicies = [randrange(len(df)) for i in range(len(df))]
            yield df.iloc[indicies]
    
    @staticmethod
    def calculate_df_stats(final_report_df:pd.DataFrame, 
                           known_outbreaks: List[str]) -> Dict:
        """
        :param final_report_df: final report format df
        :param known_outbreaks: List of known outbreaks for final report time 
        period
        :return: Dict with stats: {'tp', 'fp', 'fn', 'precision', 'recall', 
        'f1', 'novel_outbreaks'}
        """
        
        # unique incidents in report - only for NERs with count >=2
        final_report_incidents_lol = [x.replace('[','').replace(']','').replace("', '",' ').split() 
                                      for x in
                                       final_report_df[
                                           final_report_df['Iterations'] >=2
                                           ]['Incidents'].to_list()]
        
        unique_incidents = list(set([x 
                                 for sublist in final_report_incidents_lol 
                                 for x in sublist]))


        # entites NOT matched to an outbreaks WITH an address match, and iteration count >=2
        novel_outbreak_entities = \
            final_report_df[(final_report_df['Outbreaks'] == '[]') &  # not matched with a known outbreak
                            (final_report_df['Iterations'] >= 2) &  # detected in at least two incidents
                            (final_report_df['Address1'] != 'FILTERED')  # address associated with entity 
                            ]['Name'].to_list() 
        novel_outbreaks = len(novel_outbreak_entities)
        logging.debug(f'{novel_outbreaks=}')
        # all entities matched to outbreaks with iteration count >=2 or more
        tp_entities = \
            final_report_df[(final_report_df['Outbreaks'] != '[]') &  # matched at least one outbreak
                            (final_report_df['Iterations'] >= 2)  # found in at least two incidents
                            ]['Name'].to_list()
        
        tp = len(tp_entities)
        logging.debug(f'{tp=}')
        # predicted entity - not matched to outbreak and not a novel outbreak
        unmatched_APOLLO_entities = \
            final_report_df[(final_report_df['Outbreaks'] == '[]') &  # not matched to known outbreak
                            (final_report_df['Iterations'] >= 2)  # detected in at least two incidents
                            ]['Name'].to_list()
        
        # all unmatched predicted outbreaks (entities) that aren't novel outbreaks
        # define this as no address here as well, specifically.
        fp_entities = [x for x in unmatched_APOLLO_entities if x not in novel_outbreak_entities]
        fp = len(fp_entities)
        logging.debug(f'{fp=}')

        final_report_outbreak_lol = [x.replace('[','').replace(']','').replace("'",'').split(',') 
                                 for x in
                                 final_report_df[final_report_df['Outbreaks'] != '[]']['Outbreaks'].to_list()]
        
        all_matched_outbreaks = [x 
                                 for sublist in final_report_outbreak_lol 
                                 for x in sublist]
        
        fn_outbreaks = [x for x in known_outbreaks if x not in all_matched_outbreaks]
        
        logging.info(f"final report oubreaks extract: {final_report_df[final_report_df['Outbreaks'] != '[]']['Outbreaks'].to_list()}")
        logging.info(f'{final_report_outbreak_lol=}')
        logging.info(f'{all_matched_outbreaks=}')
        logging.info(f'{known_outbreaks=}')
        logging.info(f'{fn_outbreaks=}')
        fn= len(fn_outbreaks)
        logging.debug(f'{fn=}')

        try:
            recall = tp/(tp+fn)
        except ZeroDivisionError:
            logging.warning('Divide by Zero during recall calculation: tp:{tp} fn:{fn}')
            recall = 1
            
        try:
            precision = tp/(tp+fp)
        except ZeroDivisionError:
            logging.warning('Divide by Zero during recall calculation: tp:{tp} fp:{fp}')
            precision = 1
        
        try:
            f1_score = (2*tp)/((2*tp)+fp+fn)
        except ZeroDivisionError:
            logging.warning('Divide by Zero during recall calculation: tp:{tp} fp:{fp}')
            f1_score = 1
            
        
        sample_stats = {}
        sample_stats['total_unique_incidents'] = len(unique_incidents)
        sample_stats['known_outbreaks'] = len(known_outbreaks)
        sample_stats['total_entities'] = len(final_report_df)
        sample_stats['novel_outbreaks'] = novel_outbreaks
        sample_stats['TP'] = tp
        sample_stats['FP'] = fp
        sample_stats['FN'] = fn
        sample_stats['recall'] = recall
        sample_stats['precision'] = precision
        sample_stats['f1_score'] = f1_score
        return sample_stats

    @staticmethod
    def calculate_CI_from_sampled_stats(sample_statistics: List[Dict], key:str) -> Dict:
        """calc CIs from a List of sampled results Dicts
        :param sample_statistics: list of dicts containing stats from resampled data
        :param key: key in resampled dicts to calc CI for 
        :return: Dict with CI stats for given key: {'<key>_CI':CI_val}"""
        # calculate CIs from sample_stats
        # get all recalls
        sampled_stats = [v[key] for v in sample_statistics]
        # get mean recall
        sampled_stat_mean = sum(sampled_stats)/len(sampled_stats)
        # get differences from mean recall
        stat_deltas = [sampled_stat_mean - r for r in sampled_stats]            
        # rank in order of difference magnitudes
        stat_deltas = sorted(stat_deltas)
        # get 25th and 975th values - lower and upper bounds of 95%
        calculated_CI = {}      
        calculated_CI[key+'_CI_lower'] = sampled_stat_mean - stat_deltas[975]
        calculated_CI[key+'_CI_upper'] = sampled_stat_mean - stat_deltas[25]
        calculated_CI[key+'_CI_upper_delta'] = stat_deltas[25]
        calculated_CI[key+'_CI_lower_delta'] = stat_deltas[975]
        return calculated_CI
    
    def calculate_all_stats(self):
        """calculate tp, fp, fn, recall, precision, f1_score, CIs, and new outbreak count
        for each time period"""
        
        for k in tqdm(self.final_report_data):
            # get time period data set to process
            final_report = self.final_report_data[k]
            outbreaks = self.outbreak_chunks[k]
            logging.debug(f'date: {k}')
            logging.debug(f'final_report: {final_report.head()}')
            logging.debug(f'outbreaks: {outbreaks[:5]}')
            
            # calculate stats
            report_statistics = self.calculate_df_stats(final_report, outbreaks)
            
            # calculate CIs
            number_of_sampling_iterations = 1000
            final_report_sample = self.randomly_sample_df(final_report)
            sample_statistics = []
            for i in range(number_of_sampling_iterations):
                df = next(final_report_sample) 
                sample_statistics.append(self.calculate_df_stats(df,
                                                                  outbreaks)
                                          )
            report_statistics.update(self.calculate_CI_from_sampled_stats(sample_statistics, 'recall'))
            report_statistics.update(self.calculate_CI_from_sampled_stats(sample_statistics, 'precision'))
            logging.info(f'report_statistics:{report_statistics}')
            
            # append reults to 
            self.statistics[k] = report_statistics

    def calculate_all_stats_for_all_time(self):
        """calculate tp, fp, fn, recall, precision, f1_score, CIs, and new outbreak count
        for all data at once"""
        # one list of all outbreaks
        all_outbreaks = [v for k, v in self.outbreak_chunks.items()]
        outbreaks = [outbreak for lobs in all_outbreaks for outbreak in lobs]
        del(all_outbreaks)
        # one df of all final reports
        final_report = pd.concat([v for k, v in self.final_report_data.items()])
        logging.debug('processing all data')
        logging.debug(f'final_report data len: {len(final_report)}')
        logging.debug(f'outbreaks data len: {len(outbreaks)}')
        
        # calculate stats
        report_statistics = self.calculate_df_stats(final_report, outbreaks)
        
        # calculate CIs
        number_of_sampling_iterations = 1000
        final_report_sample = self.randomly_sample_df(final_report)
        sample_statistics = []
        for i in range(number_of_sampling_iterations):
            df = next(final_report_sample) 
            sample_statistics.append(self.calculate_df_stats(df,
                                                              outbreaks)
                                      )
        report_statistics.update(self.calculate_CI_from_sampled_stats(sample_statistics, 'recall'))
        report_statistics.update(self.calculate_CI_from_sampled_stats(sample_statistics, 'precision'))
        logging.info(f'report_statistics:{report_statistics}')
        
        # append reults to 
        self.statistics['all'] = report_statistics
        
    def format_output(self):
        """convert final report into stats output""" 
        self.processed_df = pd.DataFrame.from_dict(
            self.statistics,
            orient='index').sort_index()
            
    def write_output(self, output_path):
        """write processed df to disk"""
        date_string = '_'+datetime.strftime(datetime.now(),'%Y-%m-%d_%H_%M')
        output_path = Path(output_path) / ("outbreak_retrieval_stats" + date_string + ".csv")
        self.processed_df.to_csv(output_path, index_label='Month')
        print(f"retrieval statistics summary output to:\n{output_path.absolute()}")

                 
@click.command()
@click.option('--outbreak-chunks-path', type=click.Path(exists=True), help = "dir where outbreak stats .json files from ApolloDetector are found")
@click.option('--final-reports-path', type=click.Path(exists=True), help = "dir where final reports with location mapping completed are found (APOLLO*.txt)")
@click.option('--output-path', type=click.Path(exists=True), help = "dir for output")
@click.option('--all-time/--no-all-time', default=False, help = "run all data together")
def main(outbreak_chunks_path, final_reports_path, output_path, all_time):
    print(f"outbreak_chunks_path: {outbreak_chunks_path}")
    print(f"final_reports_path: {final_reports_path}")
    print(f"output_path: {output_path}")
    outbreak_chunks = OutbreakChunkFileHandler(outbreak_chunks_path)
    final_reports = FinalReportFileHandler(final_reports_path)
    processor = FinalReportProcessor(FinalReports = final_reports, 
                                     OutbreakChunks = outbreak_chunks)
    if not all_time:
        processor.calculate_all_stats()
    else:
        processor.calculate_all_stats_for_all_time()
    
    processor.format_output()
    processor.write_output(output_path)

if __name__ == "__main__":
    main()
