# -*- coding: utf-8 -*-
"""
Created on Fri Nov 12 13:44:31 2021

@author: imcconnell2
"""
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
import pandas as pd
from pathlib import Path
from typing import List
import logging
from tqdm import tqdm

logging.basicConfig(format='%(levelname)s :: %(filename)s :: %(funcName)s :: %(asctime)s :: %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class OutbreakChunker:
    """open patient and outbreak files. get all positive patient ids per month
    use those ids to break out outbreaks with postive ids per month
    A lot of this code duplicated from APOLLODetector"""
    def __init__(self, filepath, output_path):
        self.filepath = Path(filepath)
        if not self.filepath.is_dir():
            raise NotADirectoryError()
           
        self.patient_file = next(self.filepath.glob('NLP_Patient*.txt'))
        self.outbreak_file = next(self.filepath.glob('NLP_Outbreak*.txt'))
    
        self.output_path = Path(output_path) / "outbreak_chunks"
        self.output_path.mkdir(parents=True, exist_ok=True)
        
    
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


    def get_ids(self, target_date:str='', period:str='week', 
                search_county:str='Dane'):
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

        with self.patient_file.open('r', encoding='ISO-8859-1') as f: 
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
            for count, line in tqdm(enumerate(lines)):
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
          
            print(f'total any status cases, in Dane county, in week of specified date:{any_counter}')            
            print(f'total confirmed cases, in Dane county, in week of specified date:{confirmed_counter}')
            self.ids=ids
    
    def create_df(self):
        df = pd.read_csv(self.outbreak_file, index_col='IncidentID', sep='|')
        # drop 14 K rows with invalid incidentIDs
        df = df[df.index.notnull()]
        # stop pandas adding .0s everywhere
        df.index = df.index.astype('int32')
        df['OutbreakID '] = df['OutbreakID '].astype('int32')
        # get ids that match outbreaks
        current_ids = [k for k in self.ids]
        matching_ids = [x for x in df.index.to_list() if str(x) in current_ids]
        # slice df to only ids with outbreaks
        self.df = df.loc[matching_ids]
        
    def write_df(self, prefix):
        chunk_name = f'{prefix}-outbreak-chunk-{date.today()}.txt'
        print('exporting chunk')
        self.df.to_csv(self.output_path / chunk_name, sep='|')          

    def blow_chunks(self, start_from, number_of_months_to_run):
         """
         chunk out OutbreakFile by month
         """
         # define date_range dict to iterate over
         start_date = date.fromisoformat(start_from)
         date_range = []
         for i in range(number_of_months_to_run):
             date_range.append((start_date-relativedelta(months=i)).strftime("%Y-%m-%d"))
        
         # set up dicts to capture per run data and run all date periods
        
         for report_date, prefix in {k: k[:7] for k in date_range}.items():
             print(f'chunking {report_date}')
             print('')
             self.get_ids(target_date=report_date, 
                          period='month'
                          )
             self.create_df()
             self.write_df(prefix)
        
@click.command()
@click.option('--NLP_files_path', type=click.Path(exists=True))
@click.option('--output_path', type=click.Path(exists=True))
@click.option('--start_from', type=str, default='2021-06-01')  
@click.option('--number_of_months_to_run', type=int, default='12')    
def main(NLP_files_path, output_path, start_from, number_of_months_to_run):         
    c = OutbreakChunker(NLP_files_path, output_path)
    c.blow_chunks(start_from, number_of_months_to_run)
    
if __name__ == '__main__':
    main()