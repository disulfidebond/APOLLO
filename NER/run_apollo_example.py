# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 09:03:27 2021
    
@author: Iain McConnell

Run APOLLO example script:
confirm the WEDSS folder configuration and contents
/project/WEDSS/NLP should contain only one set of WEDSS extract data.
cd to the directory containing this file then 'python run_apollo_example.py'
"""
from APOLLO.ApolloDetector import ApolloDetector

detector = ApolloDetector('/project/WEDSS/NLP/', 
                          output_path = 'output_dir', 
                          nlp_fields = 'supporting_data/nlp_fields.csv',
                          stop_entities_file='supporting_data/stop_entities_example.csv')
detector.run_pipeline(target_date='2020-10-01', period='week')
