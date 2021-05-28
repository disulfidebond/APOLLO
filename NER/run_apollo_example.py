# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 09:03:27 2021
    
@author: Iain McConnell

Run APOLLO example script:
0. cd to APOLLO/ then 'pip install -e .'
1. confirm the WEDSS folder configuration and contents
   /project/WEDSS/NLP should contain only one set of WEDSS extract data.
2. cd to the directory containing this file then 'python run_apollo_example.py'
"""
from APOLLO.ApolloDetector import ApolloDetector

detector = ApolloDetector('/project/WEDSS/NLP/', 
                          output_path = 'output_dir', 
                          nlp_fields = 'supporting_data/nlp_fields.csv',
                          stop_entities_file='supporting_data/stop_entities_example.csv')
detector.run_pipeline('2020-10-01')
