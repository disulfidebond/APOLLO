#!/usr/bin/python env
from APOLLO.ApolloDetector import ApolloDetector
from datetime import date
from dateutil.relativedelta import relativedelta

# run on linsilogpu001.ssc.wisc.edu

def run_APOLLO(report_date, period, input_path, output_path, nlp_fields, stop_entities, prefix, final_report_only):
    """
    Run the APOLLO pipeline
    """
    
    detector = ApolloDetector(input_path, 
                              output_path = output_path, 
                              nlp_fields_file = nlp_fields,
                              stop_entities_file = stop_entities,
                              file_prefix=prefix,
                              final_report_only=final_report_only)
    
    detector.run_pipeline(report_date, period)

if __name__ == '__main__':
    #
    # Set up to run APOLLO for each month separately for YTD.
    #
    period='month'
    output_path='output_dir/202105_YTD'
    input_path='/project/WEDSS/NLP/'
    nlp_fields='supporting_data/nlp_fields.csv'
    stop_entities='supporting_data/all_stop_entities_list_min_df_0_pc_2021-04-22_00_31_plus_UHS_MAXIM_names.csv'
    final_report_only = False
    
    start_date = date.fromisoformat('2021-05-01')
    date_range = []
    for i in range(12):
        date_range.append((start_date-relativedelta(months=i)).strftime("%Y-%m-%d"))
        
    for report_date, prefix in {k: k[:7] for k in date_range}.items(): 
        print('')
        print(f'NOW RUNNING {report_date}')
        print('')
        run_APOLLO(report_date=report_date, 
                   period=period, 
                   input_path=input_path, 
                   output_path=output_path, 
                   nlp_fields=nlp_fields, 
                   stop_entities=stop_entities, 
                   prefix=prefix,
                   final_report_only=final_report_only)
   