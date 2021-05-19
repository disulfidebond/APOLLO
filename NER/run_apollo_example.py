#!/usr/bin/python env
import click
from APOLLO.ApolloDetector import ApolloDetector

@click.command()
@click.option('--date', type=str, help = 'YYYY-MM-DD formatted date to run the pipeline on')
@click.option('--period', type=str, default='trailing_seven_days', help = 'type of date period to run: week, trailing_seven_days, month, all')
@click.option('--output_path', type=click.Path(exists=True), default='output_dir', help = 'dir for output files')
@click.option('--input_path', type=click.Path(exists=True), default='/project/WEDSS/NLP/', help = 'dir for input WEDSS files')
@click.option('--nlp_fields', type=click.Path(exists=True), default='supporting_data/nlp_fields.csv', help = 'file with WEDSS fields to use')
@click.option('--stop_entities', type=click.Path(exists=True), default='supporting_data/stop_entities_example.csv', help = 'file with stop entities to use')
@click.option('--prefix', type=str, default='', help = 'prefix for all output file names')
def APOLLO_pipeline(date, period, input_path, output_path, nlp_fields, stop_entities, prefix):
    """
    Run the APOLLO pipeline
    """
    detector = ApolloDetector(input_path, 
                              output_path = output_path, 
                              nlp_fields_file = nlp_fields,
                              stop_entities_file = stop_entities,
                              file_prefix=prefix)
    
    detector.run_pipeline(date, period)

if __name__ == '__main__':
    APOLLO_pipeline()