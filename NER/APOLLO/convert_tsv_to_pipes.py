# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 17:52:08 2021

@author: imcconnell2
"""

import pandas as pd
from pathlib import Path
import errno
import os
from typing import List
import click
from tqdm import tqdm

import logging

logging.basicConfig(level=logging.ERROR)


class FileHandler:
    """get file list and convert"""
    def __init__(self, filepath):
        self.filepath = Path(filepath).absolute()
        if not self.filepath.exists(): 
            raise FileNotFoundError(errno.ENOENT, 
                                    os.strerror(errno.ENOENT), 
                                    filepath)
        if not self.filepath.is_dir():
            raise NotADirectoryError(f'{self.filepath} is not a directory')
    
    def _get_file_list(self) -> List:
        """define all files to load in dir filepath e.g."""
        
        filelist = [x for x in self.filepath.glob('*.tsv')]
        logging.debug(f'{self.filepath=}')
        logging.debug(f'{filelist=}')
        return filelist
        
    def convert_to_pipe_delimited(self):
        for file in tqdm(self._get_file_list()):
            df = pd.read_csv(file, sep='\t')
            out_file = file.parent / (file.stem + '.txt')
            df.to_csv(out_file, sep='|', index=False)


@click.command()
@click.argument('dirname')
def main(dirname):
    "create pipe delimited .txt from all .tsvs in specified directory"
    fh = FileHandler(dirname)  # '/project/WEDSS/NLP/Sandbox/NLPCode_ILM/APOLLO/final_report_test_dataset'
    fh.convert_to_pipe_delimited()

    
if __name__ == '__main__':
    main()
