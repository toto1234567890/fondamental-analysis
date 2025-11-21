#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional
from os import listdir as osListdir
from os.path import join as osPathJoin, exists as osPathExists, isfile as osPathIsfile, splitext as osPathSplitext
import pandas as pd

from interfaces.data_source import IDataSource




class CSVDataSource(IDataSource):
    """CSV data source implementation for reading input data"""
    
    Name = "CSVDataSource"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        self._input_dir = osPathJoin(getattr(config, 'FS_DATA', './data'), 'input')
    
    #-----------------------------------------------------------------------------------------------
    def get_data(self, source: str) -> pd.DataFrame:
        """Get data from CSV input file"""
        try:
            file_path = self._get_file_path(source)
            if not osPathExists(file_path):
                self.logger.warning("{0} : input file not found {1}".format(self.Name, file_path))
                return pd.DataFrame()
            
            df = pd.read_csv(file_path)
            self.logger.debug("{0} : retrieved data from {1}".format(self.Name, file_path))
            return df
            
        except Exception as e:
            self.logger.error("{0} : failed to get data from {1} - {2}".format(self.Name, source, str(e)))
            return pd.DataFrame()
    
    #-----------------------------------------------------------------------------------------------
    def list_sources(self) -> List[str]:
        """List available CSV input files"""
        try:
            if not osPathExists(self._input_dir):
                return []
                
            files = osListdir(self._input_dir)
            csv_files = []
            for file in files:
                file_path = osPathJoin(self._input_dir, file)
                if osPathIsfile(file_path) and file.lower().endswith('.csv'):
                    name_without_ext = osPathSplitext(file)[0]
                    csv_files.append(name_without_ext)
            return csv_files
            
        except Exception as e:
            self.logger.error("{0} : failed to list sources - {1}".format(self.Name, str(e)))
            return []
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if CSV source is healthy"""
        try:
            return osPathExists(self._input_dir)
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def _get_file_path(self, source: str) -> str:
        """Get full file path for source"""
        if not source.lower().endswith('.csv'):
            source += '.csv'
        return osPathJoin(self._input_dir, source)