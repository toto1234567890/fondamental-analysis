#!/usr/bin/env python
# coding:utf-8

from typing import Optional
from os import remove as osRemove, makedirs as osMakedirs, rename as osRename
from os.path import dirname as osPathDirname, basename as osPathBasename, join as osPathJoin, \
                    exists as osPathExists, splitext as osPathSplitext
import pandas as pd

from interfaces.data_saver import IDataSaver




class CSVDataSaver(IDataSaver):
    """CSV data saver implementation for writing and reading output data"""
    
    Name = "CSVDataSaver"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        self._setup_output_dir()
    
    #-----------------------------------------------------------------------------------------------
    def _setup_output_dir(self) -> None:
        """Setup CSV output directory"""
        try:
            from src import PROJECT_NAME
            self._output_dir = osPathJoin(getattr(self.config, 'FS_DATA', './data'), PROJECT_NAME, self.Name) 
            
            # Create main directory
            if not osPathExists(self._output_dir):
                osMakedirs(self._output_dir, exist_ok=True)

            self.logger.debug("{0} : output directory configured: {1}".format(self.Name, self._output_dir))
        except Exception as e:
            self.logger.error("{0} : failed to setup output directory - {1}".format(self.Name, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def save_data(self, data: pd.DataFrame, destination: str) -> bool:
        """Save data to CSV output file"""
        temp_path = None
        try:
            file_path = self._get_file_path(destination)
            temp_path = self._get_temp_path(file_path)
            
            # Ensure output directory exists
            file_dir = osPathDirname(file_path)
            if not osPathExists(file_dir):
                osMakedirs(file_dir, exist_ok=True)
            
            # Save to temp file first
            data.to_csv(temp_path, index=False, encoding='utf-8')
            
            # Atomic replace
            if osPathExists(file_path):
                osRemove(file_path)
            osRename(temp_path, file_path)
            
            self.logger.info("{0} : data saved to {1}".format(self.Name, file_path))
            return True
            
        except Exception as e:
            self.logger.error("{0} : failed to save data to {1} - {2}".format(self.Name, destination, str(e)))
            if temp_path and osPathExists(temp_path):
                try:
                    osRemove(temp_path)
                except:
                    pass
            return False
    
    #-----------------------------------------------------------------------------------------------
    def save_with_backup(self, data: pd.DataFrame, destination: str) -> bool:
        """Save data with automatic backup"""
        try:
            from data_backups.csv_backup import CSVBackupService
            backup_service = CSVBackupService(config=self.config, logger=self.logger)
            
            file_path = self._get_file_path(destination)
            if osPathExists(file_path):
                success, _ = backup_service.backup_data(destination)
                if not success:
                    self.logger.warning("{0} : backup failed for {1}".format(self.Name, destination))
            
            return self.save_data(data, destination)
            
        except Exception as e:
            self.logger.error("{0} : save with backup failed for {1} - {2}".format(self.Name, destination, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def read_data(self, source: str) -> Optional[pd.DataFrame]:
        """Read data from CSV file"""
        try:
            file_path = self._get_file_path(source)
            
            if not osPathExists(file_path):
                self.logger.warning("{0} : file not found - {1}".format(self.Name, file_path))
                return None
            
            df = pd.read_csv(file_path)
            self.logger.debug("{0} : read {1} records from {2}".format(
                self.Name, len(df), file_path))
            return df
            
        except Exception as e:
            self.logger.error("{0} : failed to read data from {1} - {2}".format(self.Name, source, str(e)))
            return None
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if data saver is healthy"""
        try:
            test_file = osPathJoin(self._output_dir, '.health_check')
            with open(test_file, 'w') as f:
                f.write('test')
            osRemove(test_file)
            return True
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def _get_file_path(self, destination: str) -> str:
        """Get full file path for destination"""
        if not destination.lower().endswith('.csv'):
            destination += '.csv'
        return osPathJoin(self._output_dir, destination)
    
    #-----------------------------------------------------------------------------------------------
    def _get_temp_path(self, file_path: str) -> str:
        """Get temporary file path"""
        file_dir = osPathDirname(file_path)
        file_name = osPathBasename(file_path)
        name_without_ext = osPathSplitext(file_name)[0]
        return osPathJoin(file_dir, f".{name_without_ext}.tmp")