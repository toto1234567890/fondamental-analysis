#!/usr/bin/env python
# coding:utf-8

from typing import Optional
import pandas as pd
from datetime import datetime, timezone

from interfaces.data_saver import IDataSaver



class ArcticDataSaver(IDataSaver):
    """ArcticDB data saver implementation"""
    
    Name = "ArcticDataSaver"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        self._library = None
        self._setup_arctic()
    
    #-----------------------------------------------------------------------------------------------
    def _setup_arctic(self, library_name) -> None:
        """Setup ArcticDB connection"""
        try:
            from src import PROJECT_NAME
            library_name = "{0}_{1}".format(PROJECT_NAME, self.Name)
            from arctic import Arctic
            store = Arctic(self.config.ARCTIC_HOST)
            if library_name not in store.list_libraries():
                store.initialize_library(library_name)
            self._library = store[library_name]
            self.logger.debug("{0} : ArcticDB saver connection established".format(self.Name))
        except ImportError:
            self.logger.critical("{0} : ArcticDB package required but not installed".format(self.Name))
            exit(1)
        except Exception as e:
            self.logger.error("{0} : failed to setup ArcticDB saver - {1}".format(self.Name, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def save_data(self, data: pd.DataFrame, destination: str) -> bool:
        """Save data to ArcticDB"""
        try:
            if self._library is None:
                return False
            
            # Add metadata
            data = data.copy()
            data['_saved_timestamp'] = datetime.now(timezone.utc)
            
            # Save to ArcticDB
            self._library.write(destination, data)
            self.logger.info("{0} : data successfully saved to {1}".format(self.Name, destination))
            return True
            
        except Exception as e:
            self.logger.error("{0} : failed to save data to {1} - {2}".format(self.Name, destination, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def save_with_backup(self, data: pd.DataFrame, destination: str) -> bool:
        """Save data with automatic backup"""
        try:
            # Only backup if symbol exists
            if self._library is not None and destination in self._library.list_symbols():
                from data_backups.arctic_backup import ArcticBackupService
                data_backup = ArcticBackupService(config=self.config, logger=self.logger)
                success, error = data_backup.backup_data(destination)
                
                if not success:
                    self.logger.warning("{0} : backup failed for {1}, but continuing with save".format(self.Name, destination))
            
            # Then save data
            return self.save_data(data, destination)
            
        except Exception as e:
            self.logger.error("{0} : save with backup failed for {1} - {2}".format(self.Name, destination, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def read_data(self, source: str) -> Optional[pd.DataFrame]:
        """Read data from ArcticDB symbol"""
        try:
            if self._library is None:
                return None
            
            if source not in self._library.list_symbols():
                self.logger.warning("{0} : symbol not found - {1}".format(self.Name, source))
                return None
            
            data = self._library.read(source)
            df = data.data if hasattr(data, 'data') else data
            
            self.logger.debug("{0} : read {1} records from {2}".format(
                self.Name, len(df), source))
            return df
            
        except Exception as e:
            self.logger.error("{0} : failed to read data from {1} - {2}".format(self.Name, source, str(e)))
            return None
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if data saver is healthy"""
        try:
            return self._library is not None and len(self._library.list_symbols()) >= 0
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False

