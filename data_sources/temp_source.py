#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional
from os import listdir as osListdir, makedirs as osMakedirs
from os.path import join as osPathJoin, exists as osPathExists, isfile as osPathIsfile
from pickle import load as pickleLoad
import pandas as pd


from interfaces.data_source import IDataSource





class TempFileDataSource(IDataSource):
    """Temp file data source for scrapers - reads from config.FS_TEMP/PROJECT_NAME/TempFileDataSource"""
    
    Name = "TempFileDataSource"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        self._temp_dir = None
        self._setup_temp_dir()
    
    #-----------------------------------------------------------------------------------------------
    def _setup_temp_dir(self) -> None:
        """Setup temp directory with project structure"""
        try:
            from src import PROJECT_NAME
            self._temp_dir = osPathJoin(getattr(self.config, 'FS_TEMP', './temp'), PROJECT_NAME, self.Name) 
            
            # Create main directory
            if not osPathExists(self._temp_dir):
                osMakedirs(self._temp_dir, exist_ok=True)

            self.logger.debug("{0} : temp directory configured: {1}".format(self.Name, self._temp_dir))
        except Exception as e:
            self.logger.error("{0} : failed to setup temp directory - {1}".format(self.Name, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def get_data(self, source: str) -> pd.DataFrame:
        """Get data from temp file - supports multiple formats including pickle"""
        try:
            file_path = self._get_file_path(source)
            if not osPathExists(file_path):
                raise FileNotFoundError(f"Temp file not found: {file_path}")
            
            # Support different file formats including pickle
            if source.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif source.endswith('.parquet'):
                df = pd.read_parquet(file_path)
            elif source.endswith('.json'):
                df = pd.read_json(file_path)
            elif source.endswith('.pkl'):
                # For pickled objects - could be DataFrame or any Python object
                with open(file_path, 'rb') as f:
                    loaded_data = pickleLoad(f)
                # If it's already a DataFrame, return as is
                if isinstance(loaded_data, pd.DataFrame):
                    df = loaded_data
                else:
                    # For other objects, create a DataFrame summary
                    df = self._object_to_dataframe(loaded_data, source)
            else:
                raise Exception("no file found... (expected: *.csv, *.parquet, *.json, *.pkl)") 
            
            self.logger.debug("{0} : read data from {1}".format(self.Name, file_path))
            return df
            
        except Exception as e:
            self.logger.error("{0} : failed to read data from {1} - {2}".format(
                self.Name, source, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def list_sources(self) -> List[str]:
        """List available temp files including pickle files"""
        try:
            if not osPathExists(self._temp_dir):
                return []
            
            files = []
            for file in osListdir(self._temp_dir):
                file_path = osPathJoin(self._temp_dir, file)
                if (osPathIsfile(file_path) and 
                    file.endswith(('.csv', '.parquet', '.json', '.pkl')) and
                    not file.startswith('.')):
                    files.append(file)
            
            return files
            
        except Exception as e:
            self.logger.error("{0} : failed to list temp files - {1}".format(self.Name, str(e)))
            return []
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if temp data source is healthy"""
        try:
            return osPathExists(self._temp_dir)
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    # Internal helper methods
    def _get_file_path(self, source: str) -> str:
        """Get full file path for temp file"""
        return osPathJoin(self._temp_dir, source)

    #-----------------------------------------------------------------------------------------------   
    def _object_to_dataframe(self, obj: any, source_name: str) -> pd.DataFrame:
        """Convert any Python object to a DataFrame for consistent interface"""
        try:
            if hasattr(obj, '__dict__'):
                # For objects with attributes, create a summary
                obj_dict = obj.__dict__
                df = pd.DataFrame([obj_dict])
            elif isinstance(obj, (list, tuple)):
                # For sequences
                df = pd.DataFrame(obj)
            elif isinstance(obj, dict):
                # For dictionaries
                df = pd.DataFrame([obj])
            else:
                # For other types, create a simple summary
                df = pd.DataFrame({
                    'object_type': [type(obj).__name__],
                    'object_repr': [str(obj)],
                    'source_file': [source_name]
                })
            
            return df
            
        except Exception as e:
            self.logger.warning("{0} : failed to convert object to DataFrame, creating fallback - {1}".format(
                self.Name, str(e)))
            # Fallback DataFrame
            return pd.DataFrame({
                'error': ['Failed to load object'],
                'source_file': [source_name],
                'object_type': [str(type(obj))]
            })