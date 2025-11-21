#!/usr/bin/env python
# coding:utf-8

from typing import Optional
from os import makedirs as osMakedirs
from os.path import join as osPathJoin, exists as osPathExists
from pickle import dump as pickleDump, load as pickleLoad
import pandas as pd

from interfaces.data_saver import IDataSaver




class TempFileDataSaver(IDataSaver):
    """Temp file data saver for scrapers - writes to config.FS_TEMP/PROJECT_NAME/TempFileDataSaver"""
    
    Name = "TempFileDataSaver"
    
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
    def save_data(self, data: pd.DataFrame, destination: str) -> bool:
        """Save data to temp file (overwrites existing)"""
        try:
            cleaned_data = self._validate_and_clean_data(data, destination)
            if cleaned_data is None:
                return False  # Validation failed

            file_path = self._get_file_path(destination)
            
            # Support different file formats including pickle for complex objects
            if destination.endswith('.csv'):
                cleaned_data.to_csv(file_path, index=False)  # Use cleaned_data!
            elif destination.endswith('.parquet'):
                cleaned_data.to_parquet(file_path, index=False)  # Use cleaned_data!
            elif destination.endswith('.json'):
                cleaned_data.to_json(file_path, orient='records')  # Use cleaned_data!
            elif destination.endswith('.pkl'):
                # For complex objects, pandas DataFrames, or any Python object
                with open(file_path, 'wb') as f:
                    pickleDump(cleaned_data, f)  # Use cleaned_data!
            else:
                raise Exception("wrong file extension, unable to save data... (expected: *.csv, *.parquet, *.json, *.pkl)") 
            
            self.logger.debug("{0} : saved data to {1}".format(self.Name, file_path))
            return True
            
        except Exception as e:
            self.logger.error("{0} : failed to save data to {1} - {2}".format(
                self.Name, destination, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def save_with_backup(self, data: pd.DataFrame, destination: str) -> bool:
        """Save data with automatic backup - for temp files, this is same as save_data (no backup)"""
        # For temp files, we don't create backups - just save normally
        self.logger.warning("{0} : the backup of temp data is not allowed, backup cancelled...".format(self.Name))
        return self.save_data(data, destination)
    
    #-----------------------------------------------------------------------------------------------
    def read_data(self, source: str) -> Optional[pd.DataFrame]:
        """Read data from temp file - supports multiple formats including pickle"""
        try:
            file_path = self._get_file_path(source)
            if not osPathExists(file_path):
                return None
            
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
            return None
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if temp data saver is healthy"""
        try:
            return osPathExists(self._temp_dir)
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    # Internal helper methods
    def _get_file_path(self, destination: str) -> str:
        """Get full file path for temp file"""
        return osPathJoin(self._temp_dir, destination)
    
    #-----------------------------------------------------------------------------------------------       
    def _validate_and_clean_data(self, data: pd.DataFrame, destination: str) -> Optional[pd.DataFrame]:
        """Validate and clean data before saving - returns cleaned DataFrame or None if invalid"""
        try:
            from helpers.data_validator import validate_scraped_data, get_cleaned_dataframe

            # Validate data
            is_valid, warnings = validate_scraped_data(data, destination, self.logger)
            if not is_valid:
                self.logger.error("{0} : invalid data for {1} - skipping save".format(
                    self.Name, destination))
                return None

            # Log warnings
            for warning in warnings:
                self.logger.warning("{0} : {1} - {2}".format(self.Name, destination, warning))

            # Clean data
            cleaned_data, _ = get_cleaned_dataframe(data, self.logger)

            # Add metadata for tracking
            cleaned_data = cleaned_data.copy()
            if "source" not in cleaned_data.columns:
                cleaned_data["source"] = destination
            if "scraped_timestamp" not in cleaned_data.columns:
                cleaned_data["scraped_timestamp"] = pd.Timestamp.now(tz='UTC')

            self.logger.debug("{0} : validated and cleaned data for {1} - {2} rows, {3} columns".format(
                self.Name, destination, len(cleaned_data), len(cleaned_data.columns)))

            return cleaned_data

        except Exception as e:
            self.logger.error("{0} : validation/cleaning failed for {1} - {2}".format(
                self.Name, destination, str(e)))
            return None

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