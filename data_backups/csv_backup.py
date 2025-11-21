#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional, Tuple
from os import makedirs as osMakedirs, listdir as osListdir
from os.path import join as osPathJoin, exists as osPathExists, isfile as osPathIsfile, \
                    splitext as osPathSplitext, basename as osPathBasename
from datetime import datetime, timezone
import shutil

from interfaces.data_backup import IDataBackup



class CSVBackupService(IDataBackup):
    """CSV backup service implementation"""
    
    Name = "CSVBackupService"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        self._setup_directories()
    
    #-----------------------------------------------------------------------------------------------
    def _setup_directories(self) -> None:
        """Setup CSV backup directories"""
        try:
            from src import PROJECT_NAME
            self._data_dir = osPathJoin(getattr(self.config, 'FS_DATA', './data'), PROJECT_NAME, self.Name) 
            self._backup_dir = osPathJoin(getattr(self.config, 'FS_DATA', './data'), PROJECT_NAME, "{0}_backup".format(self.Name))

            # Create main directory
            if not osPathExists(self._data_dir):
                osMakedirs(self._data_dir, exist_ok=True)

            # Create backup directory
            if not osPathExists(self._backup_dir):
                osMakedirs(self._backup_dir, exist_ok=True)
            
            self.logger.debug("{0} : CSV backup directories configured".format(self.Name))
            
        except Exception as e:
            self.logger.error("{0} : failed to setup backup directories - {1}".format(self.Name, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def backup_data(self, source: str) -> Tuple[bool, Optional[str]]:
        """Backup CSV file"""
        try:
            source_path = self._get_source_path(source)
            if not osPathExists(source_path):
                return False, f"Source file not found: {source_path}"
            
            # Create backup with timestamp
            backup_path = self._get_backup_path(source)
            
            # Copy file to backup location
            shutil.copy2(source_path, backup_path)
            
            self.logger.info("{0} : backup completed for {1}".format(self.Name, source))
            return True, None
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error("{0} : backup failed for {1} - {2}".format(self.Name, source, error_msg))
            return False, error_msg
    
    #-----------------------------------------------------------------------------------------------
    def backup_all(self) -> List[Tuple[str, str]]:
        """Backup all CSV files in data directory"""
        try:
            csv_files = self._get_all_csv_files()
            errors = []
            
            for csv_file in csv_files:
                rel_path = osPathBasename(csv_file)
                success, error = self.backup_data(rel_path)
                if not success:
                    errors.append((rel_path, error or "Unknown error"))
            
            if errors:
                self.logger.error("{0} : backup completed with {1} errors".format(self.Name, len(errors)))
            else:
                self.logger.info("{0} : all backups completed successfully".format(self.Name))
            
            return errors
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error("{0} : backup all failed - {1}".format(self.Name, error_msg))
            return [("all", error_msg)]
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if backup service is healthy"""
        try:
            return (osPathExists(self._data_dir) and 
                   osPathExists(self._backup_dir))
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    # Internal helper methods
    def _get_source_path(self, source: str) -> str:
        """Get full source file path"""
        if not source.lower().endswith('.csv'):
            source += '.csv'
        return osPathJoin(self._data_dir, source)
    
    #-----------------------------------------------------------------------------------------------
    def _get_backup_path(self, source: str) -> str:
        """Get backup file path with year folder and timestamp"""
        if not source.lower().endswith('.csv'):
            source += '.csv'
        
        name_without_ext = osPathSplitext(source)[0]
        timestamp = datetime.now(timezone.utc)
        year_folder = timestamp.strftime('%Y')
        timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
        
        # Create year subdirectory
        year_backup_dir = osPathJoin(self._backup_dir, year_folder)
        if not osPathExists(year_backup_dir):
            osMakedirs(year_backup_dir, exist_ok=True)
        
        backup_filename = f"{name_without_ext}_backup_{timestamp_str}.csv"
        
        return osPathJoin(year_backup_dir, backup_filename)
    
    #-----------------------------------------------------------------------------------------------
    def _get_all_csv_files(self) -> List[str]:
        """Get all CSV files in data directory"""
        try:
            all_files = osListdir(self._data_dir)
            csv_files = []
            for file in all_files:
                file_path = osPathJoin(self._data_dir, file)
                if (osPathIsfile(file_path) and 
                    file.lower().endswith('.csv') and 
                    not file.startswith('.')):
                    csv_files.append(file_path)
            return csv_files
        except Exception as e:
            self.logger.error("{0} : failed to list CSV files - {1}".format(self.Name, str(e)))
            return []

