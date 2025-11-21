#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional, Tuple
from datetime import datetime, timezone

from interfaces.data_backup import IDataBackup




class ArcticBackupService(IDataBackup):
    """ArcticDB backup service implementation"""
    
    Name = "ArcticBackupService"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        self._store = None  # Single store instance for all operations
        self._library = None
        self._backup_libraries = {}  # Dictionary to store backup libraries by year
        self._setup_arctic()
    
    #-----------------------------------------------------------------------------------------------
    def _setup_arctic(self) -> None:
        """Setup ArcticDB connection - single store for all operations"""
        try:
            # Single store instance for all operations
            from arctic import Arctic
            self._store = Arctic(self.config.ARCTIC_HOST)
            
            # Main library
            from src import PROJECT_NAME
            library_name = "{0}_{1}".format(PROJECT_NAME, self.Name)
            if library_name not in self._store.list_libraries():
                self._store.initialize_library(library_name)
            self._library = self._store[library_name]
            
            # Initialize current year backup library
            current_year = datetime.now(timezone.utc).strftime('%Y')
            self._ensure_backup_library_exists(current_year)
            
            self.logger.debug("{0} : ArcticDB backup connection established".format(self.Name))
        except ImportError:
            self.logger.critical("{0} : ArcticDB package required but not installed".format(self.Name))
            exit(1)
        except Exception as e:
            self.logger.error("{0} : failed to setup ArcticDB backup - {1}".format(self.Name, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def _ensure_backup_library_exists(self, year: str) -> None:
        """Ensure backup library exists for specific year using existing store"""
        try:
            if self._store is None:
                raise Exception("Arctic store not initialized")
                
            from src import PROJECT_NAME
            base_library_name = "{0}_{1}".format(PROJECT_NAME, self.Name)
            backup_library_name = f"{base_library_name}_backup_{year}"
            
            if backup_library_name not in self._store.list_libraries():
                self._store.initialize_library(backup_library_name)
                self.logger.debug("{0} : created backup library for year {1}".format(self.Name, year))
            
            self._backup_libraries[year] = self._store[backup_library_name]
            
        except Exception as e:
            self.logger.error("{0} : failed to ensure backup library for year {1} - {2}".format(
                self.Name, year, str(e)))
            raise

    #-----------------------------------------------------------------------------------------------
    def backup_data(self, source: str) -> Tuple[bool, Optional[str]]:
        """Versioned backup - keeps all historical backups in year-based libraries"""
        try:
            if self._library is None:
                return False, "No ArcticDB connection"
            
            if source not in self._library.list_symbols():
                return False, f"Source symbol {source} does not exist"
            
            # Read clean data (no modifications)
            data = self._library.read(source)
            df = data.data if hasattr(data, 'data') else data
            
            # Get current year and timestamp
            timestamp = datetime.now(timezone.utc)
            year = timestamp.strftime('%Y')
            timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
            
            # Get or create backup library for this year
            backup_library = self._get_backup_library(year)
            if backup_library is None:
                return False, f"Failed to access backup library for year {year}"
            
            # Create versioned backup symbol with timestamp
            backup_symbol = f"{source}_backup_{timestamp_str}"
            
            # Write to year-specific backup library (never overwrites)
            backup_library.write(backup_symbol, df)
            
            self.logger.info("{0} : versioned backup completed for {1} in year {2}".format(
                self.Name, source, year))
            return True, None
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error("{0} : backup failed for {1} - {2}".format(self.Name, source, error_msg))
            return False, error_msg
    
    #-----------------------------------------------------------------------------------------------
    def backup_all(self) -> List[Tuple[str, str]]:
        """Backup all symbols with versioning in year-based libraries"""
        try:
            if self._library is None:
                return [("all", "No ArcticDB connection")]
            
            symbols = self._library.list_symbols()
            errors = []
            
            for symbol in symbols:
                if not symbol.endswith('_backup'):
                    success, error = self.backup_data(symbol)
                    if not success:
                        errors.append((symbol, error or "Unknown error"))
            
            if errors:
                self.logger.error("{0} : backup completed with {1} errors".format(self.Name, len(errors)))
            else:
                self.logger.info("{0} : all backups completed successfully at {1}".format(
                    self.Name, datetime.now(timezone.utc).isoformat()))
            
            return errors
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error("{0} : backup all failed - {1}".format(self.Name, error_msg))
            return [("all", error_msg)]
        
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if backup service is healthy"""
        try:
            current_year = datetime.now(timezone.utc).strftime('%Y')
            return (self._library is not None and 
                   self._get_backup_library(current_year) is not None and 
                   len(self._library.list_symbols()) >= 0)
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False

    #-----------------------------------------------------------------------------------------------
    def _get_backup_library(self, year: str):
        """Get backup library for specific year using existing store"""
        try:
            if self._store is None:
                return None
                
            if year not in self._backup_libraries:
                self._ensure_backup_library_exists(year)
            return self._backup_libraries[year]
        except Exception as e:
            self.logger.error("{0} : failed to get backup library for year {1} - {2}".format(
                self.Name, year, str(e)))
            return None

