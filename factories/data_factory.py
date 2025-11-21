#!/usr/bin/env python
# coding:utf-8

from typing import Optional, Dict, Any
from interfaces.data_source import IDataSource
from interfaces.data_saver import IDataSaver
from interfaces.data_backup import IDataBackup



class DataFactory:
    """Global factory for creating data sources, savers, and backup services"""
    
    Name = "DataFactory"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        self.config = config
        self.logger = logger
        if name is not None: 
            self.Name = name

        from src import PROJECT_NAME
        self._project_name = PROJECT_NAME  
        
        self._source_registry: Dict[str, Any] = {}
        self._saver_registry: Dict[str, Any] = {}
        self._backup_registry: Dict[str, Any] = {}
        
        self._setup_registries()
    
    #-----------------------------------------------------------------------------------------------
    def _setup_registries(self) -> None:
        """Setup all registries"""
        self._setup_source_registry()
        self._setup_saver_registry()
        self._setup_backup_registry()
        self.logger.debug("{0} : all registries initialized".format(self.Name))
    
    #-----------------------------------------------------------------------------------------------
    def _setup_source_registry(self) -> None:
        """Setup data source registry"""
        self._source_registry = {
            "csv": self._create_csv_source,
            "postgres": self._create_postgres_source,
            "arctic": self._create_arctic_source,
            "temp": self._create_temp_source  
        }
    
    #-----------------------------------------------------------------------------------------------
    def _setup_saver_registry(self) -> None:
        """Setup data saver registry"""
        self._saver_registry = {
            "csv": self._create_csv_saver,
            "postgres": self._create_postgres_saver,
            "arctic": self._create_arctic_saver,
            "temp": self._create_temp_saver
        }
    
    #-----------------------------------------------------------------------------------------------
    def _setup_backup_registry(self) -> None:
        """Setup backup service registry"""
        self._backup_registry = {
            "csv": self._create_csv_backup,
            "postgres": self._create_postgres_backup,
            "arctic": self._create_arctic_backup
        }
    
    #-----------------------------------------------------------------------------------------------
    # Data Source Methods
    def create_data_source(self, source_type: str, name: Optional[str] = None) -> IDataSource:
        """Create data source instance"""
        try:
            if source_type not in self._source_registry:
                raise ValueError("{0} : unknown data source type {1}".format(self.Name, source_type))
            
            creator_func = self._source_registry[source_type]
            instance = creator_func(name=name)
            
            self.logger.debug("{0} : created {1} data source".format(self.Name, source_type))
            return instance
            
        except Exception as e:
            self.logger.error("{0} : failed to create {1} data source - {2}".format(
                self.Name, source_type, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def list_data_sources(self) -> list:
        """List available data source types"""
        return list(self._source_registry.keys())
    
    #-----------------------------------------------------------------------------------------------
    # Data Saver Methods
    def create_data_saver(self, saver_type: str, name: Optional[str] = None) -> IDataSaver:
        """Create data saver instance"""
        try:
            if saver_type not in self._saver_registry:
                raise ValueError("{0} : unknown data saver type {1}".format(self.Name, saver_type))
            
            creator_func = self._saver_registry[saver_type]
            instance = creator_func(name=name)
            
            self.logger.debug("{0} : created {1} data saver".format(self.Name, saver_type))
            return instance
            
        except Exception as e:
            self.logger.error("{0} : failed to create {1} data saver - {2}".format(
                self.Name, saver_type, str(e)))
            raise

    #-----------------------------------------------------------------------------------------------
    def list_data_savers(self) -> list:
        """List available data saver types"""
        return list(self._saver_registry.keys()) 
    
    #-----------------------------------------------------------------------------------------------
    # Backup Service Methods
    def create_data_backup(self, backup_type: str, name: Optional[str] = None) -> IDataBackup:
        """Create backup service instance"""
        try:
            if backup_type not in self._backup_registry:
                raise ValueError("{0} : unknown backup service type {1}".format(self.Name, backup_type))
            
            creator_func = self._backup_registry[backup_type]
            instance = creator_func(name=name)
            
            self.logger.debug("{0} : created {1} backup service".format(self.Name, backup_type))
            return instance
            
        except Exception as e:
            self.logger.error("{0} : failed to create {1} backup service - {2}".format(
                self.Name, backup_type, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def list_data_backup(self) -> list:
        """List available backup service types"""
        return list(self._backup_registry.keys())
    
    #-----------------------------------------------------------------------------------------------
    # Creator Methods - Data Sources
    def _create_csv_source(self, name: Optional[str] = None) -> IDataSource:
        """Create CSV data source"""
        from data_sources.csv_source import CSVDataSource
        return CSVDataSource(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    def _create_postgres_source(self, name: Optional[str] = None) -> IDataSource:
        """Create PostgreSQL data source"""
        from data_sources.postgres_source import PostgresDataSource
        return PostgresDataSource(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    def _create_arctic_source(self, name: Optional[str] = None) -> IDataSource:
        """Create ArcticDB data source"""
        from data_sources.arctic_source import ArcticDataSource
        return ArcticDataSource(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    def _create_temp_source(self, name: Optional[str] = None) -> IDataSource:
        """Create temp file data source"""
        from data_sources.temp_source import TempFileDataSource
        return TempFileDataSource(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    # Creator Methods - Data Savers
    def _create_csv_saver(self, name: Optional[str] = None) -> IDataSaver:
        """Create CSV data saver"""
        from data_savers.csv_saver import CSVDataSaver
        return CSVDataSaver(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    def _create_postgres_saver(self, name: Optional[str] = None) -> IDataSaver:
        """Create PostgreSQL data saver"""
        from data_savers.postgres_saver import PostgresDataSaver
        return PostgresDataSaver(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    def _create_arctic_saver(self, name: Optional[str] = None) -> IDataSaver:
        """Create ArcticDB data saver"""
        from data_savers.arctic_saver import ArcticDataSaver
        return ArcticDataSaver(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    def _create_temp_saver(self, name: Optional[str] = None) -> IDataSaver:
        """Create temp file data saver"""
        from data_savers.temp_saver import TempFileDataSaver
        return TempFileDataSaver(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    # Creator Methods - Backup Services
    def _create_csv_backup(self, name: Optional[str] = None) -> IDataBackup:
        """Create CSV backup service"""
        from data_backups.csv_backup import CSVBackupService
        return CSVBackupService(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    def _create_postgres_backup(self, name: Optional[str] = None) -> IDataBackup:
        """Create PostgreSQL backup service"""
        from data_backups.postgres_backup import PostgresBackupService
        return PostgresBackupService(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    def _create_arctic_backup(self, name: Optional[str] = None) -> IDataBackup:
        """Create ArcticDB backup service"""
        from data_backups.arctic_backup import ArcticBackupService
        return ArcticBackupService(config=self.config, logger=self.logger, name=name)
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if factory is healthy"""
        try:
            return (len(self._source_registry) > 0 and 
                   len(self._saver_registry) > 0 and 
                   len(self._backup_registry) > 0)
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False


