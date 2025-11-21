#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional
import pandas as pd

from interfaces.data_source import IDataSource




class ArcticDataSource(IDataSource):
    """ArcticDB data source implementation"""
    
    Name = "ArcticDataSource"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        self._library = None
        self._setup_arctic()
    
    #-----------------------------------------------------------------------------------------------
    def _setup_arctic(self) -> None:
        """Setup ArcticDB connection"""
        try:
            from arctic import Arctic
            store = Arctic(self.config.ARCTIC_HOST)
            library_name = getattr(self.config, 'ARCTIC_LIBRARY', 'fundamental_analysis')
            if library_name not in store.list_libraries():
                store.initialize_library(library_name)
            self._library = store[library_name]
            self.logger.debug("{0} : ArcticDB connection established".format(self.Name))
        except ImportError:
            self.logger.critical("{0} : ArcticDB package required but not installed".format(self.Name))
            exit(1)
        except Exception as e:
            self.logger.error("{0} : failed to setup ArcticDB - {1}".format(self.Name, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def get_data(self, source: str) -> pd.DataFrame:
        """Get data from ArcticDB"""
        try:
            if self._library is None:
                return pd.DataFrame()
            
            data = self._library.read(source)
            df = data.data if hasattr(data, 'data') else data
            self.logger.debug("{0} : retrieved data from {1}".format(self.Name, source))
            return df
        except Exception as e:
            self.logger.error("{0} : failed to get data from {1} - {2}".format(self.Name, source, str(e)))
            return pd.DataFrame()
    
    #-----------------------------------------------------------------------------------------------
    def list_sources(self) -> List[str]:
        """List available symbols in ArcticDB"""
        try:
            if self._library is None:
                return []
            return self._library.list_symbols()
        except Exception as e:
            self.logger.error("{0} : failed to list sources - {1}".format(self.Name, str(e)))
            return []
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if ArcticDB is healthy"""
        try:
            return self._library is not None and len(self.list_sources()) >= 0
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False