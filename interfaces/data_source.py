#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional
from abc import ABC, abstractmethod
import pandas as pd



class IDataSource(ABC):
    """Required interface for data sources"""
    
    Name = "IDataSource"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        self.config = config
        self.logger = logger
        if name is not None: 
            self.Name = name
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def get_data(self, source: str) -> pd.DataFrame:
        """Get data from source"""
        pass
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def list_sources(self) -> List[str]:
        """List available data sources"""
        pass
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def health_check(self) -> bool:
        """Check if data source is healthy"""
        pass