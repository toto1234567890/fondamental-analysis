#!/usr/bin/env python
# coding:utf-8

from typing import Optional
from abc import ABC, abstractmethod
import pandas as pd



class IDataSaver(ABC):
    """Required interface for data savers"""
    
    Name = "IDataSaver"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        self.config = config
        self.logger = logger
        if name is not None: 
            self.Name = name
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def save_data(self, data: pd.DataFrame, destination: str) -> bool:
        """Save data to destination"""
        pass
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def save_with_backup(self, data: pd.DataFrame, destination: str) -> bool:
        """Save data with automatic backup"""
        pass
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def health_check(self) -> bool:
        """Check if data saver is healthy"""
        pass