#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional, Any
from abc import ABC, abstractmethod
from .data_source import IDataSource
from .data_saver import IDataSaver
from .data_backup import IDataBackup



class ICalculator(ABC):
    """Interface for all calculators"""
    
    Name = "ICalculator"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        self.config = config
        self.logger = logger
        if name is not None: 
            self.Name = name
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def run_complete_calculation(self, data_source: IDataSource, data_saver: IDataSaver, 
                               backup_service: IDataBackup, sources: Optional[List[str]] = None) -> List[Any]:
        """
        Run complete calculation process with full control
        Calculator decides what to calculate, how to save, and when to backup
        """
        pass
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def health_check(self) -> bool:
        """Check if calculator is healthy"""
        pass