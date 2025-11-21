#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional, Tuple
from abc import ABC, abstractmethod



class IDataBackup(ABC):
    """Required interface for backup"""
    
    Name = "IBackup"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        self.config = config
        self.logger = logger
        if name is not None: 
            self.Name = name
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def backup_data(self, source: str) -> Tuple[bool, Optional[str]]:
        """Backup data from source"""
        pass
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def backup_all(self) -> List[Tuple[str, str]]:
        """Backup all data sources"""
        pass
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def health_check(self) -> bool:
        """Check if backup service is healthy"""
        pass