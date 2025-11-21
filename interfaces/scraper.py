#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional, Tuple, final
from abc import ABC, abstractmethod
import pandas as pd

from interfaces.data_saver import IDataSaver





class IScraper(ABC):
    """Optional interface for web scrapers"""
    
    Name = "IScraper"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        self.config = config
        self.logger = logger
        if name is not None: 
            self.Name = name
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def scrape_data(self, data_saver: IDataSaver) -> bool:
        """Scrape data and save using data saver interface"""
        pass

    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def scrape_single_source(self, source_name: str, data_saver: IDataSaver) -> bool:
        """Scrape a single specific source"""
        pass

    #-----------------------------------------------------------------------------------------------
    @final
    def validate_scraped_data(self, data: pd.DataFrame, source_name: str) -> Tuple[bool, List[str]]:
        """
        Validate scraped data for database compatibility, Arctic and CSV compatibility
        Returns: (is_valid, list_of_warnings)
        """
        from helpers.data_validator import validate_scraped_data

    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def get_available_sources(self) -> List[str]:
        """Get list of available data sources to scrape"""
        pass
    
    #-----------------------------------------------------------------------------------------------
    @abstractmethod
    def health_check(self) -> bool:
        """Check if scraper is healthy"""
        pass