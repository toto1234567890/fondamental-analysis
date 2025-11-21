#!/usr/bin/env python
# coding:utf-8

from factories.data_factory import DataFactory
from scrapers.finviz_scraper import FinvizScraper  # Optional
from calculators.aaa_calculator import AAACalculator    # Optional

from toReplace import get_config_logger


"""
ESSENTIAL PROCESS:
1. INIT: Setup configuration and logging for 'fondamental_analysis'
2. SCRAPE: Extract financial data from Finviz → save as temporary CSV files
3. CALCULATE: Process scraped data through AAA calculations → save to PostgreSQL
4. BACKUP: Automatic PostgreSQL backups with year-based versioning

KEY PARAMETERS:
- config: "fondamental_analysis" application configuration
- name: AAACalculator service identifier
- saver_type: "temp" (no backup) or "postgres" (with backup)
- backup_type: "postgres" for database backups

DATA FLOW:
Finviz API → Temp CSV Files → AAA Calculations → PostgreSQL Database → Automated Backups
"""

#================================================================
if __name__ == "__main__":  

    # init 
    name = AAACalculator.Name
    config, logger = get_config_logger(name="fondamental_analysis")

    # Create global data factory
    data_factory = DataFactory(config=config, logger=logger, name=name)
    
    # 1 - run scraping and save csv files
    # scraper = FinvizScraper(config=config, logger=logger, name=name)
    # temp_csv_saver = data_factory.create_data_saver(saver_type="temp", name=FinvizScraper.Name) 
    # scraper.scrape_data(data_saver=temp_csv_saver)

    # 2 - run calculation and save to postgres db
    AAA_calculation = AAACalculator(config=config, logger=logger, name=name)
    calculator_source = data_factory.create_data_source(source_type="temp", name=FinvizScraper.Name)
    calculator_saver = data_factory.create_data_saver(saver_type="postgres", name=name)
    calculator_backup = data_factory.create_data_backup(backup_type="postgres", name=name) 

    AAA_calculation.run_complete_calculation(data_backup=calculator_backup, 
                                             data_saver=calculator_saver, 
                                             data_source=calculator_source, 
                                             strategy='value')