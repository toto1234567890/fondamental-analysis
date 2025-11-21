#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional, Dict
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from interfaces.data_saver import IDataSaver
from interfaces.scraper import IScraper



class FinvizScraper(IScraper):
    """
    Finviz.com financial data scraper implementation.
    Handles parallel scraping of sectors and indexes with proper pagination.
    All pages for each source are merged, and final 'all' file combines all saved CSV files.
    """
    
    Name = "FinvizScraper"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        self._setup_scraping_tools()
    
    #-----------------------------------------------------------------------------------------------
    def _setup_scraping_tools(self) -> None:
        """Setup scraping dependencies - called from __init__"""
        try:
            from helpers.proxy import getHttpProxy, getUserAgent
            self.proxy_pool = getHttpProxy()
            self.agent_pool = getUserAgent()
        except ImportError as e:
            self.logger.critical("{0} : required scraping dependencies missing - {1}".format(self.Name, str(e)))
            exit(1)
    
    #-----------------------------------------------------------------------------------------------
    def scrape_data(self, data_saver: IDataSaver) -> bool:
        """
        Main scraping entry point - scrapes all available sources in parallel
        and saves results using provided data saver.
        """
        try:
            sources = self.get_available_sources()
            
            self.logger.info("{0} : starting parallel scraping of {1} sources".format(
                self.Name, len(sources)))
            
            # Scrape all sources in parallel (each source merges all its pages)
            scraped_data = self._scrape_sources_parallel(sources)
            
            if scraped_data:
                # Save individual source files first
                success_count = self._save_individual_sources(scraped_data, data_saver)
                
                # Create combined 'all' file by merging saved CSV files (robust approach)
                all_success = self._create_combined_file_from_csv(data_saver, list(scraped_data.keys()))
                
                if all_success:
                    success_count += 1
                
                self.logger.info("{0} : successfully processed {1}/{2} sources".format(
                    self.Name, success_count, len(sources) + 1))  # +1 for combined file
                return success_count > 0
            else:
                self.logger.error("{0} : no data scraped from any source".format(self.Name))
                return False
                
        except Exception as e:
            self.logger.error("{0} : scraping failed - {1}".format(self.Name, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def scrape_single_source(self, source_name: str, data_saver: IDataSaver) -> bool:
        """Scrape a single specific source by name (all pages merged)"""
        try:
            sources = self.get_available_sources()
            if source_name not in sources:
                self.logger.error("{0} : source '{1}' not found".format(self.Name, source_name))
                return False
            
            url = sources[source_name]
            
            self.logger.info("{0} : scraping single source '{1}'".format(self.Name, source_name))
            
            # Scrape the single source (all pages merged)
            source_data = self._finviz_scrape_data(url)
            
            if not source_data.empty:
                source_data["source"] = source_name
                source_data["scraped_timestamp"] = datetime.now(timezone.utc)
                
                # Save individual file - let saver handle folder structure
                destination = "AAA - {0}.csv".format(source_name)
                success = data_saver.save_data(source_data, destination)
                
                if success:
                    self.logger.info("{0} : '{1}' scraped and saved successfully".format(
                        self.Name, source_name))
                    return True
                else:
                    self.logger.error("{0} : failed to save '{1}'".format(self.Name, source_name))
                    return False
            else:
                self.logger.warning("{0} : no data scraped from {1}".format(self.Name, source_name))
                return False
                
        except Exception as e:
            self.logger.error("{0} : failed to scrape single source '{1}' - {2}".format(
                self.Name, source_name, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def get_available_sources(self) -> Dict[str, str]:
        """Get all available sources with their complete URLs"""
        sources = {}
        
        # Add sectors
        sectors = self._get_fa_sectors()
        base_sector_url = 'https://finviz.com/screener.ashx?v=152&c=0,1,2,3,4,5,6,8,9,10,11,13,17,18,20,22,23,32,33,34,35,36,37,38,39,40,41,43,44,45,46,47,51,67,65,66&f=sec_'
        for sector in sectors:
            sources[sector] = f"{base_sector_url}{sector}"
        
        # Add indexes
        index_urls = self._get_index_urls()
        sources.update(index_urls)
        
        return sources
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if scraper is healthy and can connect to target"""
        try:
            from requests import get as requestsGet
            test_url = "https://finviz.com"
            response = requestsGet(test_url, timeout=10)
            return response.status_code == 200
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    # Internal methods in usage order
    def _scrape_sources_parallel(self, sources: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """Scrape multiple sources in parallel using thread pool"""
        scraped_data = {}
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all scraping tasks
            future_to_source = {
                executor.submit(self._finviz_scrape_data, url): source_name 
                for source_name, url in sources.items()
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    data = future.result()
                    if not data.empty:
                        scraped_data[source_name] = data
                        self.logger.info("{0} : successfully scraped {1} records from {2}".format(
                            self.Name, len(data), source_name))
                    else:
                        self.logger.warning("{0} : no data from {1}".format(self.Name, source_name))
                except Exception as e:
                    self.logger.error("{0} : failed to scrape {1} - {2}".format(
                        self.Name, source_name, str(e)))
        
        return scraped_data
    
    #-----------------------------------------------------------------------------------------------
    def _save_individual_sources(self, scraped_data: Dict[str, pd.DataFrame], 
                                 data_saver: IDataSaver) -> int:
        """Save all scraped data to individual files"""
        success_count = 0
        
        for source_name, source_data in scraped_data.items():
            try:
                source_data = source_data.copy()
                source_data["source"] = source_name
                source_data["scraped_timestamp"] = datetime.now(timezone.utc)
                
                # Let saver handle folder structure - just provide filename
                destination = "AAA - {0}.csv".format(source_name)
                if data_saver.save_data(source_data, destination):
                    success_count += 1
                    self.logger.debug("{0} : saved {1} records to {2}".format(
                        self.Name, len(source_data), source_name))
                else:
                    self.logger.error("{0} : failed to save {1}".format(self.Name, source_name))
                    
            except Exception as e:
                self.logger.error("{0} : error saving {1} - {2}".format(
                    self.Name, source_name, str(e)))
        
        return success_count
    
    #-----------------------------------------------------------------------------------------------
    def _create_combined_file_from_csv(self, data_saver: IDataSaver, 
                                       source_names: List[str]) -> bool:
        """Create combined 'all' file by reading and merging all individual CSV files"""
        try:
            all_data_frames = []
            successful_sources = []
            
            # Read and merge all individual source files that were successfully saved
            for source_name in source_names:
                try:
                    # Use data saver's read method to get the individual file
                    individual_data = data_saver.read_data("AAA - {0}.csv".format(source_name))
                    if individual_data is not None and not individual_data.empty:
                        all_data_frames.append(individual_data)
                        successful_sources.append(source_name)
                        self.logger.debug("{0} : merged data from {1}".format(self.Name, source_name))
                    else:
                        self.logger.warning("{0} : no data to merge from {1}".format(self.Name, source_name))
                except Exception as e:
                    self.logger.error("{0} : error reading {1} for merge - {2}".format(
                        self.Name, source_name, str(e)))
            
            if all_data_frames:
                # Combine all data from successful sources
                combined_data = pd.concat(all_data_frames, ignore_index=True)
                
                # Save combined file - let saver handle folder structure
                destination = "AAA - all.csv"
                success = data_saver.save_data(combined_data, destination)
                
                if success:
                    self.logger.info("{0} : combined file created with {1} sources ({2} total records)".format(
                        self.Name, len(successful_sources), len(combined_data)))
                    return True
                else:
                    self.logger.error("{0} : failed to save combined file".format(self.Name))
                    return False
            else:
                self.logger.warning("{0} : no data to combine from any source".format(self.Name))
                return False
                
        except Exception as e:
            self.logger.error("{0} : failed to create combined file - {1}".format(self.Name, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def _finviz_scrape_data(self, url: str) -> pd.DataFrame:
        """Finviz-specific scraping logic - extracts ALL paginated data from a source"""
        try:
            from requests import get as requestsGet
            from bs4 import BeautifulSoup
            from helpers.misc import convertDigits
            
            all_data = []
            stocks_added = 0
            first_page = True
            headers = []

            while True:
                page_url = f"{url}&r={stocks_added + 1}"
                page = requestsGet(page_url, headers=next(self.agent_pool), 
                                 proxies={"http": next(self.proxy_pool)})
                soup = BeautifulSoup(page.content, 'html.parser')
                screener_data = soup.find(name='table', 
                                        class_="styled-table-new is-rounded is-tabular-nums w-full screener_table")

                if screener_data is None:
                    break

                rows = screener_data.find_all(name='tr')
                
                if first_page:
                    headers = self._parse_headers(rows[0])
                    stocks_added -= 1
                    first_page = False
                else:
                    rows = rows[1:]

                page_data = self._parse_rows(rows)
                
                # Skip empty records (first row might be empty)
                if page_data and len(page_data[0]) > 0:
                    all_data.extend(page_data)
                    stocks_added += len(page_data)

                # Continue scraping until no more data (when page has less than 20 records)
                if len(page_data) < 20:
                    break

            if all_data:
                df = pd.DataFrame(all_data, columns=headers)
                df = df.applymap(convertDigits)
                self.logger.debug("{0} : scraped {1} total records from URL".format(self.Name, len(df)))
                return df
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error("{0} : Finviz scraping failed for URL - {1}".format(self.Name, str(e)))
            return pd.DataFrame()
    
    #-----------------------------------------------------------------------------------------------
    def _get_fa_sectors(self) -> List[str]:
        """Get fundamental analysis sectors"""
        return ['basicmaterials', 'communicationservices', 'consumercyclical', 
                'consumerdefensive', 'energy', 'financial', 'healthcare', 
                'industrials', 'realestate', 'technology', 'utilities']
    
    #-----------------------------------------------------------------------------------------------
    def _get_index_urls(self) -> Dict[str, str]:
        """Get index URLs"""
        return {
            "SnP500": 'https://finviz.com/screener.ashx?v=152&c=0,1,2,3,4,5,6,8,9,10,11,13,17,18,20,22,23,32,33,34,35,36,37,38,39,40,41,43,44,45,46,47,51,67,65,66&f=idx_sp500',
            "MegaCap": 'https://finviz.com/screener.ashx?v=152&c=0,1,2,3,4,5,6,8,9,10,11,13,17,18,20,22,23,32,33,34,35,36,37,38,39,40,41,43,44,45,46,47,51,67,65,66&f=cap_mega',
            "LargeCap": 'https://finviz.com/screener.ashx?v=152&c=0,1,2,3,4,5,6,8,9,10,11,13,17,18,20,22,23,32,33,34,35,36,37,38,39,40,41,43,44,45,46,47,51,67,65,66&f=cap_large',
            "MidCap": 'https://finviz.com/screener.ashx?v=152&c=0,1,2,3,4,5,6,8,9,10,11,13,17,18,20,22,23,32,33,34,35,36,37,38,39,40,41,43,44,45,46,47,51,67,65,66&f=cap_mid',
            "SmallCap": 'https://finviz.com/screener.ashx?v=152&c=0,1,2,3,4,5,6,8,9,10,11,13,17,18,20,22,23,32,33,34,35,36,37,38,39,40,41,43,44,45,46,47,51,67,65,66&f=cap_small',
            "MicroCap": 'https://finviz.com/screener.ashx?v=152&c=0,1,2,3,4,5,6,8,9,10,11,13,17,18,20,22,23,32,33,34,35,36,37,38,39,40,41,43,44,45,46,47,51,67,65,66&f=cap_micro'
        }
    
    #-----------------------------------------------------------------------------------------------
    def _parse_headers(self, header_row) -> List[str]:
        """Parse table headers from HTML"""
        headers_cells = [th.contents[0] for th in header_row.find_all('th') if not th.find_all()]
        headers = [cell.text.strip().lower() for cell in headers_cells]
        headers.insert(1, "Ticker")
        return headers
    
    #-----------------------------------------------------------------------------------------------
    def _parse_rows(self, rows) -> List[List[str]]:
        """Parse table rows from HTML - skip empty rows"""
        table_data = []
        for row in rows:
            cells = row.find_all('td')
            row_data = [cell.text.strip() for cell in cells]
            # Skip empty rows (check if row has meaningful data)
            if row_data and any(cell.strip() for cell in row_data):
                table_data.append(row_data)
        return table_data

