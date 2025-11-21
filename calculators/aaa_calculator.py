#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional, Any, Dict
import pandas as pd
from datetime import datetime, timezone


from interfaces.calculator import ICalculator
from interfaces.data_source import IDataSource
from interfaces.data_saver import IDataSaver
from interfaces.data_backup import IDataBackup


#-----------------------------------------------------------------------------------------------
## AAA calculation ponderation matching to common strategies
STRATEGIES = {
    'balanced': {
        'name': 'Balanced',
        'description': 'Equal emphasis across all categories',
        'weights': {
            'valuation': 0.25,
            'profitability': 0.25,
            'growth': 0.25,
            'performance': 0.25
        }
    },
    'value': {
        'name': 'Value Investing', 
        'description': 'Focus on valuation metrics',
        'weights': {
            'valuation': 0.50,
            'profitability': 0.20,
            'growth': 0.15,
            'performance': 0.15
        }
    },
    'growth': {
        'name': 'Growth Investing',
        'description': 'Focus on growth potential',
        'weights': {
            'valuation': 0.20,
            'profitability': 0.25,
            'growth': 0.40, 
            'performance': 0.15
        }
    },
    'quality': {
        'name': 'Quality Investing',
        'description': 'Focus on profitability and quality',
        'weights': {
            'valuation': 0.20,
            'profitability': 0.45,
            'growth': 0.20,
            'performance': 0.15
        }
    },
    'momentum': {
        'name': 'Momentum Investing',
        'description': 'Focus on recent performance',
        'weights': {
            'valuation': 0.15,
            'profitability': 0.20,
            'growth': 0.20,
            'performance': 0.45
        }
    }
}
## AAA calculation ponderation matching to common strategies
#-----------------------------------------------------------------------------------------------





class AAACalculator(ICalculator):
    """AAA Calculator - Independent calculation with full control over logic, saving, and backup"""
    
    Name = "AAACalculator"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        self.ponderation = None
    
    #-----------------------------------------------------------------------------------------------
    def run_complete_calculation(self, data_source: IDataSource, data_saver: IDataSaver, 
                               data_backup: IDataBackup, strategy: str = 'balanced', weighted_ratios: Dict = None, sources: Optional[List[str]] = None) -> List[Any]:
        """
        Run complete AAA calculation process with full control
        """
        errors = []

        calcualation_strategy = STRATEGIES.get(strategy, "balanced")
        self.ponderation = STRATEGIES.get(calcualation_strategy, "balanced").get('weights')

        if weighted_ratios is not None: 
            for ratio, ponderation in weighted_ratios.items():
                self.ponderation[ratio] = float(ponderation)
        
        self.logger.info("{0} : starting complete AAA calculation process, with AAA calculation strategy : {1}".format(self.Name, calcualation_strategy))
        
        try:
            # Step 1: Calculate for sectors
            sector_errors = self._calculate_for_sectors(data_source, data_saver, data_backup, sources)
            errors.extend([f"Sector error: {err}" for err in sector_errors])
            
            # Step 2: Calculate for indexes
            index_errors = self._calculate_for_indexes(data_source, data_saver, data_backup)
            errors.extend([f"Index error: {err}" for err in index_errors])
            
            # Step 3: Calculate for all data
            all_success = self._calculate_for_all(data_source, data_saver, data_backup)
            if not all_success:
                errors.append("All data calculation failed")
            
            if errors:
                self.logger.error("{0} : AAA calculation completed with {1} errors".format(self.Name, len(errors)))
            else:
                self.logger.info("{0} : AAA calculation completed successfully".format(self.Name))
                
        except Exception as e:
            error_msg = f"Complete AAA calculation failed: {str(e)}"
            self.logger.error("{0} : {1}".format(self.Name, error_msg))
            errors.append(error_msg)
        
        return errors
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if calculator is healthy"""
        try:
            # Basic health check - verify we can import required dependencies
            import pandas as pd
            return True
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False
        
    #-----------------------------------------------------------------------------------------------
    def _get_fa_sectors(self) -> List[str]:
        """Get fundamental analysis sectors"""
        return ['basicmaterials', 'communicationservices', 'consumercyclical', 
                'consumerdefensive', 'energy', 'financial', 'healthcare', 
                'industrials', 'realestate', 'technology', 'utilities']
    
    #-----------------------------------------------------------------------------------------------
    def _get_indexes(self) -> List[str]:
        """Get stock indexes"""
        return ["SnP500", "MegaCap", "LargeCap", "MidCap", "SmallCap", "MicroCap"]
    
    #-----------------------------------------------------------------------------------------------
    def _calculate_for_sectors(self, data_source: IDataSource, data_saver: IDataSaver, 
                             data_backup: IDataBackup, sectors: Optional[List[str]] = None) -> List[str]:
        """Calculate AAA ratings for multiple sectors"""
        errors = []
        
        if sectors is None:
            sectors = self._get_fa_sectors()
        
        for sector in sectors:
            source = f"AAA - {sector}.csv"
            destination = f"AAA_{sector}"
            
            success = self._calculate_and_save(data_source, data_saver, data_backup, source, destination)
            if not success:
                errors.append(f"Sector {sector}")
            else:
                self.logger.info("{0} : AAA calculation for sector '{1}' completed at {2}".format(
                    self.Name, sector, datetime.now(timezone.utc).isoformat()))
        
        return errors
    
    #-----------------------------------------------------------------------------------------------
    def _calculate_for_indexes(self, data_source: IDataSource, data_saver: IDataSaver, 
                             data_backup: IDataBackup) -> List[str]:
        """Calculate AAA ratings for indexes"""
        errors = []
        indexes = self._get_indexes()
        
        for index in indexes:
            source = f"AAA - {index}.csv"
            destination = f"AAA_{index}"
            
            success = self._calculate_and_save(data_source, data_saver, data_backup, source, destination)
            if not success:
                errors.append(f"Index {index}")
            else:
                self.logger.info("{0} : AAA calculation for index '{1}' completed at {2}".format(
                    self.Name, index, datetime.now(timezone.utc).isoformat()))
        
        return errors
    
    #-----------------------------------------------------------------------------------------------
    def _calculate_for_all(self, data_source: IDataSource, data_saver: IDataSaver, 
                         data_backup: IDataBackup) -> bool:
        """Calculate AAA ratings for all data"""
        source = "AAA - all.csv"
        destination = "AAA_all"
        
        success = self._calculate_and_save(data_source, data_saver, data_backup, source, destination)
        if success:
            self.logger.info("{0} : AAA calculation for all data completed at {1}".format(
                self.Name, datetime.now(timezone.utc).isoformat()))
        
        return success
    
    #-----------------------------------------------------------------------------------------------
    def _calculate_and_save(self, data_source: IDataSource, data_saver: IDataSaver, 
                          data_backup: IDataBackup, source: str, destination: str) -> bool:
        """
        Complete AAA calculation process with full control over saving and backup
        """
        try:
            self.logger.info("{0} : starting AAA calculation for {1}".format(self.Name, source))
            
            # Step 1: Get data from data source
            raw_data = data_source.get_data(source)
            if raw_data.empty:
                self.logger.error("{0} : no data found for {1}".format(self.Name, source))
                return False
            
            # Step 2: Preprocess data
            processed_data = raw_data.fillna(0)
            
            # Step 3: Calculate individual scores and grades
            scored_data = self._set_grade(processed_data)
            
            # Step 4: Perform AAA calculation
            aaa_data = self._make_aaa_calculation(scored_data)
            
            # Step 5: Create backup before saving
            self.logger.info("{0} : creating backup for {1}".format(self.Name, destination))
            backup_success, backup_error = data_backup.backup_data(destination)
            if not backup_success:
                self.logger.warning("{0} : backup failed for {1} - {2}".format(
                    self.Name, destination, backup_error))
            
            # Step 6: Save results
            self.logger.info("{0} : saving AAA results to {1}".format(self.Name, destination))
            save_success = data_saver.save_data(aaa_data, destination)
            
            if save_success:
                self.logger.info("{0} : AAA calculation completed successfully for {1}".format(
                    self.Name, source))
                return True
            else:
                self.logger.error("{0} : failed to save AAA results for {1}".format(
                    self.Name, destination))
                return False
                
        except Exception as e:
            self.logger.error("{0} : AAA calculation failed for {1} - {2}".format(
                self.Name, source, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def _set_grade(self, df_tickers_sector: pd.DataFrame) -> pd.DataFrame:
        """Calculate individual metric scores and grades"""
        valuation_metrics = ['fwd_p_e', 'peg', 'p_s', 'p_b', 'p_fcf']
        # Calculate scores for numeric columns
        for column in df_tickers_sector.select_dtypes(include=['float']):
            if column not in ['_saved_timestamp', '_backup_timestamp']:
                if column in valuation_metrics:
                    # INVERT: Lower ratios = Higher scores
                    df_tickers_sector[f"score - {column}"] = 10 - self._scale_to_10(
                        df_tickers_sector[column],
                        df_tickers_sector[column].min(skipna=True),
                        df_tickers_sector[column].max(skipna=True)
                    )
                else:
                    df_tickers_sector[f"score - {column.lower()}"] = self._scale_to_10(
                        df_tickers_sector[column],
                        df_tickers_sector[column].min(skipna=True),
                        df_tickers_sector[column].max(skipna=True)
                    )
        
        # Convert scores to grades
        for column in df_tickers_sector.columns:
            if column.startswith("score - "):
                grade_col = f"AAA - {column.replace('score - ', '').lower()}"
                df_tickers_sector[grade_col] = df_tickers_sector[column].apply(self._convert_to_grade)
        
        return df_tickers_sector
    
    #-----------------------------------------------------------------------------------------------
    def _make_aaa_calculation(self, df_tickers: pd.DataFrame) -> pd.DataFrame:
        """Perform complete AAA calculation"""
        df_tickers = self._set_valuation_grade(df_tickerdf_tickers=df_tickers,
                                               fwd_pe_ponderation=self.ponderation.get('fwd_pe', 1),
                                               peg_ponderation=self.ponderation.get('peg', 1),
                                               ps_ponderation=self.ponderation.get('ps', 1),
                                               pb_ponderation=self.ponderation.get('pb', 1),
                                               pfcf_ponderation=self.ponderation.get('pfcf', 1))
        df_tickers = self._set_profitability_grade(df_tickers=df_tickers,
                                                   profit_margin_ponderation=self.ponderation.get('profit_margin', 1),
                                                   operating_margin_ponderation=self.ponderation.get('operating_magin', 1),
                                                   gross_margin_ponderation=self.ponderation.get('gross_margin', 1),
                                                   roe_ponderation=self.ponderation.get('roe', 1),
                                                   roa_ponderation=self.ponderation.get('roa', 1),)
        df_tickers = self._set_growth_grade(df_tickers=df_tickers, 
                                            eps_this_y_ponderation=self.ponderation.get('eps_this_y', 1),
                                            eps_next_y_ponderation=self.ponderation.get('eps_next_y', 1),
                                            eps_next_5y_ponderation=self.ponderation.get('eps_next_5y', 1),
                                            sales_qq_ponderation=self.ponderation.get('sales_qq', 1),
                                            eps_qq_ponderation=self.ponderation.get('eps_qq', 1))
        df_tickers = self._set_performance_grade(df_tickers=df_tickers, 
                                                 perf_month_ponderation=self.ponderation.get('perf_month', 1),
                                                 perf_quarter_ponderation=self.ponderation.get('perf_quarter', 1),
                                                 perf_half_year_ponderation=self.ponderation.get('perf_half_year', 1),
                                                 perf_year_ponderation=self.ponderation.get('perf_year', 1),
                                                 perf_ytd_ponderation=self.ponderation.get('perf_ytd', 1))
        df_tickers = self._set_overall_rating(df_tickers=df_tickers, 
                                              val_grade_ponderation=self.ponderation['valuation'],
                                              prof_grade_ponderation=self.ponderation['profitability'],
                                              grow_grade_ponderation=self.ponderation['growth'],
                                              perf_grade_ponderation=self.ponderation['performance'])
        return df_tickers
    
    #-----------------------------------------------------------------------------------------------
    def _set_valuation_grade(self, df_tickers: pd.DataFrame, fwd_pe_ponderation: float = 1, 
                           peg_ponderation: float = 1, ps_ponderation: float = 1, 
                           pb_ponderation: float = 1, pfcf_ponderation: float = 1) -> pd.DataFrame:
        """Calculate valuation grade"""
        df_tickers["score - valuation"] = (
            df_tickers['score - fwd_p_e'] * fwd_pe_ponderation +
            df_tickers['score - peg'] * peg_ponderation +
            df_tickers['score - p_s'] * ps_ponderation +
            df_tickers['score - p_b'] * pb_ponderation +
            df_tickers['score - p_fcf'] * pfcf_ponderation
        )
        
        df_tickers["score - valuation"] = 10 - self._scale_to_10(
            df_tickers["score - valuation"], 
            df_tickers["score - valuation"].min(skipna=True), 
            df_tickers["score - valuation"].max(skipna=True)
        )
        df_tickers["AAA - valuation"] = df_tickers["score - valuation"].apply(self._convert_to_grade)
        return df_tickers
    
    #-----------------------------------------------------------------------------------------------
    def _set_profitability_grade(self, df_tickers: pd.DataFrame, profit_margin_ponderation: float = 1,
                               operating_margin_ponderation: float = 1, gross_margin_ponderation: float = 1,
                               roe_ponderation: float = 1, roa_ponderation: float = 1) -> pd.DataFrame:
        """Calculate profitability grade"""
        df_tickers["score - profitability"] = (
            df_tickers['score - profit_m'] * profit_margin_ponderation +
            df_tickers['score - oper_m'] * operating_margin_ponderation +
            df_tickers['score - gross_m'] * gross_margin_ponderation +
            df_tickers['score - roe'] * roe_ponderation +
            df_tickers['score - roa'] * roa_ponderation
        )
        
        df_tickers["score - profitability"] = self._scale_to_10(
            df_tickers["score - profitability"],
            df_tickers["score - profitability"].min(skipna=True),
            df_tickers["score - profitability"].max(skipna=True)
        )
        df_tickers["AAA - profitability"] = df_tickers["score - profitability"].apply(self._convert_to_grade)
        return df_tickers
    
    #-----------------------------------------------------------------------------------------------
    def _set_growth_grade(self, df_tickers: pd.DataFrame, eps_this_y_ponderation: float = 1,
                         eps_next_y_ponderation: float = 1, eps_next_5y_ponderation: float = 1,
                         sales_qq_ponderation: float = 1, eps_qq_ponderation: float = 1) -> pd.DataFrame:
        """Calculate growth grade"""
        df_tickers["score - growth"] = (
            df_tickers['score - eps_this_y'] * eps_this_y_ponderation +
            df_tickers['score - eps_next_y'] * eps_next_y_ponderation +
            df_tickers['score - eps_next_5y'] * eps_next_5y_ponderation +
            df_tickers['score - sales_q_q'] * sales_qq_ponderation +
            df_tickers['score - eps_q_q'] * eps_qq_ponderation
        )
        
        df_tickers["score - growth"] = self._scale_to_10(
            df_tickers["score - growth"],
            df_tickers["score - growth"].min(skipna=True),
            df_tickers["score - growth"].max(skipna=True)
        )
        df_tickers["AAA - growth"] = df_tickers["score - growth"].apply(self._convert_to_grade)
        return df_tickers
    
    #-----------------------------------------------------------------------------------------------
    def _set_performance_grade(self, df_tickers: pd.DataFrame, perf_month_ponderation: float = 1,
                             perf_quarter_ponderation: float = 1, perf_half_year_ponderation: float = 1,
                             perf_year_ponderation: float = 1, perf_ytd_ponderation: float = 1,
                             volatility_ponderation: float = 1) -> pd.DataFrame:
        """Calculate performance grade"""
        df_tickers["score - performance"] = (
            df_tickers['score - perf_month'] * perf_month_ponderation +
            df_tickers['score - perf_quart'] * perf_quarter_ponderation +
            df_tickers['score - perf_half'] * perf_half_year_ponderation +
            df_tickers['score - perf_year'] * perf_year_ponderation +
            df_tickers['score - perf_ytd'] * perf_ytd_ponderation +
            df_tickers['score - volatility_m'] * volatility_ponderation
        )
        
        df_tickers["score - performance"] = self._scale_to_10(
            df_tickers["score - performance"],
            df_tickers["score - performance"].min(skipna=True),
            df_tickers["score - performance"].max(skipna=True)
        )
        df_tickers["AAA - performance"] = df_tickers["score - performance"].apply(self._convert_to_grade)
        return df_tickers
    
    #-----------------------------------------------------------------------------------------------
    def _set_overall_rating(self, df_tickers: pd.DataFrame, val_grade_ponderation: float = 1,
                          prof_grade_ponderation: float = 1, grow_grade_ponderation: float = 1,
                          perf_grade_ponderation: float = 1) -> pd.DataFrame:
        """Calculate overall rating"""
        df_tickers["score - overall"] = (
            df_tickers["score - valuation"] * val_grade_ponderation +
            df_tickers['score - profitability'] * prof_grade_ponderation +
            df_tickers['score - growth'] * grow_grade_ponderation +
            df_tickers['score - performance'] * perf_grade_ponderation
        )
        
        df_tickers["score - overall"] = self._scale_to_10(
            df_tickers["score - overall"],
            df_tickers["score - overall"].min(skipna=True),
            df_tickers["score - overall"].max(skipna=True)
        )
        df_tickers["AAA - overall"] = df_tickers["score - overall"].apply(self._convert_to_grade)
        return df_tickers
    
    #-----------------------------------------------------------------------------------------------
    def _convert_to_grade(self, num: float) -> str:
        """Convert numerical score to letter grade"""
        if num >= 9.23: return 'A+'
        elif num >= 8.46: return 'A'
        elif num >= 7.69: return 'A-'
        elif num >= 6.92: return 'B+'
        elif num >= 6.15: return 'B'
        elif num >= 5.38: return 'B-'
        elif num >= 4.61: return 'C+'
        elif num >= 3.85: return 'C'
        elif num >= 3.08: return 'C-'
        elif num >= 2.31: return 'D+'
        elif num >= 1.54: return 'D'
        elif num >= 0.77: return 'D-'
        else: return 'F'
    
    #-----------------------------------------------------------------------------------------------
    def _scale_to_10(self, val: float, mine: float, maxe: float) -> float:
        """Scale value to 0-10 range"""
        if maxe == mine:
            return 5.0  # Neutral score for identical values
        return (val - mine) / (maxe - mine) * 10.0
