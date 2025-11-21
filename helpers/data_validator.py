#!/usr/bin/env python
# coding:utf-8

from typing import List, Tuple, Dict
from re import search as reSearch, sub as reSub
import pandas as pd



# PostgreSQL reserved keywords
POSTGRES_RESERVED = {
    'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc', 'asymmetric', 
    'authorization', 'between', 'binary', 'both', 'case', 'cast', 'check', 'collate', 
    'column', 'constraint', 'create', 'cross', 'current_date', 'current_role', 
    'current_time', 'current_timestamp', 'current_user', 'default', 'deferrable', 
    'desc', 'distinct', 'do', 'else', 'end', 'except', 'false', 'for', 'foreign', 
    'from', 'grant', 'group', 'having', 'in', 'initially', 'intersect', 'into', 
    'join', 'leading', 'left', 'like', 'limit', 'localtime', 'localtimestamp', 
    'natural', 'not', 'null', 'offset', 'on', 'only', 'or', 'order', 'outer', 
    'overlaps', 'placing', 'primary', 'references', 'right', 'select', 'session_user', 
    'similar', 'some', 'symmetric', 'table', 'then', 'to', 'trailing', 'true', 
    'union', 'unique', 'user', 'using', 'variadic', 'verbose', 'when', 'where'
}

#-----------------------------------------------------------------------------------------------
def validate_scraped_data(data: pd.DataFrame, source_name: str, logger: object = None) -> Tuple[bool, List[str]]:
    """
    Validate scraped data for database compatibility
    Returns: (is_valid, list_of_warnings)
    """
    warnings = []
    
    try:
        # Check for empty DataFrame
        if data.empty:
            return False, ["Empty DataFrame - no data scraped"]
        
        # Check for empty column names
        empty_columns = [col for col in data.columns if not col or col.strip() == '']
        if empty_columns:
            warnings.append(f"Found {len(empty_columns)} empty column names")
        
        # Check for reserved characters in column names
        problematic_columns = []
        reserved_keyword_columns = []
        
        for col in data.columns:
            if col:
                # Check for special characters
                if reSearch(r'[^a-zA-Z0-9_]', str(col)):
                    problematic_columns.append(col)
                
                # Check for reserved keywords
                if col.lower() in POSTGRES_RESERVED:
                    reserved_keyword_columns.append(col)
        
        if problematic_columns:
            warnings.append(f"Column names with special characters: {problematic_columns}")
        
        if reserved_keyword_columns:
            warnings.append(f"Column names that are PostgreSQL reserved keywords: {reserved_keyword_columns}")
        
        # Check for duplicate column names
        if len(data.columns) != len(set(data.columns)):
            duplicates = [col for col in data.columns if list(data.columns).count(col) > 1]
            warnings.append(f"Duplicate column names: {duplicates}")
        
        # Check data types that might cause issues
        object_columns = data.select_dtypes(include=['object']).columns.tolist()
        if object_columns:
            warnings.append(f"Object/string columns that may need type conversion: {object_columns}")
        
        # Check for excessive null values (more than 50%)
        high_null_columns = []
        for col in data.columns:
            null_percentage = data[col].isnull().mean()
            if null_percentage > 0.5:  # 50% threshold
                high_null_columns.append((col, f"{null_percentage:.1%}"))
        
        if high_null_columns:
            warnings.append(f"Columns with high null percentages: {high_null_columns}")
        
        # Check row count (sanity check)
        if len(data) == 0:
            warnings.append("No rows in scraped data")
        elif len(data) > 10000:  # Arbitrary large number
            warnings.append(f"Large dataset: {len(data)} rows - consider partitioning")
        
        # Data is valid if we have at least some data and no critical errors
        is_valid = len(data) > 0 and len(data.columns) > 0 and not empty_columns
        
        if logger:
            logger.debug("DataValidator : data validation for {0} - valid: {1}, warnings: {2}".format(
                source_name, is_valid, len(warnings)))
        
        return is_valid, warnings
        
    except Exception as e:
        error_msg = f"Validation error: {str(e)}"
        if logger:
            logger.error("DataValidator : validation failed for {0} - {1}".format(source_name, error_msg))
        return False, [error_msg]

#-----------------------------------------------------------------------------------------------
def clean_column_name(column_name: str, index: int = None) -> str:
    """Clean column name for database compatibility"""
    if not column_name or column_name.strip() == '':
        # Generate a name for empty columns
        return f"column_{index}" if index is not None else "unknown_column"
    
    # Remove or replace problematic characters
    cleaned = reSub(r'[^a-zA-Z0-9_]', '_', str(column_name))
    
    # Ensure it starts with a letter or underscore
    if not cleaned or cleaned[0].isdigit():
        cleaned = f"col_{cleaned}"
    
    # Remove consecutive underscores
    cleaned = reSub(r'_+', '_', cleaned)
    
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    
    # Ensure it's not a reserved keyword
    if cleaned.lower() in POSTGRES_RESERVED:
        cleaned = f"_{cleaned}"
    
    # Ensure minimum length
    if len(cleaned) == 0:
        return f"column_{index}" if index is not None else "empty_column"
    
    return cleaned.lower()

#-----------------------------------------------------------------------------------------------
def get_cleaned_dataframe(df: pd.DataFrame, logger: object = None) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Clean all column names in a DataFrame and return cleaned DataFrame + mapping
    Returns: (cleaned_dataframe, column_mapping)
    """
    original_columns = list(df.columns)
    cleaned_columns = []
    column_mapping = {}
    
    for i, col in enumerate(original_columns):
        cleaned_col = clean_column_name(col, i)
        cleaned_columns.append(cleaned_col)
        column_mapping[cleaned_col] = col
        
        if logger and col != cleaned_col:
            logger.debug("ColumnCleaner : renamed '{0}' to '{1}'".format(col, cleaned_col))
    
    # Create new DataFrame with cleaned columns
    df_cleaned = df.copy()
    df_cleaned.columns = cleaned_columns
    
    return df_cleaned, column_mapping