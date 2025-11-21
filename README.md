# Data Processing Pipeline Framework

A flexible, modular data processing framework designed for solo developers building micro-services. This framework provides a standardized architecture for data scraping, processing, calculation, and storage with interchangeable components.

## 🏗️ Architecture Overview

The framework follows a clean interface-based architecture with four main component types:

### Core Components

1. **Data Sources** (`IDataSource`) - Read data from various storage systems
2. **Data Savers** (`IDataSaver`) - Write data to different storage backends  
3. **Backup Services** (`IDataBackup`) - Handle data backup operations
4. **Calculators** (`ICalculator`) - Process and transform data
5. **Scrapers** (`IScraper`) - Optional web scraping capabilities

### Supported Storage Types

- **CSV Files** - Simple file-based storage with backup
- **PostgreSQL** - Relational database with schema management
- **ArcticDB** - Time-series database for financial data
- **Temp Files** - Temporary storage for scrapers and intermediate data

## ⚙️ Configuration & Setup

### Core Configuration Module

The configuration that handles both development and production environments, use config in except block:

```python
#!/usr/bin/env python
# coding:utf-8

config = None
logger = None

def get_config_logger(name, config=None):
    try: 
        # relative import
        from sys import path;path.extend("..")
        from common.Helpers.helpers import init_logger

        if config is None:
            config=name
        config, logger = init_logger(name=name, config=config)

    except:
        # Basic configuration fallback
        import logging
        logging.basicConfig(level=logging.DEBUG)
        
        class Config:
            # Database
            DB_SERVER = "localhost" # For Postgres server
            DB_NAME = "my_project"
            DB_USER = "postgres"
            DB_PASSWORD = "password"
            DB_PORT = 5432
            
            # File System
            FS_DATA = "./data"      # For file based storage / Arctic storage
            FS_TEMP = "./temp"      # For temporary files
            
            # ArcticDB
            ARCTIC_HOST = "localhost" # For ArcticDB server
            ARCTIC_LIBRARY = "my_project_data"

        logger = logging.getLogger()
        config = Config()
    return config, logger
```

## 🎯 Key Features

### 1. Interface-Based Design
All components implement strict interfaces, ensuring consistent behavior across implementations:

```python
# All data sources implement this interface
class IDataSource(ABC):
    def get_data(self, source: str) -> pd.DataFrame: ...
    def list_sources(self) -> List[str]: ...
    def health_check(self) -> bool: ...

# All data savers implement this interface  
class IDataSaver(ABC):
    def save_data(self, data: pd.DataFrame, destination: str) -> bool: ...
    def save_with_backup(self, data: pd.DataFrame, destination: str) -> bool: ...
    def health_check(self) -> bool: ...

# All backup services implement this interface
class IDataBackup(ABC):
    def backup_data(self, source: str) -> Tuple[bool, Optional[str]]: ...
    def backup_all(self) -> List[Tuple[str, str]]: ...
    def health_check(self) -> bool: ...

# All calculators implement this interface
class ICalculator(ABC):
    def run_complete_calculation(self, data_source: IDataSource, 
                               data_saver: IDataSaver,
                               backup_service: IDataBackup,
                               sources: Optional[List[str]] = None) -> List[Any]: ...
    def health_check(self) -> bool: ...

# All scrapers implement this interface
class IScraper(ABC):
    def scrape_data(self, data_saver: IDataSaver) -> bool: ...
    def scrape_single_source(self, source_name: str, data_saver: IDataSaver) -> bool: ...
    def get_available_sources(self) -> List[str]: ...
    def health_check(self) -> bool: ...
```

### 2. Factory Pattern
Centralized component creation with runtime type selection:

```python
# Create any component type dynamically
data_factory = DataFactory(config=config, logger=logger)

# Choose implementation at runtime
csv_source = data_factory.create_data_source("csv")
postgres_saver = data_factory.create_data_saver("postgres") 
arctic_backup = data_factory.create_data_backup("arctic")
calculator = AAACalculator(config, logger)

# Factory supports all component types
source_types = data_factory.list_data_sources()      # ['csv', 'postgres', 'arctic', 'temp']
saver_types = data_factory.list_data_savers()        # ['csv', 'postgres', 'arctic', 'temp']  
backup_types = data_factory.list_data_backup()       # ['csv', 'postgres', 'arctic']
```

### 3. Flexible Processing Pipeline
Mix and match components to build custom workflows:
```python
# Example 1: Web scraping → Temp storage → Calculation → PostgreSQL

scraper = FinvizScraper(config, logger)
temp_saver = factory.create_data_saver("temp")
calculator = AAACalculator(config, logger) 
postgres_saver = factory.create_data_saver("postgres")
postgres_backup = factory.create_data_backup("postgres")

# Execute complete pipeline
scraper.scrape_data(temp_saver)
calculator.run_complete_calculation(
    data_source=temp_source,
    data_saver=postgres_saver, 
    backup_service=postgres_backup
)
```

```python
# Example 2: Direct CSV to ArcticDB processing  
csv_source = factory.create_data_source("csv")
arctic_saver = factory.create_data_saver("arctic")
data = csv_source.get_data("input_data.csv")
arctic_saver.save_data(data, "processed_data")
```

```python
# Example 3: Multi-storage backup strategy
savers = [
    factory.create_data_saver("csv"),
    factory.create_data_saver("postgres"),
    factory.create_data_saver("arctic")
]
for saver in savers:
    saver.save_data(important_data, "backup_copy")
```

## 📊 AAA Calculation Explained (some other will be added ASAP)

### What is AAA Calculation?
AAA calculation is a comprehensive financial rating system that evaluates stocks across multiple dimensions to generate letter grades (A+ through F).

### Calculation Process
```python
┌─────────────────┐ 
Raw Financial Data 
└─────────────────┘ 
        │  
        ▼  
┌─────────────────┐  
│ Individual      │  
│ Metric Scores   │ ← Scale metrics to 0-10  
└─────────────────┘  
        │  
        ▼  
┌─────────────────┐  
│ Category        │  
│ Grades          │ ← Convert scores to letter grades  
└─────────────────┘  
        │  
        ▼  
┌─────────────────┐  
│ Overall         │  
│ AAA Rating      │ ← Weighted combination  
└─────────────────┘
```

### Rating Categories

1. **Valuation** (30%)
   - Forward P/E, PEG Ratio, P/S, P/B, P/FCF
   - Lower multiples = Better score

2. **Profitability** (25%)  
   - Profit Margin, Operating Margin, Gross Margin, ROE, ROA
   - Higher margins = Better score

3. **Growth** (25%)
   - EPS Growth (this year, next year, 5-year), Sales Growth
   - Higher growth = Better score

4. **Performance** (20%)
   - Price Performance (month, quarter, year, YTD)
   - Volatility (lower = better)
   - Better performance = Better score

### Grade Scale

<u>A+ ≥ 9.23</u>  
<u>A ≥ 8.46</u>  
<u>A- ≥ 7.69</u>  
<u>B+ ≥ 6.92</u>  
<u>B ≥ 6.15</u>  
<u>B- ≥ 5.38</u>  
<u>C+ ≥ 4.61</u>  
<u>C ≥ 3.85</u>  
<u>C- ≥ 3.08</u>  
<u>D+ ≥ 2.31</u>  
<u>D ≥ 1.54</u>  
<u>D- ≥ 0.77</u>  
<u>F < 0.77</u>


### Example Output
```python
# For each stock:
{
    "AAA - valuation": "A",
    "AAA - profitability": "B+", 
    "AAA - growth": "A-",
    "AAA - performance": "C+",
    "AAA - overall": "B+"
}
```
This provides a quick, standardized way to compare investment opportunities across different sectors and market caps.


*doc mostly generated by IA...* 
