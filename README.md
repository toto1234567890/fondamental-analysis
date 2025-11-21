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


    
