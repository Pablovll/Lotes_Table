# core/interfaces.py
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
import pandas as pd
from core.models import DatabaseConfig, AuthenticationType

class IDatabaseConnection(ABC):
    @abstractmethod
    def connect(self, config) -> bool:
        pass
    
    @abstractmethod
    def get_tables(self) -> List[str]:
        pass
    
    @abstractmethod
    def get_tables_with_timestring(self) -> List[str]:
        pass
    
    @abstractmethod
    def read_table(self, table_name: str) -> Optional[pd.DataFrame]:
        pass
    
    @abstractmethod
    def create_lotedata_table(self, df: pd.DataFrame, table_name: str = 'LOTEDATA') -> bool:
        pass

class IDatabaseRepository(ABC):
    @abstractmethod
    def get_available_tables(self) -> List[str]:
        pass
    
    @abstractmethod
    def get_tables_with_timestring(self) -> List[str]:
        pass
    
    @abstractmethod
    def fetch_table_data(self, table_name: str) -> Optional[pd.DataFrame]:
        pass
    
    @abstractmethod
    def save_lotedata(self, df: pd.DataFrame, table_name: str = 'LOTEDATA') -> bool:
        pass
    
    @abstractmethod
    def check_table_has_timestring_fast(self, table_name: str) -> bool:
        pass

class ITableService(ABC):
    @abstractmethod
    def convert_timestring_to_datetime(self, df: pd.DataFrame, time_column: str = "TimeString") -> Optional[pd.DataFrame]:
        pass
    
    @abstractmethod
    def sort_table_by_timestring(self, df: pd.DataFrame, time_column: str = "TimeString") -> Optional[pd.DataFrame]:
        pass
    
    @abstractmethod
    def sort_multiple_tables(self, table_data: Dict[str, pd.DataFrame], time_column: str = "TimeString") -> Dict[str, pd.DataFrame]:
        pass
    
    @abstractmethod
    def verify_datetime_conversion(self, df: pd.DataFrame, time_column: str = "TimeString") -> Dict:
        pass

class IMigrationService(ABC):
    @abstractmethod
    def get_tables_with_timestring(self) -> List[str]:
        pass
    
    @abstractmethod
    def get_column_type(self, table_name: str, column_name: str = "TimeString") -> str:
        pass
    
    @abstractmethod
    def migrate_timestring_to_datetime(self) -> Dict[str, bool]:
        pass
    
    @abstractmethod
    def create_indexes_on_timestring(self) -> Dict[str, bool]:
        pass
    
    @abstractmethod
    def ensure_tables_ordered(self) -> Dict[str, bool]:
        pass