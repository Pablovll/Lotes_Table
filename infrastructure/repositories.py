# infrastructure/repositories.py
from typing import List, Optional
import pandas as pd
from core.models import DatabaseConfig
from core.interfaces import IDatabaseRepository

class DatabaseRepository(IDatabaseRepository):
    def __init__(self, db_connection):
        self.db_connection = db_connection
    
    def get_available_tables(self) -> List[str]:
        return self.db_connection.get_tables()
    
    def get_tables_with_timestring(self) -> List[str]:
        """Get only tables that have a TimeString column"""
        return self.db_connection.get_tables_with_timestring()
    
    def fetch_table_data(self, table_name: str) -> Optional[pd.DataFrame]:
        return self.db_connection.read_table(table_name)
    
    def save_lotedata(self, df: pd.DataFrame, table_name: str = 'LOTEDATA') -> bool:
        return self.db_connection.create_lotedata_table(df, table_name)
    
    def check_table_has_timestring_fast(self, table_name: str) -> bool:
        """Fast check if table has TimeString column without loading data"""
        tables_with_timestring = self.get_tables_with_timestring()
        return table_name in tables_with_timestring