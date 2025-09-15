from infrastructure.database import DatabaseConnection
from infrastructure.repositories import DatabaseRepository
from core.models import DatabaseConfig, AuthenticationType
from services.table_service import TableService
import pandas as pd

class DatabaseService:
    def __init__(self):
        self.connection = DatabaseConnection()
        self.repository = None
        self.table_service = TableService()
    
    def connect_to_database(self, config: DatabaseConfig) -> bool:
        success = self.connection.connect(config)
        
        if success:
            self.repository = DatabaseRepository(self.connection)
        
        return success
    
    def get_available_tables(self) -> list:
        if self.repository:
            return self.repository.get_available_tables()
        return []
    
    def get_tables_with_timestring(self) -> list:
        """Get tables that have TimeString column (fast method)"""
        if self.repository:
            return self.repository.get_tables_with_timestring()
        return []
    
    def fetch_table_data(self, table_name: str):
        if self.repository:
            return self.repository.fetch_table_data(table_name)
        return None
    
    def fetch_and_sort_table_data(self, table_name: str, time_column: str = "TimeString"):
        """
        Fetch table data, convert to datetime, and sort
        """
        df = self.fetch_table_data(table_name)
        if df is not None:
            # Verify conversion before and after
            print(f"🔍 Verifying datetime conversion for {table_name}:")
            before = self.table_service.verify_datetime_conversion(df, time_column)
            print(f"   Before: {before}")
            
            processed_df = self.table_service.sort_table_by_timestring(df, time_column)
            
            if processed_df is not None:
                after = self.table_service.verify_datetime_conversion(processed_df, time_column)
                print(f"   After: {after}")
            
            return processed_df
        return None
    
    def save_lotedata(self, df, table_name: str = 'LOTEDATA'):
        if self.repository:
            # Ensure TimeString is properly formatted for Power BI
            if 'TimeString' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['TimeString']):
                df = df.copy()
                df['TimeString'] = pd.to_datetime(df['TimeString'], dayfirst=True, errors='coerce')
            
            return self.repository.save_lotedata(df, table_name)
        return False
    
    def check_table_has_timestring(self, table_name: str) -> bool:
        """Check if a table has a TimeString column by loading data"""
        df = self.fetch_table_data(table_name)
        return df is not None and any(col.lower() == 'timestring' for col in df.columns)
    
    def check_table_has_timestring_fast(self, table_name: str) -> bool:
        """Fast check if table has TimeString column without loading data"""
        if self.repository:
            return self.repository.check_table_has_timestring_fast(table_name)
        return False