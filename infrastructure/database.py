# infrastructure/database.py
from sqlalchemy import create_engine, text, DateTime
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List
import pandas as pd
from core.models import AuthenticationType
from core.interfaces import IDatabaseConnection

class DatabaseConnection(IDatabaseConnection):
    def __init__(self):
        self.engine = None
        self.connection_string = None
    
    def connect(self, config) -> bool:
        try:
            if config.authentication_type == AuthenticationType.WINDOWS:
                self.connection_string = (
                    f"mssql+pyodbc://{config.host}:{config.port}/{config.database_name}"
                    f"?driver=ODBC+Driver+17+for+SQL+Server"
                    f"&trusted_connection=yes"
                )
            else:
                self.connection_string = (
                    f"mssql+pyodbc://{config.username}:{config.password}"
                    f"@{config.host}:{config.port}/{config.database_name}"
                    f"?driver=ODBC+Driver+17+for+SQL+Server"
                )
            
            self.engine = create_engine(self.connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as e:
            print(f"Connection error: {e}")
            return False
    
    def get_tables(self) -> List[str]:
        if not self.engine:
            return []
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_NAME NOT LIKE 'sys%'
                    AND TABLE_NAME NOT LIKE 'MS%'
                    ORDER BY TABLE_NAME
                """))
                return [row[0] for row in result]
        except SQLAlchemyError as e:
            print(f"Error fetching tables: {e}")
            return []
    
    def get_tables_with_timestring(self) -> List[str]:
        """Get only tables that have a TimeString column (faster method)"""
        if not self.engine:
            return []
        
        try:
            with self.engine.connect() as conn:
                # Query to find tables with TimeString column
                result = conn.execute(text("""
                    SELECT t.TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES t
                    INNER JOIN INFORMATION_SCHEMA.COLUMNS c 
                        ON t.TABLE_NAME = c.TABLE_NAME 
                        AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
                    WHERE t.TABLE_TYPE = 'BASE TABLE'
                    AND c.COLUMN_NAME = 'TimeString'
                    AND t.TABLE_NAME NOT LIKE 'sys%'
                    AND t.TABLE_NAME NOT LIKE 'MS%'
                    ORDER BY t.TABLE_NAME
                """))
                return [row[0] for row in result]
        except SQLAlchemyError as e:
            print(f"Error fetching tables with TimeString: {e}")
            return []
    def read_table(self, table_name: str) -> Optional[pd.DataFrame]:
        if not self.engine:
            return None
        
        try:
            # Simple query - we'll handle milliseconds in Python
            query = text(f"SELECT * FROM [{table_name}]")
            df = pd.read_sql(query, self.engine)
            
            # Ensure TimeString is properly handled
            if 'TimeString' in df.columns:
                # Convert to datetime if it's not already
                if not pd.api.types.is_datetime64_any_dtype(df['TimeString']):
                    df['TimeString'] = pd.to_datetime(df['TimeString'], dayfirst=True, errors='coerce')
                
                # Remove milliseconds from TimeString column itself
                df['TimeString'] = df['TimeString'].dt.strftime('%Y-%m-%d %H:%M:%S')
                df['TimeString'] = pd.to_datetime(df['TimeString'], errors='coerce')
            
            return df
            
        except SQLAlchemyError as e:
            print(f"Error reading table [{table_name}]: {e}")
            return None
    def create_lotedata_table(self, df: pd.DataFrame, table_name: str = 'LOTEDATA') -> bool:
        if not self.engine:
            return False
        
        try:
            # Ensure TimeString is datetime type for Power BI compatibility
            if 'TimeString' in df.columns:
                df = df.copy()
                # Convert to datetime if it's not already
                if not pd.api.types.is_datetime64_any_dtype(df['TimeString']):
                    df['TimeString'] = pd.to_datetime(df['TimeString'], dayfirst=True, errors='coerce')
            
            # Save with explicit datetime type
            df.to_sql(
                table_name, 
                self.engine, 
                if_exists='replace', 
                index=False,
                dtype={'TimeString': DateTime()} if 'TimeString' in df.columns else {}
            )
            
            print(f"Successfully created table: {table_name} with datetime column")
            return True
        except SQLAlchemyError as e:
            print(f"Error creating {table_name} table: {e}")
            return False