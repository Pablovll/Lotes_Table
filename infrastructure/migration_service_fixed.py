# infrastructure/migration_service_fixed.py
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict
import pandas as pd
from core.interfaces import IMigrationService

class MigrationService(IMigrationService):
    def __init__(self, db_connection):
        self.db_connection = db_connection
    
    def get_tables_with_timestring(self) -> List[str]:
        """Get all tables that have a TimeString column"""
        try:
            with self.db_connection.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT DISTINCT t.TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES t
                    INNER JOIN INFORMATION_SCHEMA.COLUMNS c 
                    ON t.TABLE_NAME = c.TABLE_NAME 
                    AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
                    WHERE c.COLUMN_NAME = 'TimeString'
                    AND t.TABLE_TYPE = 'BASE TABLE'
                    AND t.TABLE_NAME NOT LIKE 'sys%'
                    AND t.TABLE_NAME NOT LIKE 'MS%'
                """))
                return [row[0] for row in result]
        except SQLAlchemyError as e:
            print(f"Error getting tables with TimeString: {e}")
            return []
    
    def get_column_type(self, table_name: str, column_name: str = "TimeString") -> str:
        """Get the current data type of a column"""
        try:
            with self.db_connection.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT DATA_TYPE 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{table_name}' 
                    AND COLUMN_NAME = '{column_name}'
                """))
                return result.scalar() or "unknown"
        except SQLAlchemyError as e:
            print(f"Error getting column type for {table_name}.{column_name}: {e}")
            return "unknown"
    
    def migrate_timestring_to_datetime(self) -> Dict[str, bool]:
        """Migrate all TimeString columns from char to datetime type"""
        results = {}
        tables = self.get_tables_with_timestring()
        
        for table_name in tables:
            try:
                current_type = self.get_column_type(table_name)
                print(f"Table: {table_name}, Current TimeString type: {current_type}")
                
                if current_type.lower() in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
                    # Use a single transaction for all operations
                    with self.db_connection.engine.begin() as conn:
                        # Step 1: Add a new datetime column
                        conn.execute(text(f"""
                            ALTER TABLE [{table_name}]
                            ADD TimeString_temp DATETIME
                        """))
                        
                        # Step 2: Convert and copy data
                        conn.execute(text(f"""
                            UPDATE [{table_name}]
                            SET TimeString_temp = TRY_CONVERT(DATETIME, TimeString, 103)
                            WHERE ISDATE(TimeString) = 1
                        """))
                        
                        # Step 3: Drop the old column
                        conn.execute(text(f"""
                            ALTER TABLE [{table_name}]
                            DROP COLUMN TimeString
                        """))
                        
                        # Step 4: Rename the new column
                        conn.execute(text(f"""
                            EXEC sp_rename '{table_name}.TimeString_temp', 'TimeString', 'COLUMN'
                        """))
                    
                    print(f"✅ Successfully migrated {table_name}.TimeString to DATETIME")
                    results[table_name] = True
                
                elif current_type.lower() == 'datetime':
                    print(f"✅ {table_name}.TimeString is already DATETIME")
                    results[table_name] = True
                else:
                    print(f"⚠️ {table_name}.TimeString has unexpected type: {current_type}")
                    results[table_name] = False
                    
            except SQLAlchemyError as e:
                print(f"❌ Error migrating {table_name}: {e}")
                # Check if the temporary column exists and clean up if needed
                try:
                    with self.db_connection.engine.connect() as conn:
                        result = conn.execute(text(f"""
                            SELECT COUNT(*) 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_NAME = '{table_name}' 
                            AND COLUMN_NAME = 'TimeString_temp'
                        """))
                        if result.scalar() > 0:
                            conn.execute(text(f"""
                                ALTER TABLE [{table_name}]
                                DROP COLUMN TimeString_temp
                            """))
                            print(f"Cleaned up temporary column in {table_name}")
                except:
                    pass
                
                results[table_name] = False
        
        return results
    
    def create_indexes_on_timestring(self) -> Dict[str, bool]:
        """Create indexes on TimeString columns for better performance"""
        results = {}
        tables = self.get_tables_with_timestring()
        
        for table_name in tables:
            try:
                # Check if index already exists
                with self.db_connection.engine.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) 
                        FROM sys.indexes 
                        WHERE object_id = OBJECT_ID('{table_name}') 
                        AND name = 'IX_{table_name}_TimeString'
                    """))
                    if result.scalar() == 0:
                        conn.execute(text(f"""
                            CREATE INDEX IX_{table_name}_TimeString 
                            ON [{table_name}] (TimeString)
                        """))
                        print(f"✅ Created index on {table_name}.TimeString")
                    else:
                        print(f"✅ Index already exists on {table_name}.TimeString")
                results[table_name] = True
            except SQLAlchemyError as e:
                print(f"❌ Error creating index on {table_name}: {e}")
                results[table_name] = False
        
        return results
    
    def ensure_tables_ordered(self) -> Dict[str, bool]:
        """Ensure all tables have clustered index on TimeString for physical ordering"""
        results = {}
        tables = self.get_tables_with_timestring()
        
        for table_name in tables:
            try:
                # Check if table already has a clustered index
                with self.db_connection.engine.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) 
                        FROM sys.indexes 
                        WHERE object_id = OBJECT_ID('{table_name}') 
                        AND is_primary_key = 1
                    """))
                    has_primary_key = result.scalar() > 0
                
                if not has_primary_key:
                    # Check if clustered index already exists
                    with self.db_connection.engine.connect() as conn:
                        result = conn.execute(text(f"""
                            SELECT COUNT(*) 
                            FROM sys.indexes 
                            WHERE object_id = OBJECT_ID('{table_name}') 
                            AND name = 'IX_{table_name}_TimeString_Clustered'
                        """))
                        if result.scalar() == 0:
                            # Create clustered index for physical ordering
                            conn.execute(text(f"""
                                CREATE CLUSTERED INDEX IX_{table_name}_TimeString_Clustered 
                                ON [{table_name}] (TimeString)
                            """))
                            print(f"✅ Created clustered index on {table_name}.TimeString")
                        else:
                            print(f"✅ Clustered index already exists on {table_name}.TimeString")
                else:
                    print(f"⚠️ {table_name} has primary key, skipping clustered index")
                
                results[table_name] = True
                
            except SQLAlchemyError as e:
                print(f"❌ Error ensuring ordering for {table_name}: {e}")
                results[table_name] = False
        
        return results