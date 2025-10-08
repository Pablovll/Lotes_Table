import pandas as pd
from sqlalchemy import text
from datetime import datetime
from typing import List, Optional

class FactSamplesService:
    def __init__(self, db_service, source_tables: Optional[List[str]] = None):
        """
        Initialize with your existing DatabaseService
        If source_tables is None, automatically discover ALL tables with TimeString
        """
        self.db_service = db_service
        self.excluded_tables = {
           # "LOTE_SUMMARY",
            "FactSamples",
            "Cycle_Events"   # 🚫 Explicitly exclude cycle events table
        }

        # Use provided list or auto-discover
        self.source_tables = source_tables or self._get_all_tables_with_timestring()

        self.lote_table = "LOTE_DATA"
        self.summary_table = "LOTE_SUMMARY"
        print(f"📊 Found {len(self.source_tables)} tables for FactSamples ETL")

    def _get_all_tables_with_timestring(self) -> List[str]:
        """Get ALL tables that have a TimeString column, excluding cycle + warehouse tables"""
        tables = self.db_service.get_tables_with_timestring()
        return [t for t in tables if t not in self.excluded_tables]

    def create_schema(self) -> bool:
        """Create FactSamples table with proper SQL Server syntax"""
        try:
            with self.db_service.connection.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = 'FactSamples'
                """))
                if result.scalar() == 0:
                    conn.execute(text("""
                        CREATE TABLE FactSamples (
                            SampleID BIGINT IDENTITY(1,1) PRIMARY KEY,
                            TimeString DATETIME NOT NULL,
                            VariableName NVARCHAR(100) NOT NULL,
                            Value FLOAT,
                            CycleID BIGINT,
                            MachineName NVARCHAR(100),
                            PartitionKey AS (CONVERT(VARCHAR(7), TimeString, 120)) PERSISTED
                        )
                    """))
                    conn.execute(text("""
                        CREATE VIEW vw_FactSamples_Partitioned
                        WITH SCHEMABINDING
                        AS
                        SELECT SampleID, TimeString, VariableName, Value, CycleID, MachineName, PartitionKey
                        FROM dbo.FactSamples
                    """))
                    print("✅ Created FactSamples table and partitioned view")
                else:
                    print("✅ FactSamples table already exists")
                return True
        except Exception as e:
            print(f"❌ Error creating schema: {e}")
            return False

    def create_indexes(self) -> bool:
        """Create necessary indexes for performance"""
        try:
            with self.db_service.connection.engine.connect() as conn:
                indexes = [
                    "CREATE INDEX IX_FactSamples_TimeString ON FactSamples(TimeString)",
                    "CREATE INDEX IX_FactSamples_CycleID ON FactSamples(CycleID)",
                    "CREATE INDEX IX_FactSamples_VariableName ON FactSamples(VariableName)",
                    "CREATE INDEX IX_FactSamples_PartitionKey ON FactSamples(PartitionKey)",
                    "CREATE INDEX IX_FactSamples_MachineName ON FactSamples(MachineName)"
                ]
                for index_sql in indexes:
                    try:
                        conn.execute(text(index_sql))
                        print(f"✅ Created index: {index_sql[:50]}...")
                    except Exception as index_error:
                        print(f"⚠️ Index may already exist: {index_error}")
                return True
        except Exception as e:
            print(f"❌ Error creating indexes: {e}")
            return False

    def load_cycles(self) -> tuple:
        """Load cycle mapping tables"""
        try:
            lote_df = self.db_service.fetch_table_data(self.lote_table)
            if lote_df is not None and 'TimeString' in lote_df.columns and 'CycleID' in lote_df.columns:
                lote_df = lote_df[['TimeString', 'CycleID']].copy()
                lote_df['TimeString'] = pd.to_datetime(lote_df['TimeString'], errors='coerce')
                lote_df = lote_df.dropna(subset=['TimeString'])
            else:
                lote_df = pd.DataFrame(columns=['TimeString', 'CycleID'])

            summary_df = self.db_service.fetch_table_data(self.summary_table)
            if summary_df is not None and all(col in summary_df.columns for col in ['CycleID', 'StartTime', 'EndTime']):
                summary_df = summary_df[['CycleID', 'StartTime', 'EndTime']].copy()
                summary_df['StartTime'] = pd.to_datetime(summary_df['StartTime'], errors='coerce')
                summary_df['EndTime'] = pd.to_datetime(summary_df['EndTime'], errors='coerce')
                summary_df = summary_df.dropna(subset=['StartTime', 'EndTime'])
            else:
                summary_df = pd.DataFrame(columns=['CycleID', 'StartTime', 'EndTime'])

            return lote_df, summary_df
        except Exception as e:
            print(f"❌ Error loading cycle data: {e}")
            return pd.DataFrame(), pd.DataFrame()

    def process_table(self, table_name: str, lote_df: pd.DataFrame, summary_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Process a single table into fact samples format"""
        try:
            print(f"📊 Processing {table_name}...")
            df = self.db_service.fetch_table_data(table_name)
            if df is None or df.empty or 'TimeString' not in df.columns:
                print(f"⚠️  Skipping {table_name} - no data or missing TimeString")
                return None

            result_df = df[['TimeString']].copy()
            result_df['TimeString'] = pd.to_datetime(result_df['TimeString'], errors='coerce')
            result_df = result_df.dropna(subset=['TimeString'])

            if result_df.empty:
                return None

            value_col = next((col for col in ['VarValue', 'Value', 'VALOR', 'Medicion'] if col in df.columns), None)
            if not value_col:
                return None

            result_df['Value'] = df[value_col].astype(float)
            result_df['VariableName'] = table_name
            result_df['MachineName'] = table_name

            if not lote_df.empty:
                result_df = result_df.merge(lote_df, on='TimeString', how='left')
            else:
                result_df['CycleID'] = None

            missing_cycles = result_df[result_df['CycleID'].isna()]
            if not missing_cycles.empty and not summary_df.empty:
                summary_sorted = summary_df.sort_values('StartTime')
                missing_sorted = missing_cycles.sort_values('TimeString')
                merged = pd.merge_asof(
                    missing_sorted, summary_sorted,
                    left_on='TimeString', right_on='StartTime',
                    direction='forward'
                )
                valid_cycles = merged[
                    (merged['TimeString'] >= merged['StartTime']) &
                    (merged['TimeString'] <= merged['EndTime'])
                ]
                result_df.loc[valid_cycles.index, 'CycleID'] = valid_cycles['CycleID_y']

            return result_df[['TimeString', 'VariableName', 'Value', 'CycleID', 'MachineName']]
        except Exception as e:
            print(f"❌ Error processing {table_name}: {e}")
            return None

    def process_all_tables(self) -> pd.DataFrame:
        """Process all source tables and combine results"""
        print("🔄 Loading cycle mapping data...")
        lote_df, summary_df = self.load_cycles()

        all_results = []
        for table_name in self.source_tables:
            df = self.process_table(table_name, lote_df, summary_df)
            if df is not None and not df.empty:
                all_results.append(df)

        if all_results:
            return pd.concat(all_results, ignore_index=True)
        return pd.DataFrame()

    def save_to_db(self, fact_samples: pd.DataFrame) -> bool:
        """Save processed data to FactSamples table"""
        if fact_samples.empty:
            return False
        try:
            fact_samples['TimeString'] = pd.to_datetime(fact_samples['TimeString']).dt.strftime('%Y-%m-%d %H:%M:%S')
            fact_samples['TimeString'] = pd.to_datetime(fact_samples['TimeString'])
            return self.db_service.save_lotedata(fact_samples, 'FactSamples')
        except Exception as e:
            print(f"❌ Error saving to database: {e}")
            return False

    def run(self) -> bool:
        """Complete ETL pipeline"""
        print("=" * 60)
        print("🚀 STARTING FactSamples ETL PROCESS")
        print("=" * 60)

        if not self.create_schema():
            return False
        self.create_indexes()

        fact_data = self.process_all_tables()
        if fact_data.empty:
            print("⚠️ No data processed, exiting")
            return False

        success = self.save_to_db(fact_data)
        print("=" * 60)
        print("🎉 FACT SAMPLES ETL COMPLETED SUCCESSFULLY!" if success else "❌ FACT SAMPLES ETL FAILED!")
        return success


# 👇 Top-level helper function (outside the class!)
def run_fact_samples_etl(db_service):
    """Convenience function to run the ETL process"""
    service = FactSamplesService(db_service)
    return service.run()
