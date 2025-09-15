import pandas as pd
from typing import Optional, Dict
from core.cycle_analyzer import CycleAnalyzer

class TableService:
    def __init__(self):
        self.analyzer = CycleAnalyzer(time_threshold_minutes=10, expected_frequency_minutes=5)
    
    def convert_timestring_to_datetime(self, df: pd.DataFrame, time_column: str = "TimeString") -> Optional[pd.DataFrame]:
        """
        Convert TimeString column from varchar to datetime for Power BI compatibility
        """
        try:
            if df is None or df.empty or time_column not in df.columns:
                return None
            
            df = df.copy()
            
            # Convert to datetime with dayfirst=True for dd/mm/yyyy format
            df[time_column] = pd.to_datetime(df[time_column], dayfirst=True, errors='coerce')
            
            # Remove rows with invalid dates
            valid_mask = df[time_column].notna()
            df = df[valid_mask].copy()
            
            return df
            
        except Exception as e:
            print(f"Error converting {time_column} to datetime: {e}")
            return None
    
    def sort_table_by_timestring(self, df: pd.DataFrame, time_column: str = "TimeString") -> Optional[pd.DataFrame]:
        """
        Sort DataFrame by TimeString column in ascending order with datetime conversion
        """
        try:
            if df is None or df.empty or time_column not in df.columns:
                return None
            
            # First convert to datetime
            df = self.convert_timestring_to_datetime(df, time_column)
            if df is None:
                return None
            
            # Sort by the datetime column
            df = df.sort_values(time_column, ascending=True)
            
            # Reset index after sorting
            df = df.reset_index(drop=True)
            
            return df
            
        except Exception as e:
            print(f"Error sorting table by {time_column}: {e}")
            return None
    
    def sort_multiple_tables(self, table_data: Dict[str, pd.DataFrame], time_column: str = "TimeString") -> Dict[str, pd.DataFrame]:
        """
        Sort multiple tables by TimeString column with datetime conversion
        """
        sorted_tables = {}
        
        for table_name, df in table_data.items():
            sorted_df = self.sort_table_by_timestring(df, time_column)
            if sorted_df is not None:
                sorted_tables[table_name] = sorted_df
                print(f"✅ Table '{table_name}' converted to datetime and sorted")
            else:
                print(f"❌ Failed to process table '{table_name}'")
                # Keep original if processing fails
                sorted_tables[table_name] = df
        
        return sorted_tables
    
    def verify_datetime_conversion(self, df: pd.DataFrame, time_column: str = "TimeString") -> Dict:
        """
        Verify that datetime conversion was successful
        """
        result = {
            'success': False,
            'original_type': str(type(df[time_column].iloc[0])) if not df.empty and time_column in df.columns else 'N/A',
            'converted_type': 'N/A',
            'valid_count': 0,
            'invalid_count': 0,
            'date_range': 'N/A'
        }
        
        if df is not None and not df.empty and time_column in df.columns:
            # Check if already datetime
            if pd.api.types.is_datetime64_any_dtype(df[time_column]):
                result['success'] = True
                result['converted_type'] = 'datetime64[ns]'
                result['valid_count'] = len(df)
                result['date_range'] = f"{df[time_column].min()} to {df[time_column].max()}"
            else:
                # Try conversion
                converted = pd.to_datetime(df[time_column], dayfirst=True, errors='coerce')
                valid_mask = converted.notna()
                
                result['valid_count'] = valid_mask.sum()
                result['invalid_count'] = (~valid_mask).sum()
                result['converted_type'] = str(converted.dtype)
                
                if result['valid_count'] > 0:
                    result['success'] = True
                    result['date_range'] = f"{converted[valid_mask].min()} to {converted[valid_mask].max()}"
        
        return result