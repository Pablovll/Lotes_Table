# services/format_preservation_service.py
import pandas as pd
from datetime import datetime
import re

class FormatPreservationService:
    @staticmethod
    def ensure_timestring_format(df: pd.DataFrame, time_column: str = "TimeString") -> pd.DataFrame:
        """
        Ensure TimeString column is always in DD/MM/YYYY HH:MM:SS format
        """
        if time_column not in df.columns:
            return df
        
        df = df.copy()
        
        # Handle different input formats
        for i, value in enumerate(df[time_column]):
            if pd.isna(value) or value == "":
                continue
                
            # If already in correct format, keep it
            if isinstance(value, str) and re.match(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}', value):
                continue
                
            # If it's a datetime object, format it
            if isinstance(value, (datetime, pd.Timestamp)):
                df[time_column].iloc[i] = value.strftime('%d/%m/%Y %H:%M:%S')
            else:
                # Try to parse and reformat
                try:
                    parsed = pd.to_datetime(value, dayfirst=True, errors='coerce')
                    if not pd.isna(parsed):
                        df[time_column].iloc[i] = parsed.strftime('%d/%m/%Y %H:%M:%S')
                except:
                    pass
        
        return df
    