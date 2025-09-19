# services/timestring_recovery_service.py
from unittest import result
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re

class TimeStringRecoveryService:
    def __init__(self):
        self.common_date_patterns = [
            '%d/%m/%Y %H:%M:%S',    # 15/01/2024 10:30:45
            '%Y-%m-%d %H:%M:%S',    # 2024-01-15 10:30:45
            '%d-%m-%Y %H:%M:%S',    # 15-01-2024 10:30:45
            '%m/%d/%Y %H:%M:%S',    # 01/15/2024 10:30:45 (US format)
            '%Y/%m/%d %H:%M:%S',    # 2024/01/15 10:30:45
        ]
    
    def analyze_timestring_quality(self, df: pd.DataFrame, time_column: str = "TimeString") -> Dict:
        """
        Comprehensive analysis of TimeString column quality
        """
        if time_column not in df.columns:
            return {"error": f"Column '{time_column}' not found"}
        
        analysis = {
            "total_rows": len(df),
            "valid_count": 0,
            "invalid_count": 0,
            "null_count": 0,
            "invalid_examples": [],
            "date_range_valid": None,
            "common_issues": {},
            "data_loss_percentage": 0
        }
        
        valid_timestamps = []
        
        for idx, value in enumerate(df[time_column]):
            if self._is_null_value(value):
                analysis["null_count"] += 1
                analysis["invalid_examples"].append({
                    "index": idx, "value": value, "issue": "null_value"
                })
                continue
                
            parsed, issue = self._parse_timestamp(value)
            if parsed is not None and not pd.isna(parsed):
                analysis["valid_count"] += 1
                valid_timestamps.append(parsed)
            else:
                analysis["invalid_count"] += 1
                analysis["invalid_examples"].append({
                    "index": idx, "value": value, "issue": issue
                })
        
        # Calculate statistics
        if valid_timestamps:
            analysis["date_range_valid"] = {
                "min": min(valid_timestamps),
                "max": max(valid_timestamps)
            }
        
        analysis["data_loss_percentage"] = (
            (analysis["invalid_count"] + analysis["null_count"]) / 
            analysis["total_rows"] * 100
        )
        
        # Analyze patterns
        analysis["common_issues"] = self._analyze_issue_patterns(analysis["invalid_examples"])
        
        return analysis
    
    def _is_null_value(self, value) -> bool:
        """Check if value is null-like"""
        if pd.isna(value):
            return True
        if isinstance(value, str) and value.strip().lower() in ['', 'null', 'none', 'nan', 'n/a', 'na']:
            return True
        return False
    
    def _parse_timestamp(self, value) -> Tuple[Optional[datetime], str]:
        """Try to parse timestamp with multiple strategies"""
        if pd.isna(value):
            return None, "null_value"
        
        # Try standard parsing first
        try:
            parsed = pd.to_datetime(value, dayfirst=True, errors='coerce')
            if not pd.isna(parsed):
                return parsed, "valid"
        except:
            pass
        
        # Try multiple date patterns
        for pattern in self.common_date_patterns:
            try:
                parsed = datetime.strptime(str(value), pattern)
                return parsed, "fixed_format"
            except:
                continue
        
        # Try to fix common issues
        fixed_value, fix_type = self._try_fix_common_issues(value)
        if fixed_value != value:
            try:
                parsed = pd.to_datetime(fixed_value, dayfirst=True, errors='coerce')
                if not pd.isna(parsed):
                    return parsed, f"fixed_{fix_type}"
            except:
                pass
        
        return None, "unparseable"
    
    def _try_fix_common_issues(self, value: str) -> Tuple[str, str]:
        """Try to fix common timestamp issues"""
        original = str(value).strip()
        
        # Fix time overflow (25:00 -> 01:00 next day)
        time_overflow_match = re.match(r'(\d{1,2}/\d{1,2}/\d{4}) (\d{2}):(\d{2}):(\d{2})', original)
        if time_overflow_match:
            date_part, hour, minute, second = time_overflow_match.groups()
            hour_int = int(hour)
            if hour_int >= 24:
                fixed_hour = hour_int % 24
                days_to_add = hour_int // 24
                try:
                    base_date = datetime.strptime(date_part, '%d/%m/%Y')
                    new_date = base_date + timedelta(days=days_to_add)
                    fixed_date = new_date.strftime('%d/%m/%Y')
                    return f"{fixed_date} {fixed_hour:02d}:{minute}:{second}", "time_overflow"
                except:
                    pass
        
        # Fix date overflow (32/01/2024 -> 31/01/2024)
        date_overflow_match = re.match(r'(\d{2})/(\d{2})/(\d{4})', original)
        if date_overflow_match:
            day, month, year = date_overflow_match.groups()
            day_int, month_int, year_int = int(day), int(month), int(year)
            
            # Check if day is invalid for month
            try:
                datetime(year_int, month_int, 1)  # Check if month is valid
                days_in_month = (datetime(year_int, month_int % 12 + 1, 1) - 
                               timedelta(days=1)).day
                if day_int > days_in_month:
                    fixed_day = days_in_month
                    return original.replace(day, str(fixed_day)), "date_overflow"
            except:
                pass
        
        return original, "unfixable"
    
    def _analyze_issue_patterns(self, invalid_examples: List[Dict]) -> Dict:
        """Analyze patterns in invalid timestamps"""
        patterns = {
            "null_values": 0,
            "wrong_format": 0,
            "time_overflow": 0,
            "date_overflow": 0,
            "text_values": 0,
            "unparseable": 0
        }
        
        for example in invalid_examples:
            issue = example.get("issue", "unparseable")
            if issue in patterns:
                patterns[issue] += 1
            else:
                patterns["unparseable"] += 1
        
        return patterns
    
    def recover_timestrings(self, df: pd.DataFrame, time_column: str = "TimeString", 
                        strategy: str = "auto", confidence_threshold: float = 0.7) -> Dict:
        """
        Recover invalid timestamps with detailed reporting
        """
        # Analyze first
        analysis = self.analyze_timestring_quality(df, time_column)
        
        if analysis["valid_count"] == 0:
            return {
                "success": False,
                "message": "No valid timestamps found for reference",
                "analysis": analysis
            }
        
        # Choose strategy automatically if needed
        if strategy == "auto":
            if analysis["data_loss_percentage"] < 5:
                strategy = "interpolate"
            elif analysis["data_loss_percentage"] < 20:
                strategy = "reconstruct"
            else:
                strategy = "pattern"
        
        # Create working copy
        df_recovered = df.copy()
        recovery_report = {
            "strategy_used": strategy,
            "total_recovered": 0,
            "recovery_details": [],
            "confidence_scores": []
        }
        
        # Convert to datetime for processing
        datetime_series = pd.to_datetime(df_recovered[time_column], dayfirst=True, errors='coerce')
        valid_mask = datetime_series.notna()
        
        if strategy == "interpolate":
            # Simple interpolation between valid values
            datetime_series = datetime_series.interpolate(method='time')
            recovered_mask = datetime_series.notna() & ~valid_mask
            df_recovered[time_column] = datetime_series.dt.strftime('%d/%m/%Y %H:%M:%S')
            
            recovery_report["total_recovered"] = recovered_mask.sum()
            recovery_report["average_confidence"] = 0.85
            
        elif strategy in ["reconstruct", "pattern"]:
            # More sophisticated reconstruction
            valid_indices = np.where(valid_mask)[0]
            valid_times = datetime_series[valid_mask].values
            
            for i in range(len(df_recovered)):
                if not valid_mask.iloc[i]:
                    confidence, estimated_time = self._estimate_timestamp(
                        i, valid_indices, valid_times, strategy, df_recovered, time_column
                    )
                    
                    if confidence >= confidence_threshold:
                        df_recovered[time_column].iloc[i] = estimated_time.strftime('%d/%m/%Y %H:%M:%S')
                        recovery_report["total_recovered"] += 1
                        recovery_report["recovery_details"].append({
                            "index": i,
                            "original_value": str(df[time_column].iloc[i]),
                            "recovered_value": estimated_time.strftime('%d/%m/%Y %H:%M:%S'),
                            "confidence": confidence
                        })
                        recovery_report["confidence_scores"].append(confidence)
            
            if recovery_report["confidence_scores"]:
                recovery_report["average_confidence"] = sum(recovery_report["confidence_scores"]) / len(recovery_report["confidence_scores"])
            else:
                recovery_report["average_confidence"] = 0
        
    # ⭐⭐⭐ REMOVE MILLISECONDS FROM TIMESTRING COLUMN ITSELF ⭐⭐⭐
        # Ensure proper datetime type
        if not pd.api.types.is_datetime64_any_dtype(df_recovered[time_column]):
            # Convert string timestamps to datetime
            df_recovered[time_column] = pd.to_datetime(df_recovered[time_column], dayfirst=True, errors='coerce')
        
        # Remove milliseconds by converting to string and back to datetime
        df_recovered[time_column] = df_recovered[time_column].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_recovered[time_column] = pd.to_datetime(df_recovered[time_column], errors='coerce')
        
        return {
            "success": True,
            "recovered_df": df_recovered,
            "analysis": analysis,
            "recovery_report": recovery_report
        }

    def _estimate_timestamp(self, index: int, valid_indices: np.array, valid_times: np.array,
                          strategy: str, df: pd.DataFrame, time_column: str) -> Tuple[float, datetime]:
        """Estimate a missing timestamp"""
        # Find nearest valid timestamps
        prev_valid_idx = valid_indices[valid_indices < index]
        next_valid_idx = valid_indices[valid_indices > index]
        
        has_prev = len(prev_valid_idx) > 0
        has_next = len(next_valid_idx) > 0
        
        if has_prev and has_next:
            # Between two valid points - use interpolation
            prev_idx = prev_valid_idx[-1]
            next_idx = next_valid_idx[0]
            
            prev_time = pd.to_datetime(df[time_column].iloc[prev_idx], dayfirst=True)
            next_time = pd.to_datetime(df[time_column].iloc[next_idx], dayfirst=True)
            
            # Calculate position between points
            position = (index - prev_idx) / (next_idx - prev_idx)
            time_diff = next_time - prev_time
            estimated_time = prev_time + time_diff * position
            
            confidence = 0.9 - (0.2 * (next_idx - prev_idx - 1) / len(df))  # Confidence decreases with gap size
            
            return confidence, estimated_time
            
        elif has_prev:
            # Only previous point available - use pattern if possible
            prev_idx = prev_valid_idx[-1]
            prev_time = pd.to_datetime(df[time_column].iloc[prev_idx], dayfirst=True)
            
            if strategy == "pattern" and len(prev_valid_idx) > 1:
                # Calculate average interval from recent points
                recent_intervals = []
                for i in range(1, min(4, len(prev_valid_idx))):
                    idx1, idx2 = prev_valid_idx[-i-1], prev_valid_idx[-i]
                    time1 = pd.to_datetime(df[time_column].iloc[idx1], dayfirst=True)
                    time2 = pd.to_datetime(df[time_column].iloc[idx2], dayfirst=True)
                    recent_intervals.append((time2 - time1) / (idx2 - idx1))
                
                if recent_intervals:
                    avg_interval = sum(recent_intervals, timedelta(0)) / len(recent_intervals)
                    estimated_time = prev_time + avg_interval * (index - prev_idx)
                    confidence = 0.7
                    return confidence, estimated_time
            
            # Fallback: assume regular interval
            estimated_time = prev_time + timedelta(minutes=1) * (index - prev_idx)
            return 0.6, estimated_time
            
        elif has_next:
            # Only next point available
            next_idx = next_valid_idx[0]
            next_time = pd.to_datetime(df[time_column].iloc[next_idx], dayfirst=True)
            estimated_time = next_time - timedelta(minutes=1) * (next_idx - index)
            return 0.6, estimated_time
        
        else:
            # No reference points - use overall pattern (shouldn't happen with validation)
            return 0.3, datetime.now()