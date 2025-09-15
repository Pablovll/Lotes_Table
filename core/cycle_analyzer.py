import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from core.models import Cycle, TableResult

class CycleAnalyzer:
    def __init__(self, time_threshold_minutes: int, expected_frequency_minutes: int):
        self.time_threshold = timedelta(minutes=time_threshold_minutes)
        self.expected_frequency = timedelta(minutes=expected_frequency_minutes)
    # --- Time Parsing ---
    def parse_time_string(self, time_series: pd.Series) -> pd.Series:
        """Parse TimeString into datetime with dd/mm/yyyy hh:mm:ss format"""
        try:
            return pd.to_datetime(time_series, dayfirst=True, errors='coerce')
        except Exception:
            def parse_custom_date(date_str):
                try:
                    return datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')
                except Exception:
                    return pd.NaT
            return time_series.apply(parse_custom_date)

    # --- Core Cycle Detection ---
    def detect_cycles(self, time_series: pd.Series) -> List[Cycle]:
        if len(time_series) == 0:
            return []

        # Convert to datetime and clean invalid
        time_series = self.parse_time_string(time_series).dropna()

        if len(time_series) == 0:
            return []

        # Sort by datetime
        time_series = time_series.sort_values().reset_index(drop=True)

        cycles: List[Cycle] = []
        current_cycle_id = 1
        cycle_start = time_series.iloc[0]
        previous_time = time_series.iloc[0]
        sample_count = 1

        for i in range(1, len(time_series)):
            current_time = time_series.iloc[i]
            time_diff = current_time - previous_time
            sample_count += 1

            if time_diff > self.time_threshold:
                # Close current cycle
                duration = (previous_time - cycle_start).total_seconds() / 60
                cycles.append(
                    Cycle(
                        cycle_id=current_cycle_id,
                        start_time=cycle_start,
                        end_time=previous_time,
                        sample_count=sample_count - 1,  # Don't count the gap
                        duration_minutes=duration
                    )
                )
                # Start new cycle
                current_cycle_id += 1
                cycle_start = current_time
                sample_count = 1

            previous_time = current_time

        # Append last cycle
        duration = (previous_time - cycle_start).total_seconds() / 60
        cycles.append(
            Cycle(
                cycle_id=current_cycle_id,
                start_time=cycle_start,
                end_time=previous_time,
                sample_count=sample_count,
                duration_minutes=duration
            )
        )

        return cycles

    # --- Table Analyzer ---
    def analyze_table(self, table_name: str, df: pd.DataFrame, time_column: str = "TimeString") -> TableResult:
        try:
            if time_column not in df.columns:
                return TableResult(
                    table_name=table_name,
                    cycles=[],
                    total_cycles=0,
                    error_message=f"Column '{time_column}' not found"
                )

            if df.empty:
                return TableResult(
                    table_name=table_name,
                    cycles=[],
                    total_cycles=0,
                    error_message="Table is empty"
                )

            cycles = self.detect_cycles(df[time_column])

            return TableResult(
                table_name=table_name,
                cycles=cycles,
                total_cycles=len(cycles),
            )
        except Exception as e:
            return TableResult(
                table_name=table_name,
                cycles=[],
                total_cycles=0,
                error_message=f"Analysis error: {str(e)}"
            )
    # --- Enhanced Cross-Table Check with Debugging ---
    def check_time_matching(self, tables: Dict[str, pd.DataFrame], time_column: str = "TimeString") -> Tuple[bool, Dict]:
        """
        Check if all selected tables have the same TimeString sequence.
        Returns: (is_matching, debug_info)
        """
        debug_info = {
            "tables_checked": [],
            "mismatch_reasons": [],
            "summary": {}
        }
        
        if not tables:
            debug_info["mismatch_reasons"].append("No tables provided")
            return False, debug_info

        parsed_series = {}
        table_names = list(tables.keys())
        
        # Parse and prepare all time series
        for table_name, df in tables.items():
            if time_column not in df.columns:
                reason = f"Table '{table_name}' missing '{time_column}' column"
                debug_info["mismatch_reasons"].append(reason)
                debug_info["tables_checked"].append({"table": table_name, "status": "error", "reason": reason})
                continue
            
            series = self.parse_time_string(df[time_column]).dropna()
            if series.empty:
                reason = f"Table '{table_name}' has no valid timestamps after parsing"
                debug_info["mismatch_reasons"].append(reason)
                debug_info["tables_checked"].append({"table": table_name, "status": "error", "reason": reason})
                continue
            
            parsed_series[table_name] = series.reset_index(drop=True)
            debug_info["tables_checked"].append({
                "table": table_name, 
                "status": "ok", 
                "count": len(series),
                "time_range": f"{series.min()} to {series.max()}"
            })
        
        if len(parsed_series) < 2:
            debug_info["mismatch_reasons"].append("Need at least 2 valid tables for comparison")
            return False, debug_info
        
        # Use the table with most data as reference
        reference_table = max(parsed_series.items(), key=lambda x: len(x[1]))[0]
        reference = parsed_series[reference_table]
        
        debug_info["reference_table"] = reference_table
        debug_info["reference_count"] = len(reference)
        debug_info["reference_range"] = f"{reference.min()} to {reference.max()}"
        
        all_matching = True
        
        for table_name, series in parsed_series.items():
            if table_name == reference_table:
                continue
                
            comparison = self._compare_time_series(reference, series, reference_table, table_name)
            
            if not comparison["matches"]:
                all_matching = False
                debug_info["mismatch_reasons"].extend(comparison["reasons"])
            
            debug_info["summary"][table_name] = comparison
        
        return all_matching, debug_info

    def _compare_time_series(self, ref_series: pd.Series, test_series: pd.Series,
                              ref_name: str, test_name: str) -> Dict:
        """Compare two time series and return detailed comparison results with row-level info"""
        result = {
            "matches": True,
            "reasons": [],
            "stats": {},
            "mismatch_details": [],
            "row_comparisons": []
        }
        
        min_length = min(len(ref_series), len(test_series))
        max_length = max(len(ref_series), len(test_series))
        
        # Check length difference
        if len(ref_series) != len(test_series):
            result["matches"] = False
            result["reasons"].append(
                f"Different row counts: {ref_name}({len(ref_series)}) vs {test_name}({len(test_series)})"
            )
            result["stats"]["length_difference"] = abs(len(ref_series) - len(test_series))
        
        # Compare row by row
        mismatch_count = 0
        time_diffs = []
        
        for i in range(min_length):
            ref_time = ref_series.iloc[i]
            test_time = test_series.iloc[i]
            
            if ref_time != test_time:
                mismatch_count += 1
                diff_seconds = abs((ref_time - test_time).total_seconds())
                time_diffs.append(diff_seconds)
                
                # Store detailed mismatch info
                mismatch_info = {
                    "row_index": i,
                    "reference_value": ref_time,
                    "test_value": test_time,
                    "difference_seconds": diff_seconds,
                    "reference_str": ref_time.strftime('%d/%m/%Y %H:%M:%S.%f')[:-3],
                    "test_str": test_time.strftime('%d/%m/%Y %H:%M:%S.%f')[:-3]
                }
                result["mismatch_details"].append(mismatch_info)
                
                # Store for detailed row comparison
                result["row_comparisons"].append({
                    "row": i + 1,  # 1-based indexing for readability
                    "reference": ref_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    "test": test_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    "diff_seconds": f"{diff_seconds:.6f}",
                    "match": "❌" if diff_seconds > 0.001 else "⚠️"  # 1ms tolerance
                })
        
        # Check for extra rows in either series
        if len(ref_series) > len(test_series):
            extra_rows = len(ref_series) - len(test_series)
            result["reasons"].append(f"{ref_name} has {extra_rows} extra rows at the end")
            result["stats"]["extra_rows_in_reference"] = extra_rows
            
        if len(test_series) > len(ref_series):
            extra_rows = len(test_series) - len(ref_series)
            result["reasons"].append(f"{test_name} has {extra_rows} extra rows at the end")
            result["stats"]["extra_rows_in_test"] = extra_rows
        
        # Add statistical information
        if time_diffs:
            result["matches"] = False
            result["stats"]["mismatch_count"] = mismatch_count
            result["stats"]["mismatch_percentage"] = (mismatch_count / min_length) * 100
            result["stats"]["avg_time_diff_seconds"] = sum(time_diffs) / len(time_diffs)
            result["stats"]["max_time_diff_seconds"] = max(time_diffs) if time_diffs else 0
            result["stats"]["min_time_diff_seconds"] = min(time_diffs) if time_diffs else 0
            
            result["reasons"].append(f"{mismatch_count} rows have timestamp mismatches")
            result["reasons"].append(f"Average time difference: {result['stats']['avg_time_diff_seconds']:.6f} seconds")
            result["reasons"].append(f"Maximum time difference: {result['stats']['max_time_diff_seconds']:.6f} seconds")
        
        # Check data types
        if ref_series.dtype != test_series.dtype:
            result["matches"] = False
            result["reasons"].append(
                f"Different dtypes: {ref_name}({ref_series.dtype}) vs {test_name}({test_series.dtype})"
            )
        
        # Check if differences are within acceptable tolerance (1 second)
        if time_diffs and max(time_diffs) <= 1.0:
            result["reasons"].append("All differences are within 1 second tolerance")
        
        return result

    # --- Enhanced analysis method to use the new matching check ---
    def analyze_table(self, table_name: str, df: pd.DataFrame, time_column: str = "TimeString") -> TableResult:
        try:
            if time_column not in df.columns:
                return TableResult(
                    table_name=table_name,
                    cycles=[],
                    total_cycles=0,
                    error_message=f"Column '{time_column}' not found"
                )

            if df.empty:
                return TableResult(
                    table_name=table_name,
                    cycles=[],
                    total_cycles=0,
                    error_message="Table is empty"
                )

            cycles = self.detect_cycles(df[time_column])

            return TableResult(
                table_name=table_name,
                cycles=cycles,
                total_cycles=len(cycles),
            )
        except Exception as e:
            return TableResult(
                table_name=table_name,
                cycles=[],
                total_cycles=0,
                error_message=f"Analysis error: {str(e)}"
            )