from core.cycle_analyzer import CycleAnalyzer
from core.models import TableResult, AnalysisConfig
import pandas as pd
from datetime import timedelta

class AnalysisService:
    def __init__(self, config: AnalysisConfig):
        self.config = config
        self.analyzer = CycleAnalyzer(
            time_threshold_minutes=self.config.time_threshold_minutes,
            expected_frequency_minutes=self.config.expected_frequency_minutes
        )
        self.analysis_results = {}
        self.time_matched = False
    
    def analyze_tables(self, table_data: dict) -> dict:
        self.analysis_results = {}
        
        # Enhanced time matching with debugging
        time_matched, debug_info = self.analyzer.check_time_matching(table_data, self.config.time_column)
        self.time_matched = time_matched
        
        # Print detailed debug information
        print("\n" + "="*80)
        print("TIME STRING MATCHING ANALYSIS - DETAILED REPORT")
        print("="*80)
        
        print(f"\nTables analyzed: {len(debug_info.get('tables_checked', []))}")
        for table_info in debug_info.get('tables_checked', []):
            status = "✅" if table_info['status'] == 'ok' else "❌"
            print(f"{status} {table_info['table']}: {table_info.get('reason', 'OK')}")
            if 'count' in table_info:
                print(f"   Rows: {table_info['count']}, Range: {table_info.get('time_range', 'N/A')}")
        
        if debug_info.get('reference_table'):
            print(f"\n📊 Reference table: {debug_info['reference_table']}")
            print(f"   Rows: {debug_info['reference_count']}")
            print(f"   Time range: {debug_info['reference_range']}")
        
        # Print detailed comparison results
        for table_name, comparison in debug_info.get('summary', {}).items():
            print(f"\n🔍 Comparing {debug_info['reference_table']} vs {table_name}:")
            
            if comparison['matches']:
                print("   ✅ Perfect match!")
            else:
                print("   ❌ Mismatches found:")
                for reason in comparison.get('reasons', []):
                    print(f"      - {reason}")
                
                # Print statistics
                stats = comparison.get('stats', {})
                if stats:
                    print(f"\n   📈 Statistics:")
                    for stat_name, stat_value in stats.items():
                        if isinstance(stat_value, float):
                            print(f"      {stat_name}: {stat_value:.6f}")
                        else:
                            print(f"      {stat_name}: {stat_value}")
                
                # Show first few mismatches with details
                mismatches = comparison.get('mismatch_details', [])
                if mismatches:
                    print(f"\n   🚫 First 10 row mismatches (showing {min(10, len(mismatches))} of {len(mismatches)}):")
                    for i, mismatch in enumerate(mismatches[:10]):
                        print(f"      Row {mismatch['row_index'] + 1}:")
                        print(f"        Reference: {mismatch['reference_str']}")
                        print(f"        Test:      {mismatch['test_str']}")
                        print(f"        Difference: {mismatch['difference_seconds']:.6f} seconds")
                        
                        # Show human-readable difference
                        if mismatch['difference_seconds'] > 60:
                            mins = mismatch['difference_seconds'] / 60
                            print(f"        ({mins:.1f} minutes)")
                        elif mismatch['difference_seconds'] > 1:
                            print(f"        ({mismatch['difference_seconds']:.1f} seconds)")
                        else:
                            ms = mismatch['difference_seconds'] * 1000
                            print(f"        ({ms:.1f} milliseconds)")
                        
                        if i < 9:  # Add separator except for last item
                            print("      " + "-" * 50)
        
        if debug_info.get('mismatch_reasons'):
            print(f"\n❌ OVERALL MISMATCH REASONS:")
            for reason in debug_info['mismatch_reasons']:
                print(f"   - {reason}")
        else:
            print(f"\n✅ All tables have matching time sequences!")
        
        print(f"\n🎯 Overall time matching: {'✅ YES' if time_matched else '❌ NO'}")
        print("="*80 + "\n")
        
        # Continue with individual table analysis
        for table_name, df in table_data.items():
            try:
                result = self.analyzer.analyze_table(table_name, df, self.config.time_column)
                result.time_matched = time_matched
                self.analysis_results[table_name] = result
                
                if result.error_message:
                    print(f"Warning analyzing {table_name}: {result.error_message}")
                else:
                    print(f"Analyzed {table_name}: {result.total_cycles} cycles found")
                    
            except Exception as e:
                error_result = TableResult(
                    table_name=table_name,
                    cycles=[],
                    total_cycles=0,
                    error_message=f"Unexpected error: {str(e)}"
                )
                self.analysis_results[table_name] = error_result
                print(f"Error analyzing table {table_name}: {e}")
        
        return self.analysis_results
    
    def generate_lotedata_summary(self, reference_table_name: str) -> pd.DataFrame:
        """
        Generate LOTEDATA table with cycle summary information
        Columns: CycleID, StartTime, EndTime, TotalTime, SamplesCount
        """
        if reference_table_name not in self.analysis_results:
            raise ValueError("Reference table not analyzed")
        
        result = self.analysis_results[reference_table_name]
        
        if result.error_message:
            raise ValueError(f"Cannot generate LOTEDATA: {result.error_message}")
        
        if not result.cycles:
            raise ValueError("No cycles found in reference table")
        
        # Create summary DataFrame
        summary_data = []
        for cycle in result.cycles:
            # Calculate total time in hh:mm:ss format
            total_seconds = int(cycle.duration_minutes * 60)
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            total_time = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            
            summary_data.append({
                'CycleID': cycle.cycle_id,
                'StartTime': cycle.start_time.strftime('%d/%m/%Y %H:%M:%S'),
                'EndTime': cycle.end_time.strftime('%d/%m/%Y %H:%M:%S'),
                'TotalTime': total_time,
                'SamplesCount': cycle.sample_count
            })
        
        return pd.DataFrame(summary_data)
    
    def generate_lotedata_detailed(self, reference_table_name: str, reference_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate detailed LOTEDATA table with TimeString and CycleID mapping
        """
        if reference_table_name not in self.analysis_results:
            raise ValueError("Reference table not analyzed")
        
        result = self.analysis_results[reference_table_name]
        
        if result.error_message:
            raise ValueError(f"Cannot generate LOTEDATA: {result.error_message}")
        
        lotedata_df = reference_df.copy()
        
        # Add LoteID column
        lotedata_df['CycleID'] = 0
        
        # Parse TimeString to datetime for comparison
        time_series = self.analyzer.parse_time_string(lotedata_df[self.config.time_column])
        
        # Remove rows with invalid dates
        valid_mask = time_series.notna()
        time_series = time_series[valid_mask]
        lotedata_df = lotedata_df[valid_mask]
        
        # Sort by datetime
        sorted_indices = time_series.sort_values().index
        time_series = time_series.loc[sorted_indices]
        lotedata_df = lotedata_df.loc[sorted_indices]
        
        # Assign CycleID based on cycles
        for cycle in result.cycles:
            mask = (time_series >= cycle.start_time) & (time_series <= cycle.end_time)
            lotedata_df.loc[mask, 'CycleID'] = cycle.cycle_id
        
        return lotedata_df[[self.config.time_column, 'CycleID']]
    
    def get_analysis_summary(self) -> dict:
        """Get a summary of the analysis results"""
        summary = {
            "total_tables": len(self.analysis_results),
            "tables_with_errors": 0,
            "tables_with_cycles": 0,
            "total_cycles": 0,
            "time_matched": self.time_matched,
            "table_details": {}
        }
        
        for table_name, result in self.analysis_results.items():
            table_info = {
                "cycles_found": result.total_cycles,
                "has_error": bool(result.error_message),
                "error_message": result.error_message,
                "time_matched": result.time_matched
            }
            
            if result.error_message:
                summary["tables_with_errors"] += 1
            elif result.total_cycles > 0:
                summary["tables_with_cycles"] += 1
                summary["total_cycles"] += result.total_cycles
            
            summary["table_details"][table_name] = table_info
        
        return summary