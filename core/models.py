from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum

class AuthenticationType(Enum):
    SQL_SERVER = "sql_server"
    WINDOWS = "windows"

@dataclass
class Cycle:
    cycle_id: int
    start_time: datetime
    end_time: datetime
    sample_count: int = 0  # Added field for sample count
    duration_minutes: float = 0.0  # Added field for duration

@dataclass
class TableResult:
    table_name: str
    cycles: List[Cycle]
    total_cycles: int
    time_matched: bool = False  # Added field for cross-table matching
    error_message: str = ""  # Added field for error information

@dataclass
class AnalysisConfig:
    time_threshold_minutes: int = 10
    expected_frequency_minutes: int = 5
    time_column: str = "TimeString"

@dataclass
class DatabaseConfig:
    host: str
    port: int = 1433
    username: str = ""
    password: str = ""
    database_name: str = ""
    authentication_type: AuthenticationType = AuthenticationType.SQL_SERVER
    save_credentials: bool = False