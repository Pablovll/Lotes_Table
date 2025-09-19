# main.py
import tkinter as tk
from app.welcome_window import WelcomeWindow
from app.main_window import MainWindow
from app.results_window import ResultsWindow
from infrastructure.db_service import DatabaseService
from infrastructure.database import DatabaseConnection
from infrastructure.repositories import DatabaseRepository
from infrastructure.table_service import TableService
from infrastructure.migration_service_fixed import MigrationService
from services.analysis_service import AnalysisService
from core.models import DatabaseConfig, AnalysisConfig

class ProductionCycleAnalyzerApp:
    def __init__(self):
        # Initialize infrastructure components
        db_connection = DatabaseConnection()
        repository = DatabaseRepository(db_connection)
        table_service = TableService()
        migration_service = MigrationService(db_connection)
        
        # Initialize services with dependency injection
        self.db_service = DatabaseService(db_connection, repository, table_service, migration_service)
        self.analysis_service = None
        self.current_window = None
        
    def run(self):
        self.show_welcome_window()
    
    def show_welcome_window(self):
        welcome = WelcomeWindow(self.on_database_connect)
        welcome.show()
    
    def on_database_connect(self, db_config: DatabaseConfig, analysis_config: AnalysisConfig, should_migrate: bool) -> bool:
        success = self.db_service.connect_to_database(db_config)
        if success:
            # Perform migration if requested
            if should_migrate:
                migration_results = self.db_service.migrate_database_schema()
                # Show migration results
                successful_migrations = sum(1 for result in migration_results.get('migration', {}).values() if result)
                total_tables = len(migration_results.get('migration', {}))
                
                if successful_migrations > 0:
                    print(f"✅ Successfully migrated {successful_migrations}/{total_tables} tables")
                else:
                    print("⚠️ No migrations were performed")
            
            # Initialize analysis service
            self.analysis_service = AnalysisService(analysis_config)
            self.show_main_window()
        return success
    
    def show_main_window(self):
        main = MainWindow(self.db_service, self.on_analyze_tables)
        main.show()
    
    def on_analyze_tables(self, table_data):
        # Ask user for recovery strategy
        print("\n🔄 Choose TimeString recovery strategy:")
        print("1. Auto (recommended) - Let system choose best approach")
        print("2. Interpolate - Simple time-based interpolation")
        print("3. Reconstruct - Smart pattern-based reconstruction")
        print("4. Pattern - Advanced pattern detection")
        print("5. None - Skip recovery (original behavior)")
        
        choice = input("Enter choice (1-5, default=1): ").strip()
        strategies = {"1": "auto", "2": "interpolate", "3": "reconstruct", "4": "pattern", "5": "none"}
        strategy = strategies.get(choice, "auto")
        
        if strategy == "none":
            print("⏭️ Skipping data recovery...")
            analysis_results = self.analysis_service.analyze_tables(table_data, "none")
        else:
            print(f"🔧 Using {strategy} recovery strategy...")
            analysis_results = self.analysis_service.analyze_tables(table_data, strategy)
        
        self.show_results_window(analysis_results)
    
    def show_results_window(self, analysis_results):
        results = ResultsWindow(self.analysis_service, self.db_service, self.on_complete)
        results.show(analysis_results)
    
    def on_complete(self):
        print("Analysis completed successfully!")
        # Option to restart or exit
        restart = input("Would you like to restart? (y/n): ")
        if restart.lower() == 'y':
            self.run()

if __name__ == "__main__":
    app = ProductionCycleAnalyzerApp()
    app.run()