import tkinter as tk
from tkinter import ttk, messagebox
from core.models import DatabaseConfig, AuthenticationType, AnalysisConfig

class WelcomeWindow:
    def __init__(self, on_connect_callback):
        self.on_connect_callback = on_connect_callback
        self.window = None
        # Remove variable initialization from here
        
    def show(self):
        self.window = tk.Tk()
        self.window.title("Production Cycle Analyzer - Database Connection")
        self.window.geometry("500x500")
        
        # Initialize Tkinter variables AFTER creating the root window
        self.save_credentials_var = tk.BooleanVar(value=False)
        self.auth_type_var = tk.StringVar(value=AuthenticationType.SQL_SERVER.value)
        self.time_threshold_var = tk.StringVar(value="15")  # DEFAULT TO 15 MINUTES
        self.expected_frequency_var = tk.StringVar(value="5")  # DEFAULT TO 5 MINUTES
        self.migrate_var = tk.BooleanVar(value=True)  # Add migration option
        
        tk.Label(self.window, text="Database Connection", font=("Arial", 16)).pack(pady=10)
        
        # Analysis configuration frame
        config_frame = tk.LabelFrame(self.window, text="Analysis Configuration", padx=10, pady=10)
        config_frame.pack(pady=10, padx=20, fill='x')
        
        # Time threshold setting
        threshold_frame = tk.Frame(config_frame)
        threshold_frame.pack(pady=5, fill='x')
        
        tk.Label(threshold_frame, text="Cycle Threshold (minutes):", width=20, anchor='w').pack(side='left')
        threshold_entry = tk.Entry(threshold_frame, textvariable=self.time_threshold_var, width=10)
        threshold_entry.pack(side='right')
        
        # Expected frequency setting
        frequency_frame = tk.Frame(config_frame)
        frequency_frame.pack(pady=5, fill='x')
        
        tk.Label(frequency_frame, text="Expected Frequency (minutes):", width=20, anchor='w').pack(side='left')
        frequency_entry = tk.Entry(frequency_frame, textvariable=self.expected_frequency_var, width=10)
        frequency_entry.pack(side='right')
        
        # Migration checkbox
        migrate_frame = tk.Frame(self.window)
        migrate_frame.pack(pady=10, fill='x', padx=20)
        
        tk.Label(migrate_frame, text="Database Migration:", width=15, anchor='w').pack(side='left')
        tk.Checkbutton(migrate_frame, text="Convert TimeString to DATETIME", 
                      variable=self.migrate_var, font=("Arial", 10)).pack(side='left')
        tk.Label(migrate_frame, text="(Required for proper analysis)", 
                fg="gray", font=("Arial", 8)).pack(side='left', padx=5)
        
        # Authentication type selection
        auth_frame = tk.Frame(self.window)
        auth_frame.pack(pady=5, fill='x', padx=20)
        
        tk.Label(auth_frame, text="Authentication:", width=15, anchor='w').pack(side='left')
        auth_combo = ttk.Combobox(auth_frame, textvariable=self.auth_type_var, 
                                 values=[auth.value for auth in AuthenticationType], state="readonly")
        auth_combo.pack(side='right', fill='x', expand=True)
        auth_combo.bind('<<ComboboxSelected>>', self.on_auth_type_changed)
        
        # Input fields
        fields = [
            ("Server/Host", "DESKTOP-L16QENB"),
            ("Port", "1433"),
            ("Username", "sa"),
            ("Password", "", True),  # Password field
            ("Database Name", "BIOP_AGOS_2025")
        ]
        
        self.entries = {}
        for i, (label, default, *is_password) in enumerate(fields):
            frame = tk.Frame(self.window)
            frame.pack(pady=5, fill='x', padx=20)
            
            tk.Label(frame, text=label, width=15, anchor='w').pack(side='left')
            entry = tk.Entry(frame, show="*" if is_password else "")
            entry.insert(0, default)
            entry.pack(side='right', fill='x', expand=True)
            self.entries[label.lower().replace(" ", "_").replace("/", "_")] = entry
        
        # Initially disable username/password for Windows auth
        self.toggle_auth_fields()
        
        # Save credentials checkbox
        tk.Checkbutton(self.window, text="Save credentials", 
                      variable=self.save_credentials_var).pack(pady=5)
        
        # Connect button
        tk.Button(self.window, text="Connect", command=self.connect, 
                 bg="green", fg="white", width=15).pack(pady=20)
        
        # Info label
        info_text = "For trusted connection (Windows Authentication):\n" \
                   "1. Select 'windows' authentication type\n" \
                   "2. Enter Server/Host and Database Name only\n" \
                   "3. Leave Username/Password empty"
        tk.Label(self.window, text=info_text, justify=tk.LEFT, 
                font=("Arial", 9), fg="gray").pack(pady=5)
        
        self.window.mainloop()
    
    def on_auth_type_changed(self, event):
        self.toggle_auth_fields()
    
    def toggle_auth_fields(self):
        auth_type = self.auth_type_var.get()
        if auth_type == AuthenticationType.WINDOWS.value:
            self.entries['username'].config(state='disabled')
            self.entries['password'].config(state='disabled')
        else:
            self.entries['username'].config(state='normal')
            self.entries['password'].config(state='normal')

    def connect(self):
        try:
            auth_type = AuthenticationType(self.auth_type_var.get())
            
            # Get analysis configuration
            time_threshold = int(self.time_threshold_var.get())
            expected_frequency = int(self.expected_frequency_var.get())
            
            db_config = DatabaseConfig(
                host=self.entries['server_host'].get(),
                port=int(self.entries['port'].get()),
                username=self.entries['username'].get() if auth_type == AuthenticationType.SQL_SERVER else "",
                password=self.entries['password'].get() if auth_type == AuthenticationType.SQL_SERVER else "",
                database_name=self.entries['database_name'].get(),
                authentication_type=auth_type,
                save_credentials=self.save_credentials_var.get()
            )
            
            analysis_config = AnalysisConfig(
                time_threshold_minutes=time_threshold,
                expected_frequency_minutes=expected_frequency,
                time_column="TimeString"
            )
            
            # Pass migration flag to callback
            success = self.on_connect_callback(db_config, analysis_config, self.migrate_var.get())
            if success:
                self.window.destroy()
            else:
                messagebox.showerror("Connection Error", "Failed to connect to database. "
                                    "Please check your connection details.")
                
        except ValueError as e:
            messagebox.showerror("Input Error", f"Please check your inputs: {str(e)}")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {str(e)}")