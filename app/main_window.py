import tkinter as tk
from tkinter import ttk, messagebox
import threading
from tkinter import ttk

class MainWindow:
    def __init__(self, db_service, on_analyze_callback):
        self.db_service = db_service
        self.on_analyze_callback = on_analyze_callback
        self.window = None
        self.table_vars = {}
        self.sort_var = tk.BooleanVar(value=True)
        self.loading = False
        
    def show(self):
        self.window = tk.Tk()
        self.window.title("Production Cycle Analyzer - Main Panel")
        self.window.geometry("650x550")
        
        tk.Label(self.window, text="Select Tables to Analyze", font=("Arial", 14)).pack(pady=10)
        
        # Info label
        info_label = tk.Label(self.window, 
                            text="Only tables with 'TimeString' column will be available for analysis",
                            fg="blue", font=("Arial", 10))
        info_label.pack(pady=5)
        
        # Auto-sort checkbox
        sort_frame = tk.Frame(self.window)
        sort_frame.pack(pady=5)
        
        tk.Checkbutton(sort_frame, text="Auto-sort tables by TimeString", 
                      variable=self.sort_var, font=("Arial", 10)).pack()
        
        tk.Label(sort_frame, text="(Recommended for accurate cycle detection)", 
                fg="gray", font=("Arial", 8)).pack()
        
        # Progress bar
        self.progress = ttk.Progressbar(self.window, mode='indeterminate')
        self.progress.pack(fill='x', padx=20, pady=5)
        
        # Table list frame with scrollbar
        frame = tk.Frame(self.window)
        frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        # Create a frame for the listbox and scrollbar
        list_frame = tk.Frame(frame)
        list_frame.pack(fill='both', expand=True)
        
        scrollbar = tk.Scrollbar(list_frame)
        scrollbar.pack(side='right', fill='y')
        
        self.listbox = tk.Listbox(list_frame, selectmode='multiple', 
                                 yscrollcommand=scrollbar.set, height=15)
        self.listbox.pack(fill='both', expand=True)
        scrollbar.config(command=self.listbox.yview)
        
        # Status label
        self.status_label = tk.Label(self.window, text="Click 'Refresh Tables' to load tables", 
                                   fg="gray", font=("Arial", 9))
        self.status_label.pack(pady=5)
        
        # Button frame
        button_frame = tk.Frame(self.window)
        button_frame.pack(pady=10)
        
        # Refresh button
        self.refresh_btn = tk.Button(button_frame, text="Refresh Tables", 
                                   command=self.start_loading_tables,
                                   bg="orange", fg="white")
        self.refresh_btn.pack(side='left', padx=5)
        
        # Preview button
        self.preview_btn = tk.Button(button_frame, text="Preview Table", 
                                   command=self.preview_table,
                                   bg="purple", fg="white", state=tk.DISABLED)
        self.preview_btn.pack(side='left', padx=5)
        
        # Analyze button
        self.analyze_btn = tk.Button(button_frame, text="Analyze Tables", 
                                   command=self.analyze_tables,
                                   bg="blue", fg="white", state=tk.DISABLED)
        self.analyze_btn.pack(side='left', padx=5)
        
        self.window.mainloop()
    
    def start_loading_tables(self):
        """Start loading tables in a separate thread to avoid freezing"""
        if self.loading:
            return
            
        self.loading = True
        self.refresh_btn.config(state=tk.DISABLED)
        self.preview_btn.config(state=tk.DISABLED)
        self.analyze_btn.config(state=tk.DISABLED)
        self.status_label.config(text="Loading tables...", fg="blue")
        self.progress.start()
        
        # Run in separate thread to avoid freezing the UI
        thread = threading.Thread(target=self.load_tables_with_timestring_thread)
        thread.daemon = True
        thread.start()
    
    def load_tables_with_timestring_thread(self):
        """Thread function to load tables"""
        try:
            # Use the fast method to get tables with TimeString
            valid_tables = self.db_service.get_tables_with_timestring()
            
            # Update UI in the main thread
            self.window.after(0, self.update_table_list, valid_tables)
            
        except Exception as e:
            error_msg = f"Error loading tables: {str(e)}"
            self.window.after(0, self.show_error, error_msg)
    
    def update_table_list(self, valid_tables):
        """Update the UI with the loaded tables"""
        self.listbox.delete(0, tk.END)
        
        if valid_tables:
            for table in valid_tables:
                self.listbox.insert(tk.END, table)
            
            status_text = f"Found {len(valid_tables)} tables with TimeString column"
            self.status_label.config(text=status_text, fg="green")
            self.preview_btn.config(state=tk.NORMAL)
            self.analyze_btn.config(state=tk.NORMAL)
        else:
            self.listbox.insert(tk.END, "No tables with TimeString column found")
            self.listbox.config(state=tk.DISABLED)
            self.status_label.config(text="No tables with TimeString found", fg="red")
        
        self.loading = False
        self.refresh_btn.config(state=tk.NORMAL)
        self.progress.stop()
    
    def show_error(self, error_msg):
        """Show error message"""
        self.loading = False
        self.refresh_btn.config(state=tk.NORMAL)
        self.progress.stop()
        self.status_label.config(text=error_msg, fg="red")
        messagebox.showerror("Error", error_msg)
    
    def preview_table(self):
        """Preview the selected table with sorting applied"""
        if self.loading:
            return
            
        selected_indices = self.listbox.curselection()
        if not selected_indices:
            messagebox.showwarning("Selection Error", "Please select a table to preview")
            return
        
        table_name = self.listbox.get(selected_indices[0])
        
        # Show loading for preview
        self.status_label.config(text=f"Loading {table_name}...", fg="blue")
        self.progress.start()
        self.preview_btn.config(state=tk.DISABLED)
        
        # Run preview in separate thread
        thread = threading.Thread(target=self.preview_table_thread, args=(table_name,))
        thread.daemon = True
        thread.start()
    
    def preview_table_thread(self, table_name):
        """Thread function to preview table"""
        try:
            # Fetch and optionally sort the table
            if self.sort_var.get():
                df = self.db_service.fetch_and_sort_table_data(table_name)
                title_suffix = " (Sorted)"
            else:
                df = self.db_service.fetch_table_data(table_name)
                title_suffix = " (Original)"
            
            if df is None:
                self.window.after(0, lambda: messagebox.showerror("Error", f"Could not load table '{table_name}'"))
            else:
                # Show preview window in main thread
                self.window.after(0, self.show_table_preview, df, f"Preview: {table_name}{title_suffix}")
                
        except Exception as e:
            error_msg = f"Error previewing table: {str(e)}"
            self.window.after(0, lambda: messagebox.showerror("Error", error_msg))
        finally:
            self.window.after(0, self.end_preview_loading)
    
    def end_preview_loading(self):
        """End the preview loading state"""
        self.progress.stop()
        self.preview_btn.config(state=tk.NORMAL)
        self.status_label.config(text="Ready", fg="green")
    
    def show_table_preview(self, df, title):
        """Show a preview of the table data with datetime info"""
        preview = tk.Toplevel(self.window)
        preview.title(title)
        preview.geometry("900x500")
        
        tk.Label(preview, text=f"{title} - First 50 rows", 
                font=("Arial", 12, "bold")).pack(pady=10)
        
        # Show datetime conversion info
        if 'TimeString' in df.columns:
            from infrastructure.table_service import TableService
            ts = TableService()
            conversion_info = ts.verify_datetime_conversion(df)
            
            info_text = f"Datetime conversion: {'✅ SUCCESS' if conversion_info['success'] else '❌ FAILED'}\n"
            info_text += f"Valid dates: {conversion_info['valid_count']}, Invalid: {conversion_info['invalid_count']}\n"
            if conversion_info['success']:
                info_text += f"Range: {conversion_info['date_range']}"
            
            info_label = tk.Label(preview, text=info_text, font=("Arial", 9), 
                                fg="green" if conversion_info['success'] else "red")
            info_label.pack(pady=5)
        
        # Create treeview for preview
        columns = list(df.columns)
        tree_frame = ttk.Frame(preview)
        tree_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=15)
        
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=120)
        
        # Add scrollbar
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Add data to treeview (first 50 rows)
        for _, row in df.head(50).iterrows():
            tree.insert("", "end", values=tuple(row))
        
        tk.Button(preview, text="Close", command=preview.destroy).pack(pady=10)
    
    def analyze_tables(self):
        selected_indices = self.listbox.curselection()
        selected_tables = [self.listbox.get(i) for i in selected_indices]
        
        if not selected_tables or selected_tables[0] == "No tables with TimeString column found":
            messagebox.showwarning("Selection Error", "Please select at least one valid table")
            return
        
        # Fetch table data with optional sorting
        table_data = {}
        for table in selected_tables:
            if self.sort_var.get():
                # Use sorted data
                df = self.db_service.fetch_and_sort_table_data(table)
            else:
                # Use original data
                df = self.db_service.fetch_table_data(table)
            
            if df is not None and any(col.lower() == 'timestring' for col in df.columns):
                table_data[table] = df
                status = "sorted" if self.sort_var.get() else "original"
                print(f"Loaded {table} ({status})")
            else:
                messagebox.showwarning("Data Error", 
                    f"Table '{table}' doesn't have TimeString column or is empty")
        
        if table_data:
            self.window.destroy()
            self.on_analyze_callback(table_data)
        else:
            messagebox.showerror("Error", "No valid table data could be loaded for analysis")