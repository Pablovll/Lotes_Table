# app/results_window.py
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import pandas as pd
from core.models import AuthenticationType
from services.fact_samples_service import FactSamplesService
import threading
class ResultsWindow:
    def __init__(self, analysis_service, db_service, on_complete_callback):
        self.analysis_service = analysis_service
        self.db_service = db_service
        self.on_complete_callback = on_complete_callback
        self.window = None
        self.selected_table = None
        self.lotedata_type = tk.StringVar(value="detailed_mapping")  # summary, detailed, or detailed_mapping
        self.ref_var = tk.StringVar()  # Move this to init
        self.ref_dropdown = None
        self.valid_tables = [] 
        
    def show(self, analysis_results):
        self.window = tk.Tk()
        self.window.title("Production Cycle Analyzer - Results")
        self.window.geometry("800x600")
        
        # Get analysis summary
        summary = self.analysis_service.get_analysis_summary()
        
        # Summary frame
        summary_frame = tk.Frame(self.window)
        summary_frame.pack(pady=10, padx=20, fill='x')
        
        summary_text = f"Tables: {summary['total_tables']} | " \
                      f"With Cycles: {summary['tables_with_cycles']} | " \
                      f"With Errors: {summary['tables_with_errors']} | " \
                      f"Total Cycles: {summary['total_cycles']} | " \
                      f"Time Matched: {'Yes' if summary['time_matched'] else 'No'}"
        
        tk.Label(summary_frame, text=summary_text, font=("Arial", 11, "bold"), 
                fg="green" if not summary['tables_with_errors'] else "orange").pack()
        
        # Create notebook for tabs
        notebook = ttk.Notebook(self.window)
        notebook.pack(pady=10, padx=20, fill='both', expand=True)
        
        # Results tab
        results_frame = ttk.Frame(notebook)
        notebook.add(results_frame, text="Analysis Results")
        
        # Create treeview for results with scrollbar
        tree_frame = ttk.Frame(results_frame)
        tree_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        columns = ("Table", "Cycles", "Time Matched", "Status", "First Cycle", "Last Cycle")
        tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=12)
        
        column_widths = {"Table": 150, "Cycles": 80, "Time Matched": 100, 
                        "Status": 120, "First Cycle": 150, "Last Cycle": 150}
        
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=column_widths.get(col, 100))
        
        # Add scrollbar
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Add data to treeview
        valid_tables = []
        for table_name, result in analysis_results.items():
            first_cycle = result.cycles[0] if result.cycles else None
            last_cycle = result.cycles[-1] if result.cycles else None
            
            status = "✅ OK" if not result.error_message else f"❌ Error: {result.error_message}"
            
            tree.insert("", "end", values=(
                table_name,
                result.total_cycles,
                "Yes" if result.time_matched else "No",
                status,
                first_cycle.start_time.strftime("%d/%m/%Y %H:%M") if first_cycle else "N/A",
                last_cycle.end_time.strftime("%d/%m/%Y %H:%M") if last_cycle else "N/A"
            ))
            
            if not result.error_message and result.total_cycles > 0:
                valid_tables.append(table_name)
        
        # Details tab
        details_frame = ttk.Frame(notebook)
        notebook.add(details_frame, text="Cycle Details")
        
        # Cycle details text area
        details_text = scrolledtext.ScrolledText(details_frame, width=70, height=20)
        details_text.pack(padx=10, pady=10, fill='both', expand=True)
        
        # Add cycle details
        details_text.insert(tk.END, "CYCLE DETAILS\n" + "="*50 + "\n\n")
        for table_name, result in analysis_results.items():
            if result.cycles:
                details_text.insert(tk.END, f"Table: {table_name}\n")
                details_text.insert(tk.END, f"Total Cycles: {result.total_cycles}\n")
                for cycle in result.cycles[:5]:  # Show first 5 cycles
                    details_text.insert(tk.END, 
                        f"Cycle {cycle.cycle_id}: {cycle.start_time.strftime('%d/%m/%Y %H:%M')} "
                        f"to {cycle.end_time.strftime('%d/%m/%Y %H:%M')} "
                        f"({cycle.duration_minutes:.1f} min, {cycle.sample_count} samples)\n"
                    )
                if result.total_cycles > 5:
                    details_text.insert(tk.END, f"... and {result.total_cycles - 5} more cycles\n")
                details_text.insert(tk.END, "-"*50 + "\n")
        
        details_text.config(state=tk.DISABLED)
        
        # LOTEDATA creation section (only if we have valid tables)
        if valid_tables:
            self.valid_tables = valid_tables
            lotedata_frame = tk.Frame(self.window)
            lotedata_frame.pack(pady=10)
            
            # Reference table selection
            ref_frame = tk.Frame(lotedata_frame)
            ref_frame.pack(pady=5)
            
            tk.Label(ref_frame, text="Reference Table:").pack(side='left')
            
            # Initialize the dropdown and store reference
            self.ref_dropdown = ttk.Combobox(ref_frame, textvariable=self.ref_var, 
                                           values=valid_tables, state="readonly")
            self.ref_dropdown.pack(side='left', padx=5)
            self.ref_dropdown.set(valid_tables[0])
            
            # LOTEDATA type selection
            type_frame = tk.Frame(lotedata_frame)
            type_frame.pack(pady=5)
            
            tk.Label(type_frame, text="LOTEDATA Type:").pack(side='left')
            
            ttk.Radiobutton(type_frame, text="Summary Table", 
               variable=self.lotedata_type, value="summary").pack(side='left', padx=5)
            ttk.Radiobutton(type_frame, text="Detailed Mapping", 
               variable=self.lotedata_type, value="detailed").pack(side='left', padx=5)
            ttk.Radiobutton(type_frame, text="BOTH: LOTE_SUMMARY + LOTE_DATA", 
               variable=self.lotedata_type, value="detailed_mapping").pack(side='left', padx=5)
            
            tk.Button(lotedata_frame, text="Create LOTEDATA Table", command=self.create_lotedata,
                     bg="green", fg="white", font=("Arial", 10, "bold")).pack(pady=10)
            
            # FACT SAMPLES ETL Button
            etl_frame = tk.Frame(self.window)
            etl_frame.pack(pady=10)

            fact_samples_btn = tk.Button(etl_frame, text="🚀 Build Complete Data Warehouse", 
                    command=self.run_fact_samples_etl,
                    bg="blue", fg="white", font=("Arial", 10, "bold"),
                    padx=20, pady=10)
            fact_samples_btn.pack(pady=5)

            # Add tooltip
            self._create_tooltip(fact_samples_btn, 
                "Build FactSamples data warehouse from ALL tables with TimeString data\n"
                "Includes both analyzed and non-analyzed tables for complete reporting")

            tk.Label(etl_frame, text="Includes ALL tables with TimeString data for complete Power BI reporting",
                    fg="gray", font=("Arial", 9)).pack()

        else:
            tk.Label(self.window, text="No valid tables with cycles found for LOTEDATA creation",
                    fg="red", font=("Arial", 10)).pack(pady=20)
        
        self.window.mainloop()
    

    
    def create_lotedata(self):
        # Get the selected value from the combobox
        selected_table = self.ref_var.get()
        
        # DEBUG: Print the selected options
        print(f"DEBUG: Selected table: {selected_table}")
        print(f"DEBUG: Selected lotedata_type: {self.lotedata_type.get()}")
        
        if not selected_table or selected_table.strip() == "":
            # If no selection from variable, try to get from combobox directly
            selected_table = self.ref_dropdown.get()
            
        if not selected_table or selected_table.strip() == "":
            messagebox.showwarning("Selection Error", "Please select a reference table")
            return
        
        try:
            # Get reference table data for detailed mapping
            ref_df = self.db_service.fetch_table_data(selected_table)
            if ref_df is None or ref_df.empty:
                raise ValueError("Could not fetch reference table data or table is empty")
            
            # Generate LOTEDATA based on selected type
            lotedata_type = self.lotedata_type.get()
            print(f"DEBUG: Processing lotedata_type: {lotedata_type}")
            
            if lotedata_type == "summary":
                print("DEBUG: Generating summary table")
                lotedata_df = self.analysis_service.generate_lotedata_summary(selected_table)
                success = self.db_service.save_lotedata(lotedata_df, "LOTEDATA_SUMMARY")
                table_name = "LOTEDATA_SUMMARY"
                
            elif lotedata_type == "detailed":
                print("DEBUG: Generating detailed table")
                lotedata_df = self.analysis_service.generate_lotedata_detailed(selected_table, ref_df)
                success = self.db_service.save_lotedata(lotedata_df, "LOTEDATA_DETAILED")
                table_name = "LOTEDATA_DETAILED"
                
            elif lotedata_type == "detailed_mapping":
                print("DEBUG: Generating both LOTE tables")
                # Generate both tables simultaneously
                lote_tables = self.analysis_service.generate_both_lote_tables(selected_table, ref_df)
                
                # Save both tables to database
                success1 = self.db_service.save_lotedata(lote_tables['LOTE_SUMMARY'], "LOTE_SUMMARY")
                success2 = self.db_service.save_lotedata(lote_tables['LOTE_DATA'], "LOTE_DATA")
                
                success = success1 and success2
                table_name = "LOTE_SUMMARY and LOTE_DATA"
                
                # DEBUG: Print information about the tables
                print(f"DEBUG: LOTE_SUMMARY shape: {lote_tables['LOTE_SUMMARY'].shape}")
                print(f"DEBUG: LOTE_DATA shape: {lote_tables['LOTE_DATA'].shape}")
                print(f"DEBUG: LOTE_DATA columns: {list(lote_tables['LOTE_DATA'].columns)}")
                print(f"DEBUG: Save LOTE_SUMMARY success: {success1}")
                print(f"DEBUG: Save LOTE_DATA success: {success2}")
                
            else:
                raise ValueError(f"Invalid LOTEDATA type selected: {lotedata_type}")
            
            if success:
                messagebox.showinfo("Success", f"{table_name} created successfully!")
                
                # Show preview of the created data
                if lotedata_type == "detailed_mapping":
                    # Show preview of both tables
                    self.show_lotedata_preview(lote_tables['LOTE_SUMMARY'], "LOTE_SUMMARY")
                    self.show_lotedata_preview(lote_tables['LOTE_DATA'], "LOTE_DATA")
                else:
                    self.show_lotedata_preview(lotedata_df, table_name)
                    
            else:
                messagebox.showerror("Error", f"Failed to create {table_name}")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create LOTEDATA: {str(e)}")
            import traceback
            traceback.print_exc()  # This will print the full traceback to help debug
    def show_lotedata_preview(self, df, table_name):
        """Show a preview of the LOTEDATA table"""
        preview = tk.Toplevel(self.window)
        preview.title(f"{table_name} Preview")
        preview.geometry("800x400")
        
        tk.Label(preview, text=f"{table_name} Preview (First 20 rows)", 
                font=("Arial", 12, "bold")).pack(pady=10)
        
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
        
        # Add data to treeview (first 20 rows)
        for _, row in df.head(20).iterrows():
            tree.insert("", "end", values=tuple(row))
        
        tk.Button(preview, text="Close", command=preview.destroy).pack(pady=10)

    def run_fact_samples_etl(self):
        """Run the FactSamples ETL process using ALL available tables"""
        # Get ALL tables with TimeString, not just the analyzed ones
        all_tables = self.db_service.get_tables_with_timestring()
        
        if not all_tables:
            messagebox.showwarning("Warning", "No tables with TimeString found for ETL")
            return
        
        # Confirm with user - show how many tables will be processed
        confirm = messagebox.askyesno(
            "Confirm Full ETL Process",
            "This will build the complete FactSamples data warehouse for Power BI.\n\n"
            f"Process ALL {len(all_tables)} tables with TimeString data?\n"
            "This may take several minutes for large datasets.",
            icon='warning'
        )
        
        if not confirm:
            return
        
        # Disable button during processing
        for widget in self.window.winfo_children():
            if isinstance(widget, tk.Button) and "Run FactSamples ETL" in widget.cget('text'):
                widget.config(state='disabled', bg='gray')
                break
        
        # Show progress window
        self.show_etl_progress(all_tables)
        
        # Run ETL in separate thread to avoid freezing UI
        thread = threading.Thread(target=self._run_etl_thread, args=(all_tables,))
        thread.daemon = True
        thread.start()

    def _run_etl_thread(self, all_tables):
        """Run ETL in background thread using all tables"""
        try:
            # Initialize and run the ETL service with ALL tables
            etl_service = FactSamplesService(self.db_service, all_tables)
            success = etl_service.run()
            
            # Update UI in main thread
            self.window.after(0, self._etl_complete, success, len(all_tables))
            
        except Exception as e:
            self.window.after(0, self._etl_error, str(e))

    def _etl_complete(self, success, table_count):
        """Handle ETL completion"""
        # Re-enable button
        for widget in self.window.winfo_children():
            if isinstance(widget, tk.Button) and widget.cget('state') == 'disabled':
                widget.config(state='normal', bg='blue')
                break
        
        # Close progress window
        #if hasattr(self, 'progress_window'):
         #   self.progress_window.destroy()
        
        if success:
            messagebox.showinfo("Success", 
                f"FactSamples ETL completed successfully!\n\n"
                f"Processed {table_count} tables\n"
                "Data warehouse is ready for Power BI reporting.")
        else:
            messagebox.showerror("Error", 
                "FactSamples ETL failed. Check console for details.")

    def show_etl_progress(self, all_tables):
        """Show progress window for ETL process with all tables"""
        self.progress_window = tk.Toplevel(self.window)
        self.progress_window.title("FactSamples ETL - Processing ALL Tables")
        self.progress_window.geometry("500x300")
        self.progress_window.transient(self.window)
        self.progress_window.grab_set()
        
        # Center the window
        self.progress_window.update_idletasks()
        x = self.window.winfo_x() + (self.window.winfo_width() - 500) // 2
        y = self.window.winfo_y() + (self.window.winfo_height() - 300) // 2
        self.progress_window.geometry(f"+{x}+{y}")
        
        # Progress content
        tk.Label(self.progress_window, text="🚀 Building Complete FactSamples Data Warehouse", 
                font=("Arial", 12, "bold")).pack(pady=20)
        
        tk.Label(self.progress_window, text=f"Processing ALL {len(all_tables)} tables:", 
                font=("Arial", 10)).pack()
        
        # Scrollable table list
        frame = tk.Frame(self.progress_window)
        frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        scrollbar = tk.Scrollbar(frame)
        scrollbar.pack(side='right', fill='y')
        
        listbox = tk.Listbox(frame, yscrollcommand=scrollbar.set, font=("Arial", 9))
        listbox.pack(fill='both', expand=True)
        
        for table in all_tables:
            listbox.insert('end', f"• {table}")
        
        scrollbar.config(command=listbox.yview)
        
        # Progress bar
        self.etl_progress = ttk.Progressbar(self.progress_window, mode='indeterminate')
        self.etl_progress.pack(fill='x', padx=50, pady=10)
        self.etl_progress.start()
        
        tk.Label(self.progress_window, text="Building complete data warehouse from all available tables...", 
                fg="gray", font=("Arial", 9)).pack()

    def _etl_error(self, error_msg):
        """Handle ETL errors"""
        # Re-enable button
        for widget in self.window.winfo_children():
            if isinstance(widget, tk.Button) and widget.cget('state') == 'disabled':
                widget.config(state='normal', bg='blue')
                break
        
        # Close progress window
       # if hasattr(self, 'progress_window'):
        #    self.progress_window.destroy()
        
        messagebox.showerror("ETL Error", f"FactSamples ETL failed:\n\n{error_msg}")
    
    def _create_tooltip(self, widget, text):
        """Create a tooltip for a widget"""
        def on_enter(event):
            tooltip = tk.Toplevel(self.window)
            tooltip.wm_overrideredirect(True)
            tooltip.wm_geometry(f"+{event.x_root+10}+{event.y_root+10}")
            
            label = tk.Label(tooltip, text=text, justify='left',
                        background="#ffffe0", relief='solid', borderwidth=1,
                        font=("Arial", 8))
            label.pack()
            
            widget.tooltip = tooltip
        
        def on_leave(event):
            if hasattr(widget, 'tooltip'):
                widget.tooltip.destroy()
        
        widget.bind('<Enter>', on_enter)
        widget.bind('<Leave>', on_leave)