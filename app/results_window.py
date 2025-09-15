import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import pandas as pd
from core.models import AuthenticationType

class ResultsWindow:
    def __init__(self, analysis_service, db_service, on_complete_callback):
        self.analysis_service = analysis_service
        self.db_service = db_service
        self.on_complete_callback = on_complete_callback
        self.window = None
        self.selected_table = None
        self.lotedata_type = tk.StringVar(value="summary")  # summary or detailed
        self.ref_var = tk.StringVar()  # Move this to init
        self.ref_dropdown = None
        
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
            
            # Create LOTEDATA button
            tk.Button(lotedata_frame, text="Create LOTEDATA Table", command=self.create_lotedata,
                     bg="green", fg="white", font=("Arial", 10, "bold")).pack(pady=10)
        else:
            tk.Label(self.window, text="No valid tables with cycles found for LOTEDATA creation",
                    fg="red", font=("Arial", 10)).pack(pady=20)
        
        self.window.mainloop()
    
    def create_lotedata(self):
        # Get the selected value from the combobox
        selected_table = self.ref_var.get()
        
        # Debug print to see what's being retrieved
        print(f"Selected table: '{selected_table}'")
        print(f"Combobox values: {self.ref_dropdown['values']}")
        print(f"Current combobox selection: {self.ref_dropdown.get()}")
        
        if not selected_table or selected_table.strip() == "":
            # If no selection from variable, try to get from combobox directly
            selected_table = self.ref_dropdown.get()
            print(f"Falling back to combobox get(): '{selected_table}'")
            
        if not selected_table or selected_table.strip() == "":
            messagebox.showwarning("Selection Error", "Please select a reference table")
            return
        
        try:
            # Get reference table data for detailed mapping
            ref_df = self.db_service.fetch_table_data(selected_table)
            if ref_df is None or ref_df.empty:
                raise ValueError("Could not fetch reference table data or table is empty")
            
            # Generate LOTEDATA based on selected type
            if self.lotedata_type.get() == "summary":
                lotedata_df = self.analysis_service.generate_lotedata_summary(selected_table)
                table_name = "LOTEDATA_SUMMARY"
            else:
                lotedata_df = self.analysis_service.generate_lotedata_detailed(selected_table, ref_df)
                table_name = "LOTEDATA_DETAILED"
            
            # Save to database
            success = self.db_service.save_lotedata(lotedata_df, table_name)
            
            if success:
                messagebox.showinfo("Success", f"{table_name} created successfully!")
                # Show preview of the created data
                self.show_lotedata_preview(lotedata_df, table_name)
                self.window.destroy()
                self.on_complete_callback()
            else:
                messagebox.showerror("Error", f"Failed to create {table_name}")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create LOTEDATA: {str(e)}")
    
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