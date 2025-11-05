#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WFM Turni Generator - GUI Interface
Interfaccia grafica per la generazione automatica dei turni
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import sys
import os
import subprocess
import threading
from pathlib import Path

class WfmGui:
    def __init__(self, root):
        self.root = root
        self.root.title("WFM Turni Generator v6.0")
        self.root.geometry("800x700")
        self.root.resizable(True, True)

        # Variables
        self.input_file = tk.StringVar()
        self.output_file = tk.StringVar(value="output.xlsx")
        self.grid_step = tk.StringVar(value="15")
        self.prefer_phase = tk.StringVar(value="15,45")
        self.strict_phase = tk.BooleanVar(value=False)
        self.force_ot = tk.BooleanVar(value=False)
        self.force_balance = tk.BooleanVar(value=False)
        self.overcap = tk.StringVar(value="")
        self.overcap_penalty = tk.StringVar(value="")

        self.is_running = False

        self.setup_ui()

    def setup_ui(self):
        """Setup the user interface"""
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)

        # Title
        title = ttk.Label(main_frame, text="🔧 WFM Turni Generator",
                         font=('Arial', 16, 'bold'))
        title.grid(row=0, column=0, columnspan=3, pady=(0, 20))

        row = 1

        # === FILE SECTION ===
        files_frame = ttk.LabelFrame(main_frame, text="📁 Files", padding="10")
        files_frame.grid(row=row, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        files_frame.columnconfigure(1, weight=1)

        # Input file
        ttk.Label(files_frame, text="File Input:").grid(row=0, column=0, sticky=tk.W, pady=5)
        ttk.Entry(files_frame, textvariable=self.input_file, width=50).grid(
            row=0, column=1, sticky=(tk.W, tk.E), padx=5, pady=5)
        ttk.Button(files_frame, text="Browse...", command=self.browse_input).grid(
            row=0, column=2, pady=5)

        # Output file
        ttk.Label(files_frame, text="File Output:").grid(row=1, column=0, sticky=tk.W, pady=5)
        ttk.Entry(files_frame, textvariable=self.output_file, width=50).grid(
            row=1, column=1, sticky=(tk.W, tk.E), padx=5, pady=5)
        ttk.Button(files_frame, text="Browse...", command=self.browse_output).grid(
            row=1, column=2, pady=5)

        row += 1

        # === PARAMETERS SECTION ===
        params_frame = ttk.LabelFrame(main_frame, text="⚙️ Parametri", padding="10")
        params_frame.grid(row=row, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        params_frame.columnconfigure(1, weight=1)
        params_frame.columnconfigure(3, weight=1)

        # Grid step
        ttk.Label(params_frame, text="Grid Step (min):").grid(row=0, column=0, sticky=tk.W, pady=5)
        grid_combo = ttk.Combobox(params_frame, textvariable=self.grid_step,
                                  values=["15", "30", "60"], width=10)
        grid_combo.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)

        # Prefer phase
        ttk.Label(params_frame, text="Prefer Phase:").grid(row=0, column=2, sticky=tk.W, padx=(20, 0), pady=5)
        ttk.Entry(params_frame, textvariable=self.prefer_phase, width=15).grid(
            row=0, column=3, sticky=tk.W, padx=5, pady=5)

        # Overcap
        ttk.Label(params_frame, text="Overcap:").grid(row=1, column=0, sticky=tk.W, pady=5)
        ttk.Entry(params_frame, textvariable=self.overcap, width=10).grid(
            row=1, column=1, sticky=tk.W, padx=5, pady=5)

        # Overcap penalty
        ttk.Label(params_frame, text="Overcap Penalty:").grid(row=1, column=2, sticky=tk.W, padx=(20, 0), pady=5)
        ttk.Entry(params_frame, textvariable=self.overcap_penalty, width=15).grid(
            row=1, column=3, sticky=tk.W, padx=5, pady=5)

        row += 1

        # === FLAGS SECTION ===
        flags_frame = ttk.LabelFrame(main_frame, text="🚩 Opzioni", padding="10")
        flags_frame.grid(row=row, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)

        ttk.Checkbutton(flags_frame, text="Strict Phase Mode",
                       variable=self.strict_phase).grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Checkbutton(flags_frame, text="Force Overtime",
                       variable=self.force_ot).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Checkbutton(flags_frame, text="Force Balance",
                       variable=self.force_balance).grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)

        row += 1

        # === EXECUTE BUTTON ===
        self.run_button = ttk.Button(main_frame, text="🚀 GENERA TURNI",
                                     command=self.run_generation,
                                     style='Accent.TButton')
        self.run_button.grid(row=row, column=0, columnspan=3, pady=20, ipadx=20, ipady=10)

        row += 1

        # === PROGRESS BAR ===
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.grid(row=row, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)

        row += 1

        # === LOG SECTION ===
        log_frame = ttk.LabelFrame(main_frame, text="📋 Log", padding="10")
        log_frame.grid(row=row, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        # Configure main frame to expand log
        main_frame.rowconfigure(row, weight=1)

        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, wrap=tk.WORD,
                                                  font=('Consolas', 9))
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure tags for colored output
        self.log_text.tag_config('info', foreground='black')
        self.log_text.tag_config('success', foreground='green')
        self.log_text.tag_config('warning', foreground='orange')
        self.log_text.tag_config('error', foreground='red')

        # Initial message
        self.log("WFM Turni Generator v6.0 - Pronto all'uso!", 'info')
        self.log("Seleziona il file di input e configura i parametri.", 'info')

    def browse_input(self):
        """Browse for input file"""
        filename = filedialog.askopenfilename(
            title="Seleziona file input",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")]
        )
        if filename:
            self.input_file.set(filename)
            # Auto-set output name based on input
            base = Path(filename).stem
            output_path = Path(filename).parent / f"{base}_output.xlsx"
            self.output_file.set(str(output_path))
            self.log(f"Input file: {filename}", 'success')

    def browse_output(self):
        """Browse for output file"""
        filename = filedialog.asksaveasfilename(
            title="Salva output come",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        if filename:
            self.output_file.set(filename)
            self.log(f"Output file: {filename}", 'success')

    def log(self, message, tag='info'):
        """Add message to log"""
        self.log_text.insert(tk.END, message + "\n", tag)
        self.log_text.see(tk.END)
        self.root.update_idletasks()

    def validate_inputs(self):
        """Validate all inputs before running"""
        if not self.input_file.get():
            messagebox.showerror("Errore", "Seleziona un file di input!")
            return False

        if not os.path.exists(self.input_file.get()):
            messagebox.showerror("Errore", f"Il file di input non esiste:\n{self.input_file.get()}")
            return False

        if not self.output_file.get():
            messagebox.showerror("Errore", "Specifica un file di output!")
            return False

        try:
            grid = int(self.grid_step.get())
            if grid not in [15, 30, 60]:
                raise ValueError()
        except:
            messagebox.showerror("Errore", "Grid step deve essere 15, 30 o 60!")
            return False

        return True

    def build_command(self):
        """Build the command line arguments"""
        # Find the script
        script_path = Path(__file__).parent / "wfm_claudegit6.py"
        if not script_path.exists():
            # Try alternative name
            script_path = Path(__file__).parent / "generate_turni_v5_11_ok_only_copconfig.py"

        if not script_path.exists():
            raise FileNotFoundError("Script WFM non trovato!")

        cmd = [sys.executable, str(script_path)]
        cmd.extend(["--input", self.input_file.get()])
        cmd.extend(["--out", self.output_file.get()])
        cmd.extend(["--grid", self.grid_step.get()])

        if self.prefer_phase.get().strip():
            cmd.extend(["--prefer_phase", self.prefer_phase.get()])

        if self.strict_phase.get():
            cmd.append("--strict-phase")

        if self.force_ot.get():
            cmd.append("--force-ot")

        if self.force_balance.get():
            cmd.append("--force-balance")

        if self.overcap.get().strip():
            cmd.extend(["--overcap", self.overcap.get()])

        if self.overcap_penalty.get().strip():
            cmd.extend(["--overcap-penalty", self.overcap_penalty.get()])

        return cmd

    def run_generation(self):
        """Run the generation in a separate thread"""
        if self.is_running:
            messagebox.showwarning("Attenzione", "Generazione già in corso!")
            return

        if not self.validate_inputs():
            return

        # Start in thread
        thread = threading.Thread(target=self._run_generation_thread, daemon=True)
        thread.start()

    def _run_generation_thread(self):
        """Thread function to run generation"""
        self.is_running = True
        self.run_button.config(state='disabled')
        self.progress.start(10)

        try:
            cmd = self.build_command()
            self.log("=" * 60, 'info')
            self.log("Avvio generazione turni...", 'info')
            self.log(f"Comando: {' '.join(cmd)}", 'info')
            self.log("=" * 60, 'info')

            # Run subprocess
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            # Read output in real-time
            for line in process.stdout:
                line = line.rstrip()
                if line:
                    # Determine tag based on content
                    if 'ERROR' in line or '⛔' in line or 'ERRORE' in line:
                        tag = 'error'
                    elif 'WARNING' in line or '⚠️' in line:
                        tag = 'warning'
                    elif '✅' in line or 'OK' in line or 'Completato' in line:
                        tag = 'success'
                    else:
                        tag = 'info'

                    self.log(line, tag)

            # Wait for completion
            return_code = process.wait()

            self.log("=" * 60, 'info')
            if return_code == 0:
                self.log("✅ GENERAZIONE COMPLETATA CON SUCCESSO!", 'success')
                self.log(f"Output salvato in: {self.output_file.get()}", 'success')
                messagebox.showinfo("Successo",
                                   f"Turni generati con successo!\n\nOutput: {self.output_file.get()}")
            else:
                self.log(f"❌ ERRORE: processo terminato con codice {return_code}", 'error')
                messagebox.showerror("Errore",
                                    f"Generazione fallita!\nCodice errore: {return_code}")
            self.log("=" * 60, 'info')

        except Exception as e:
            self.log(f"❌ ERRORE: {str(e)}", 'error')
            messagebox.showerror("Errore", f"Errore durante la generazione:\n{str(e)}")

        finally:
            self.is_running = False
            self.run_button.config(state='normal')
            self.progress.stop()


def main():
    root = tk.Tk()

    # Style configuration
    style = ttk.Style()

    # Try to use a modern theme
    try:
        style.theme_use('clam')
    except:
        pass

    # Configure custom button style
    style.configure('Accent.TButton', font=('Arial', 12, 'bold'))

    app = WfmGui(root)
    root.mainloop()


if __name__ == "__main__":
    main()
