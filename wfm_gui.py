
import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import pandas as pd
import re
import subprocess
import threading
import sys
import os
import wfm_claudegitmultiskill3 as engine

class WFMLauncher:
    def __init__(self, root):
        self.root = root
        self.root.title("WFM Multi-Skill Launcher v3.0 (Strategies)")
        self.root.geometry("750x900")

        # --- Variabili di Configurazione ---
        self.input_path = tk.StringVar()
        self.output_path = tk.StringVar(value="risultato_turni.xlsx")
        self.grid_val = tk.IntVar(value=15)
        self.prefer_phase = tk.StringVar(value="15,45")
        
        # --- Strategie ---
        # 1. Rigidità Oraria
        self.hourly_strategy_var = tk.StringVar(value="Flessibile (Default)")
        self.hourly_options = ["Flessibile (Default)", "Rigida (Strict Phase)", "Obbligata (Force Phase)"]
        
        # 2. Copertura
        self.coverage_strategy_var = tk.StringVar(value="Efficienza (Default)")
        self.coverage_options = ["Efficienza (Default)", "Bilanciamento (Garantisci turni)"]

        # 3. Straordinario
        self.ot_strategy_var = tk.StringVar(value="Al Bisogno (Default)")
        self.ot_options = ["Al Bisogno (Default)", "Massimizza (Usa TUTTO)"]
        
        # Altro
        self.use_predefined = tk.BooleanVar()
        self.weekend_guard_var = tk.BooleanVar(value=True)
        self.weekend_overcap_pct = tk.IntVar(value=0)
        self.uniform_overcap_var = tk.BooleanVar(value=False)
        self.uniform_overcap_tol_pct = tk.IntVar(value=10)
        self.skills_loaded = []

        # Rigidity Preset (Overcap)
        self.rigidity_var = tk.StringVar(value="Normale")
        self.rigidity_map = {
            "Blanda (Tollera eccessi)": 1.0,
            "Normale": 10.0,
            "Rigida (Evita eccessi)": 50.0
        }

        # Dizionari per gli override giornalieri
        # Chiavi: Lun, Mar, Mer, Gio, Ven, Sab, Dom
        self.days = ["Lun", "Mar", "Mer", "Gio", "Ven", "Sab", "Dom"]
        self.overcap_vars = {day: tk.StringVar() for day in self.days}
        self.penalty_vars = {day: tk.StringVar() for day in self.days}

        self._apply_style()
        self._build_ui()

    def _apply_style(self):
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("TLabel", font=("Segoe UI", 9))
        style.configure("TButton", font=("Segoe UI", 9, "bold"))
        style.configure("Header.TLabel", font=("Segoe UI", 10, "bold"))
        style.configure("Group.TLabel", font=("Segoe UI", 9, "bold", "underline"))

    def _build_ui(self):
        outer = ttk.Frame(self.root)
        outer.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(outer, highlightthickness=0)
        vscroll = ttk.Scrollbar(outer, orient=tk.VERTICAL, command=canvas.yview)
        canvas.configure(yscrollcommand=vscroll.set)

        vscroll.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        container = ttk.Frame(canvas, padding="15")
        container_id = canvas.create_window((0, 0), window=container, anchor="nw")

        def _on_frame_configure(_event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _on_canvas_configure(event):
            canvas.itemconfigure(container_id, width=event.width)

        container.bind("<Configure>", _on_frame_configure)
        canvas.bind("<Configure>", _on_canvas_configure)

        def _on_mousewheel(event):
            if isinstance(event.widget, tk.Text):
                return
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        def _bind_mousewheel(_event):
            canvas.bind_all("<MouseWheel>", _on_mousewheel)

        def _unbind_mousewheel(_event):
            canvas.unbind_all("<MouseWheel>")

        canvas.bind("<Enter>", _bind_mousewheel)
        canvas.bind("<Leave>", _unbind_mousewheel)

        # --- 1. Selezione File ---
        file_frame = ttk.LabelFrame(container, text="1. Selezione File", padding="10")
        file_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(file_frame, text="File Input (.xlsx):").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Entry(file_frame, textvariable=self.input_path, width=50).grid(row=0, column=1, padx=5, pady=2)
        ttk.Button(file_frame, text="Sfoglia...", command=self.browse_input).grid(row=0, column=2, pady=2)

        ttk.Label(file_frame, text="File Output (.xlsx):").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Entry(file_frame, textvariable=self.output_path, width=50).grid(row=1, column=1, padx=5, pady=2)
        ttk.Button(file_frame, text="Sfoglia...", command=self.browse_output).grid(row=1, column=2, pady=2)

        # --- 2. Strategie e Parametri ---
        config_frame = ttk.LabelFrame(container, text="2. Strategie e Parametri", padding="10")
        config_frame.pack(fill=tk.X, pady=(0, 10))

        # Row 1: Grid Base
        r1 = ttk.Frame(config_frame)
        r1.pack(fill=tk.X, pady=5)
        ttk.Label(r1, text="Griglia Minuti:").pack(side=tk.LEFT)
        ttk.Spinbox(r1, from_=5, to=60, increment=5, textvariable=self.grid_val, width=5).pack(side=tk.LEFT, padx=5)
        ttk.Label(r1, text="Minuti Inizio Preferiti:").pack(side=tk.LEFT, padx=(15, 5))
        ttk.Entry(r1, textvariable=self.prefer_phase, width=15).pack(side=tk.LEFT)
        
        # Separator
        ttk.Separator(config_frame, orient='horizontal').pack(fill=tk.X, pady=10)

        # Strategies Grid
        strat_frame = ttk.Frame(config_frame)
        strat_frame.pack(fill=tk.X, pady=5)
        
        # Col 1: Orari
        c1 = ttk.Frame(strat_frame); c1.pack(side=tk.LEFT, padx=10, fill=tk.Y)
        ttk.Label(c1, text="Rigidità Oraria", style="Group.TLabel").pack(anchor="w")
        ttk.Combobox(c1, textvariable=self.hourly_strategy_var, values=self.hourly_options, state="readonly", width=25).pack(anchor="w", pady=2)
        
        # Col 2: Copertura
        c2 = ttk.Frame(strat_frame); c2.pack(side=tk.LEFT, padx=10, fill=tk.Y)
        ttk.Label(c2, text="Strategia Copertura", style="Group.TLabel").pack(anchor="w")
        ttk.Combobox(c2, textvariable=self.coverage_strategy_var, values=self.coverage_options, state="readonly", width=25).pack(anchor="w", pady=2)
        ttk.Label(c2, text="Attenzione: Bilanciamento puo' creare overstaff", font=("Segoe UI", 8, "italic")).pack(anchor="w", pady=(2, 0))

        # Col 3: Straordinario
        c3 = ttk.Frame(strat_frame); c3.pack(side=tk.LEFT, padx=10, fill=tk.Y)
        ttk.Label(c3, text="Politica Straordinario", style="Group.TLabel").pack(anchor="w")
        ttk.Combobox(c3, textvariable=self.ot_strategy_var, values=self.ot_options, state="readonly", width=25).pack(anchor="w", pady=2)

        # Weekend guard + max overcoverage
        weekend_frame = ttk.Frame(config_frame)
        weekend_frame.pack(fill=tk.X, pady=5)
        ttk.Checkbutton(
            weekend_frame,
            text="Limita overstaff weekend / solo turni con domanda utile",
            variable=self.weekend_guard_var,
        ).pack(side=tk.LEFT)
        ttk.Label(weekend_frame, text="Max overcoverage weekend (%):").pack(side=tk.LEFT, padx=(10, 5))
        tk.Scale(
            weekend_frame,
            from_=0,
            to=100,
            orient=tk.HORIZONTAL,
            resolution=1,
            showvalue=True,
            length=160,
            variable=self.weekend_overcap_pct,
        ).pack(side=tk.LEFT)
        ttk.Label(weekend_frame, text="0 = automatico", font=("Segoe UI", 8, "italic")).pack(side=tk.LEFT, padx=(5, 0))

        uniform_frame = ttk.Frame(config_frame)
        uniform_frame.pack(fill=tk.X, pady=5)
        ttk.Checkbutton(
            uniform_frame,
            text="Overstaff uniforme (proporzionale alla domanda)",
            variable=self.uniform_overcap_var,
        ).pack(side=tk.LEFT)
        ttk.Label(uniform_frame, text="Tolleranza uniformita (%):").pack(side=tk.LEFT, padx=(10, 5))
        tk.Scale(
            uniform_frame,
            from_=0,
            to=50,
            orient=tk.HORIZONTAL,
            resolution=1,
            showvalue=True,
            length=140,
            variable=self.uniform_overcap_tol_pct,
        ).pack(side=tk.LEFT)
        ttk.Label(uniform_frame, text="0 = default", font=("Segoe UI", 8, "italic")).pack(side=tk.LEFT, padx=(5, 0))

        # Separator
        ttk.Separator(config_frame, orient='horizontal').pack(fill=tk.X, pady=10)

        # Guida rapida
        guide_frame = ttk.LabelFrame(container, text="Guida rapida (consigli)", padding="10")
        guide_frame.pack(fill=tk.X, pady=(0, 10))
        quick_text = (
            "Suggerimenti rapidi:\n"
            "A) Efficienza (meno overstaff): Flessibile + Efficienza + Al Bisogno, Weekend guard ON (0-10%), Overcap Normale/Rigida.\n"
            "B) Giorni minimi (contratto): Flessibile + Bilanciamento, Overcap Blanda, Weekend guard ON (10-30%).\n"
            "C) Orari puliti: Rigida/Obbligata, ma rischio gap e meno copertura.\n"
            "D) Overstaff uniforme: Bilanciamento + Overcap Blanda, Weekend guard ON, attiva 'Overstaff uniforme'.\n"
            "Override giornalieri: % Overcap in decimali (0.10=10%). Penalita 1=standard, >1 piu rigido.\n"
            "Nota: se la domanda e bassa rispetto alle risorse, in Efficienza alcuni non raggiungono i giorni minimi."
        )
        ttk.Label(guide_frame, text=quick_text, wraplength=680, justify=tk.LEFT).pack(anchor="w")

        # Row 3: Predefined & Skill & Overcap
        r3 = ttk.Frame(config_frame)
        r3.pack(fill=tk.X, pady=5)

        # Predefined Checkbox
        ttk.Checkbutton(r3, text="Usa Turni Predefiniti (da foglio 'Turni')", variable=self.use_predefined).pack(side=tk.LEFT, padx=(0, 20))

        # Setup Overcap Rigidity
        ttk.Label(r3, text="Rigidità Overcap:", font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT, padx=(20, 5))
        ttk.Combobox(r3, textvariable=self.rigidity_var, values=list(self.rigidity_map.keys()), state="readonly", width=20).pack(side=tk.LEFT)


        # --- Skill selection ---
        skill_frame = ttk.LabelFrame(container, text="Skill da elaborare", padding="10")
        skill_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(
            skill_frame,
            text="Seleziona una o piu skill. Nessuna selezione = tutte.",
            font=("Segoe UI", 8, "italic"),
        ).pack(anchor="w")
        self.skill_listbox = tk.Listbox(
            skill_frame,
            selectmode=tk.EXTENDED,
            height=4,
            exportselection=False,
        )
        self.skill_listbox.pack(fill=tk.X, pady=4)
        self.skill_status = ttk.Label(skill_frame, text="Skill: (non caricate)")
        self.skill_status.pack(anchor="w")

        skill_btns = ttk.Frame(skill_frame)
        skill_btns.pack(fill=tk.X, pady=(4, 0))
        ttk.Button(skill_btns, text="Aggiorna elenco skill", command=self.refresh_skills).pack(side=tk.LEFT)
        ttk.Button(skill_btns, text="Seleziona tutte", command=self.select_all_skills).pack(side=tk.LEFT, padx=5)
        ttk.Button(skill_btns, text="Svuota selezione", command=self.clear_skill_selection).pack(side=tk.LEFT)

        # --- 3. Override Giornalieri ---
        over_frame = ttk.LabelFrame(container, text="3. Override Giornalieri (Avanzato)", padding="10")
        over_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(over_frame, text="Giorno", style="Header.TLabel").grid(row=0, column=0, padx=5, pady=2)
        ttk.Label(over_frame, text="% Overcap (es. 0.1)", style="Header.TLabel").grid(row=0, column=1, padx=5, pady=2)
        ttk.Label(over_frame, text="Penalità Specifica", style="Header.TLabel").grid(row=0, column=2, padx=5, pady=2)

        for i, day in enumerate(self.days):
            r = i + 1
            ttk.Label(over_frame, text=day).grid(row=r, column=0, padx=5, pady=2)
            ttk.Entry(over_frame, textvariable=self.overcap_vars[day], width=10).grid(row=r, column=1, padx=5, pady=2)
            ttk.Entry(over_frame, textvariable=self.penalty_vars[day], width=10).grid(row=r, column=2, padx=5, pady=2)

        ttk.Label(
            over_frame,
            text="* I giorni senza valore useranno i default delle strategie selezionate sopra.",
            font=("Segoe UI", 8, "italic"),
        ).grid(row=8, column=0, columnspan=3, pady=5)
        ttk.Label(
            over_frame,
            text="Override: % Overcap in decimali (0.10=10%). Penalita: 1.0 standard, >1 piu rigida, <1 piu permissiva. Vuoto = default.",
            font=("Segoe UI", 8, "italic"),
            wraplength=620,
            justify=tk.LEFT,
        ).grid(row=9, column=0, columnspan=3, pady=(0, 5))

        # --- 4. Comandi ---
        btn_frame = ttk.Frame(container)
        btn_frame.pack(fill=tk.X, pady=(0, 10))

        self.help_btn = ttk.Button(btn_frame, text="❓ GUIDA E ISTRUZIONI", command=self.show_guide)
        self.help_btn.pack(side=tk.LEFT, padx=5)

        self.merge_btn = ttk.Button(btn_frame, text="UNISCI OUTPUT", command=self.merge_outputs)
        self.merge_btn.pack(side=tk.LEFT, padx=5)

        self.run_btn = ttk.Button(btn_frame, text="▶ AVVIA ELABORAZIONE", command=self.run_process)
        self.run_btn.pack(side=tk.RIGHT, padx=5, fill=tk.X, expand=True)

        # --- 5. Log ---
        log_frame = ttk.LabelFrame(container, text="Log Esecuzione", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True)
        self.log_text = scrolledtext.ScrolledText(log_frame, state='disabled', height=8, font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def browse_input(self):
        f = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx *.xlsm")])
        if f:
            self.input_path.set(f)
            self.refresh_skills()

    def browse_output(self):
        f = filedialog.asksaveasfilename(defaultextension=".xlsx", initialfile="risultato_turni.xlsx", filetypes=[("Excel Files", "*.xlsx")])
        if f: self.output_path.set(f)

    def refresh_skills(self):
        path = self.input_path.get().strip()
        if not path or not os.path.exists(path):
            self.skills_loaded = []
            if hasattr(self, "skill_listbox"):
                self.skill_listbox.delete(0, tk.END)
            if hasattr(self, "skill_status"):
                self.skill_status.config(text="Skill: (nessuna trovata)")
            return

        try:
            xls = pd.ExcelFile(path)
        except Exception as exc:
            messagebox.showerror("Errore", f"Impossibile leggere il file input:\\n{exc}")
            self.skills_loaded = []
            if hasattr(self, "skill_listbox"):
                self.skill_listbox.delete(0, tk.END)
            if hasattr(self, "skill_status"):
                self.skill_status.config(text="Skill: (nessuna trovata)")
            return

        skills = []
        generic = False
        for name in xls.sheet_names:
            lowered = name.strip().lower()
            if lowered == "requisiti":
                generic = True
                continue
            suffix = None
            if lowered.startswith("requisiti"):
                suffix = name[len("requisiti"):]
            elif lowered.startswith("requisit"):
                suffix = name[len("requisit"):]
            if suffix is not None:
                suffix_clean = suffix.lstrip(" _-").strip()
                if suffix_clean:
                    skills.append(suffix_clean)

        unique = []
        seen = set()
        for s in skills:
            key = s.lower()
            if key not in seen:
                seen.add(key)
                unique.append(s)

        if not unique and generic:
            unique = ["Generica"]

        self.skills_loaded = unique
        if hasattr(self, "skill_listbox"):
            self.skill_listbox.delete(0, tk.END)
            for s in unique:
                self.skill_listbox.insert(tk.END, s)
        if hasattr(self, "skill_status"):
            if unique:
                self.skill_status.config(text=f"Skill: {', '.join(unique)}")
            else:
                self.skill_status.config(text="Skill: (nessuna trovata)")

    def select_all_skills(self):
        if hasattr(self, "skill_listbox") and self.skill_listbox.size() > 0:
            self.skill_listbox.selection_set(0, tk.END)

    def clear_skill_selection(self):
        if hasattr(self, "skill_listbox"):
            self.skill_listbox.selection_clear(0, tk.END)

    def get_selected_skills(self):
        if not hasattr(self, "skill_listbox"):
            return []
        return [self.skill_listbox.get(i) for i in self.skill_listbox.curselection()]

    def _build_output_with_skill(self, base_path, skill_name):
        root, ext = os.path.splitext(base_path)
        if not ext:
            ext = ".xlsx"
        safe = re.sub(r"[^A-Za-z0-9_-]+", "_", skill_name).strip("_")
        if not safe:
            safe = "skill"
        return f"{root}_{safe}{ext}"

    def _build_command(self, output_path, skill_name=None):
        args = []
        args.extend(["--input", self.input_path.get()])
        args.extend(["--out", output_path])
        args.extend(["--grid", str(self.grid_val.get())])

        prefer_val = self.prefer_phase.get().strip()
        if prefer_val:
            args.extend(["--prefer_phase", prefer_val])

        h_strat = self.hourly_strategy_var.get()
        if "Strict Phase" in h_strat:
            args.append("--strict-phase")
        elif "Force Phase" in h_strat:
            args.append("--force_phase")

        c_strat = self.coverage_strategy_var.get()
        if "Bilanciamento" in c_strat:
            args.append("--force-balance")

        ot_strat = self.ot_strategy_var.get()
        if "Massimizza" in ot_strat:
            args.append("--force-ot")

        if self.use_predefined.get():
            args.append("--use-predefined")

        if skill_name:
            args.extend(["--skill", skill_name])

        if self.weekend_guard_var.get():
            args.append("--weekend-guard")
            max_weekend_pct = int(self.weekend_overcap_pct.get() or 0)
            if max_weekend_pct > 0:
                args.extend(["--weekend-overcap-max", str(max_weekend_pct)])

        if self.uniform_overcap_var.get():
            args.append("--uniform-overcap")
            tol_pct = int(self.uniform_overcap_tol_pct.get() or 0)
            if tol_pct > 0:
                args.extend(["--uniform-overcap-tol", str(tol_pct)])

        overcap_parts = []
        penalty_parts = []
        default_penalty = self.rigidity_map.get(self.rigidity_var.get(), 10.0)

        for day in self.days:
            val = self.overcap_vars[day].get().strip()
            if val:
                overcap_parts.append(f"{day}={val}")

            p_val = self.penalty_vars[day].get().strip()
            if p_val:
                penalty_parts.append(f"{day}={p_val}")
            else:
                penalty_parts.append(f"{day}={default_penalty}")

        if overcap_parts:
            args.extend(["--overcap", ",".join(overcap_parts)])

        if penalty_parts:
            args.extend(["--overcap-penalty", ",".join(penalty_parts)])

        return args

    def log(self, msg):
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, msg)
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')

    def show_guide(self):
        top = tk.Toplevel(self.root)
        top.title("Guida WFM - Strategie")
        top.geometry("650x650")
        
        txt = scrolledtext.ScrolledText(top, wrap=tk.WORD, font=("Segoe UI", 10), padx=10, pady=10)
        txt.pack(fill=tk.BOTH, expand=True)
        
        guide_content = """
GUIDA ALLE STRATEGIE WFM

1. RIGIDITÀ ORARIA (Dropdown)
   Configura quanto rigorosamente rispettare i "Minuti Inizio Preferiti" (es. :15, :45).
   
   - Flessibile (Default):
     Il sistema prova ad assegnare i turni ai minuti preferiti. Se però la domanda richiede un orario diverso (es. :00), fa un'eccezione pur di coprire il fabbisogno.
     (Consigliato per la maggior parte dei casi).

   - Rigida (Strict Phase):
     "O minuti giusti, o niente". Se la domanda richiede un turno alle :00 ma io voglio solo :15, il turno NON viene assegnato.
     Usa questo se la pulizia degli orari è più importante della copertura perfetta.

   - Obbligata (Force Phase):
     Simile a Rigida, ma forza direttamente la generazione dei soli turni candidati con quegli orari. 

2. STRATEGIA COPERTURA (Dropdown)
   
   - Efficienza (Default):
     Assegna turni solo se c'è domanda da coprire. Rischia di far lavorare meno giorni del previsto alcune persone se c'è poco lavoro.
   
   - Bilanciamento (Force Balance):
     Cerca di garantire a tutti il numero minimo di giorni lavorativi contrattuali, anche se questo significa metterli in turno quando c'è già abbastanza gente (crea leggera overcoverage/esubero pur di farli lavorare).

3. POLITICA STRAORDINARIO (Dropdown)

   - Al Bisogno (Default):
     Usa lo straordinario (ore extra) SOLO se mancano persone per coprire il fabbisogno.
   
   - Massimizza (Usa TUTTO): 
     Assegna tutto lo straordinario contrattualmente possibile, anche se la fascia oraria è già coperta. Utile per massimizzare le ore pagate.

4. RIGIDITÀ OVERCAP (Dropdown)
   
   - Blanda: Tollera molto personale in più (Penalità bassa).
   - Normale: Standard.
   - Rigida: Evita assolutamente personale in più (Penalità alta).

5. OVERRIDE GIORNALIERI
   Serve per personalizzare la tolleranza di overstaff su singoli giorni.
   Campi:
   - % Overcap (es. 0.10) = quanto extra e consentito oltre la domanda su quel giorno.
     0.00 = nessun extra, 0.05 = +5%, 0.30 = +30%. Usa sempre il formato decimale.
     Se lasci vuoto, usa i valori standard (in base alla strategia).
   - Penalita Specifica = moltiplicatore della penalita per overstaff.
     1.0 = standard, 2.0 = piu rigido, 0.5 = piu permissivo.
     Se lasci vuoto, usa la Rigidita Overcap scelta sopra (Blanda/Normale/Rigida).
   Esempi pratici:
   - Dom: % Overcap = 0.00 e Penalita = 3.0 per evitare overstaff domenica.
   - Sab: % Overcap = 0.05 e Penalita = 1.5 per un margine piccolo.
   - Lun-Ven: lascia vuoto per usare il preset generale.
   Nota: in Bilanciamento il sistema puo assegnare turni extra per rispettare i giorni minimi,
   quindi l'override guida il punteggio ma non e un blocco rigido.

6. TURNI PREDEFINITI
   Se spunti "Usa Turni Predefiniti", lo script ignora la griglia e le fasi e usa solo gli orari scritti manualmente nel foglio 'Turni' del file Excel input.

7. LIMITA OVERSTAFF WEEKEND
   - Checkbox: blocca turni che non coprono domanda e frena overstaff su Sab/Dom.
   - Slider: max overcoverage weekend in percento (0 = automatico).
   - Se vuoi garantire giorni minimi, aumenta il limite (es. 20-30) o disattiva la spunta.

8. PARAMETRI BASE
   - Griglia minuti: passo di generazione dei turni. 15 = quarti d'ora, 30 = meno opzioni ma piu semplice.
   - Minuti inizio preferiti: lista separata da virgola (es. 15,45). Influisce sul punteggio, non sul vincolo se Flessibile.
   - Selezione skill: usa la lista "Skill da elaborare". Nessuna selezione = tutte.

9. SCENARI CONSIGLIATI (ESEMPI)
   - Obiettivo: minimo overstaff -> Flessibile + Efficienza + Al Bisogno, Weekend guard ON, Overcap Normale/Rigida.
   - Obiettivo: rispettare giorni minimi -> Flessibile + Bilanciamento + Overcap Blanda, Weekend guard ON (10-30%).
   - Obiettivo: orari puliti -> Rigida/Obbligata, ma aumenta il rischio di gap.
   - Obiettivo: overstaff uniforme -> Bilanciamento + Overcap Blanda + Overstaff uniforme ON.

10. OVERSTAFF UNIFORME
   Distribuisce l'overstaff in modo proporzionale alla domanda di ogni giorno (rapporto copertura/domanda simile).
   Se hai overcapacity alta, evita che un solo giorno si carichi tutto l'extra.
   - Tolleranza uniformita (%): quanto puoi scostarti dal target. 10 = +/-10% di scostamento.
   - 0 = default (usa un valore automatico).
   Nota: funziona meglio con Bilanciamento attivo, altrimenti l'extra non viene assegnato.
   Nota 2: e una logica per giorno, non per singola fascia oraria.
   Distribuzione dentro il giorno: l'extra viene appoggiato sulle fasce con domanda
   (seguendo la curva), non e uniforme per ora.

11. COPERTURA PER FASCIA ORARIA (CURVA)
   Il planner lavora a slot orari (griglia) presi dal foglio Requisiti.
   - I turni vengono scelti per coprire gli slot con domanda > 0.
   - In Efficienza, i turni che includono slot a domanda 0 vengono scartati.
   - In Bilanciamento, il vincolo puo essere allentato per rispettare i giorni minimi.
   - Se non ci sono gap, l'algoritmo preferisce le fasce gia meno sovracoperte.
   - Coverage enforcement tenta di evitare fasce con copertura 0, ma solo se restano giorni disponibili.

12. SELEZIONE SKILL (LISTA)
   - Dopo aver scelto l'input, usa "Aggiorna elenco skill" per caricare le skill presenti.
   - Seleziona una o piu skill: "Avvia elaborazione" le esegue in sequenza con i settaggi attuali.
   - Nessuna selezione = tutte le skill.
   - Se selezioni piu skill, il sistema crea output separati con suffisso e puoi unirli con "Unisci Output".

NOTE IMPORTANTI
   - Se la domanda e bassa rispetto alle risorse, in Efficienza e normale vedere violazioni dei giorni minimi.
   - Se la domenica resta scoperta, verifica forced_off e le fasce orarie disponibili in Risorse.
        """
        txt.insert(tk.END, guide_content.strip())
        txt.config(state='disabled')

    def merge_outputs(self):
        files = filedialog.askopenfilenames(
            title="Seleziona file output da unire",
            filetypes=[("Excel Files", "*.xlsx")],
        )
        if not files or len(files) < 2:
            return

        out = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile="risultato_unificato.xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
        )
        if not out:
            return

        sheet_names = ["Pianificazione", "Assegnazioni", "Copertura", "Warnings"]
        merged = {}
        try:
            for sheet in sheet_names:
                parts = []
                for path in files:
                    try:
                        df = pd.read_excel(path, sheet_name=sheet)
                    except ValueError:
                        continue
                    if df is None or df.empty:
                        continue
                    if "Skill" not in df.columns:
                        skill_name = os.path.splitext(os.path.basename(path))[0]
                        df = df.copy()
                        df.insert(0, "Skill", skill_name)
                    parts.append(df)
                if parts:
                    combined = pd.concat(parts, ignore_index=True)
                    cols = list(combined.columns)
                    if "Skill" in cols:
                        cols.insert(0, cols.pop(cols.index("Skill")))
                        combined = combined[cols]
                    merged[sheet] = combined

            if not merged:
                messagebox.showwarning("Attenzione", "Nessun foglio compatibile trovato nei file selezionati.")
                return

            with pd.ExcelWriter(out) as writer:
                for sheet, df in merged.items():
                    df.to_excel(writer, sheet_name=sheet, index=False)

            messagebox.showinfo("Completato", f"File unificato salvato:\\n{out}")
        except Exception as exc:
            messagebox.showerror("Errore merge", f"Errore durante l'unione:\\n{exc}")

    def run_batch(self, cfg_path=None):
        messagebox.showinfo(
            "Info",
            "La modalita batch non e disponibile. Usa la selezione skill nella schermata principale.",
        )
        return
        if not self.input_path.get():
            messagebox.showerror("Errore", "Seleziona un file di Input!")
            return

        if cfg_path is None:
            cfg_path = filedialog.askopenfilename(
                title="Seleziona batch per skill (.csv o .xlsx)",
                filetypes=[("CSV/Excel", "*.csv *.xlsx *.xlsm")],
            )
            if not cfg_path:
                return

        try:
            ext = os.path.splitext(cfg_path)[1].lower()
            if ext in (".xlsx", ".xlsm"):
                df = pd.read_excel(cfg_path)
            else:
                df = pd.read_csv(cfg_path, sep=None, engine="python")
        except Exception as exc:
            messagebox.showerror("Errore", f"Impossibile leggere il file batch:\\n{exc}")
            return

        if df is None or df.empty:
            messagebox.showwarning("Attenzione", "Il file batch e vuoto.")
            return

        col_map = {str(c).strip().lower(): c for c in df.columns}

        def _col(*names):
            for name in names:
                key = str(name).strip().lower()
                if key in col_map:
                    return col_map[key]
            return None

        skill_col = _col("skill", "skills")
        if not skill_col:
            messagebox.showerror("Errore", "Colonna obbligatoria mancante: skill")
            return

        def _to_str(val):
            if val is None:
                return None
            if isinstance(val, float) and pd.isna(val):
                return None
            s = str(val).strip()
            return s or None

        def _to_bool(val):
            if val is None:
                return None
            if isinstance(val, float) and pd.isna(val):
                return None
            if isinstance(val, bool):
                return val
            s = str(val).strip().lower()
            if not s:
                return None
            return s in ("1", "true", "yes", "y", "si", "on")

        def _to_int(val):
            if val is None:
                return None
            if isinstance(val, float) and pd.isna(val):
                return None
            try:
                return int(float(val))
            except (TypeError, ValueError):
                return None

        def _parse_hourly(val, default_mode):
            if val is None:
                return default_mode
            if isinstance(val, bool):
                return default_mode
            s = str(val).strip().lower()
            if not s:
                return default_mode
            if "rigida" in s or "strict" in s:
                return "strict"
            if "obbligata" in s or "force" in s:
                return "force"
            return "flex"

        def _parse_coverage(val, default_force):
            if val is None:
                return default_force
            if isinstance(val, bool):
                return val
            s = str(val).strip().lower()
            if not s:
                return default_force
            if "bilanci" in s or "force" in s:
                return True
            return False

        def _parse_ot(val, default_force):
            if val is None:
                return default_force
            if isinstance(val, bool):
                return val
            s = str(val).strip().lower()
            if not s:
                return default_force
            if "massim" in s or "tutto" in s:
                return True
            return False

        def _parse_preset(val, default_label):
            if val is None:
                return default_label
            s = str(val).strip().lower()
            if not s:
                return default_label
            if "blanda" in s:
                return "Blanda (Tollera eccessi)"
            if "rigida" in s:
                return "Rigida (Evita eccessi)"
            if "normale" in s:
                return "Normale"
            return default_label

        def _build_output(base_out, skill_name):
            safe = re.sub(r"[^A-Za-z0-9]+", "_", skill_name).strip("_") or "skill"
            root, ext = os.path.splitext(base_out)
            if not ext:
                ext = ".xlsx"
            return f"{root}_{safe}{ext}"

        base_h_strat = self.hourly_strategy_var.get()
        base_c_strat = self.coverage_strategy_var.get()
        base_ot_strat = self.ot_strategy_var.get()
        base_force_balance = "Bilanciamento" in base_c_strat
        base_force_ot = "Massimizza" in base_ot_strat
        base_hourly_mode = "flex"
        if "Strict Phase" in base_h_strat:
            base_hourly_mode = "strict"
        elif "Force Phase" in base_h_strat:
            base_hourly_mode = "force"

        base_overcap_parts = []
        base_penalty_parts = []
        base_default_penalty = self.rigidity_map.get(self.rigidity_var.get(), 10.0)
        for day in self.days:
            val = self.overcap_vars[day].get().strip()
            if val:
                base_overcap_parts.append(f"{day}={val}")
            p_val = self.penalty_vars[day].get().strip()
            if p_val:
                base_penalty_parts.append(f"{day}={p_val}")
            else:
                base_penalty_parts.append(f"{day}={base_default_penalty}")
        base_overcap_arg = ",".join(base_overcap_parts) if base_overcap_parts else None
        base_penalty_arg = ",".join(base_penalty_parts) if base_penalty_parts else None

        output_col = _col("output", "out", "file")
        grid_col = _col("grid")
        prefer_col = _col("prefer_phase", "prefer", "phase")
        hourly_col = _col("hourly_strategy", "hourly")
        coverage_col = _col("coverage_strategy", "coverage")
        ot_col = _col("ot_strategy", "ot")
        force_balance_col = _col("force_balance")
        use_predef_col = _col("use_predefined", "predefined")
        weekend_guard_col = _col("weekend_guard")
        weekend_overcap_col = _col("weekend_overcap_max", "weekend_overcap")
        uniform_overcap_col = _col("uniform_overcap")
        uniform_tol_col = _col("uniform_overcap_tol", "uniform_tol")
        overcap_col = _col("overcap", "overcap_percent")
        penalty_col = _col("overcap_penalty", "penalty")
        preset_col = _col("overcap_preset", "preset")

        tasks = []
        for _, row in df.iterrows():
            skill_val = _to_str(row.get(skill_col))
            if not skill_val:
                continue

            out_val = _to_str(row.get(output_col)) if output_col else None
            if not out_val:
                out_val = _build_output(self.output_path.get(), skill_val)

            grid_val = _to_int(row.get(grid_col)) if grid_col else None
            if grid_val is None:
                grid_val = int(self.grid_val.get())

            prefer_val = _to_str(row.get(prefer_col)) if prefer_col else None
            if prefer_val is None:
                prefer_val = self.prefer_phase.get().strip()

            hourly_mode = _parse_hourly(_to_str(row.get(hourly_col)) if hourly_col else None, base_hourly_mode)
            force_balance = _parse_coverage(_to_str(row.get(coverage_col)) if coverage_col else None, base_force_balance)
            if force_balance_col:
                force_override = _to_bool(row.get(force_balance_col))
                if force_override is not None:
                    force_balance = force_override
            force_ot = _parse_ot(_to_str(row.get(ot_col)) if ot_col else None, base_force_ot)

            use_predefined = self.use_predefined.get()
            if use_predef_col:
                pref_val = _to_bool(row.get(use_predef_col))
                if pref_val is not None:
                    use_predefined = pref_val

            weekend_guard = self.weekend_guard_var.get()
            if weekend_guard_col:
                pref_val = _to_bool(row.get(weekend_guard_col))
                if pref_val is not None:
                    weekend_guard = pref_val

            weekend_overcap = int(self.weekend_overcap_pct.get() or 0)
            if weekend_overcap_col:
                val = _to_int(row.get(weekend_overcap_col))
                if val is not None:
                    weekend_overcap = val

            uniform_overcap = self.uniform_overcap_var.get()
            if uniform_overcap_col:
                pref_val = _to_bool(row.get(uniform_overcap_col))
                if pref_val is not None:
                    uniform_overcap = pref_val

            uniform_tol = int(self.uniform_overcap_tol_pct.get() or 0)
            if uniform_tol_col:
                val = _to_int(row.get(uniform_tol_col))
                if val is not None:
                    uniform_tol = val

            overcap_arg = base_overcap_arg
            if overcap_col:
                val = row.get(overcap_col)
                val_str = _to_str(val)
                if val_str:
                    if "=" in val_str:
                        overcap_arg = val_str
                    else:
                        try:
                            num = float(val_str)
                            overcap_arg = ",".join([f"{d}={num}" for d in self.days])
                        except ValueError:
                            pass

            penalty_arg = base_penalty_arg
            preset_label = _parse_preset(_to_str(row.get(preset_col)) if preset_col else None, self.rigidity_var.get())
            if penalty_col:
                val = row.get(penalty_col)
                val_str = _to_str(val)
                if val_str:
                    if "=" in val_str:
                        penalty_arg = val_str
                    else:
                        try:
                            num = float(val_str)
                            penalty_arg = ",".join([f"{d}={num}" for d in self.days])
                        except ValueError:
                            pass
            elif preset_col:
                default_penalty = self.rigidity_map.get(preset_label, 10.0)
                penalty_arg = ",".join([f"{d}={default_penalty}" for d in self.days])

            cmd = ["python", "wfm_claudegitmultiskill3.py"]
            cmd.extend(["--input", self.input_path.get()])
            cmd.extend(["--out", out_val])
            cmd.extend(["--grid", str(grid_val)])

            if prefer_val:
                cmd.extend(["--prefer_phase", prefer_val])

            if hourly_mode == "strict":
                cmd.append("--strict-phase")
            elif hourly_mode == "force":
                cmd.append("--force_phase")

            if force_balance:
                cmd.append("--force-balance")

            if force_ot:
                cmd.append("--force-ot")

            if use_predefined:
                cmd.append("--use-predefined")

            if skill_val:
                cmd.extend(["--skill", skill_val])

            if weekend_guard:
                cmd.append("--weekend-guard")
                if weekend_overcap > 0:
                    cmd.extend(["--weekend-overcap-max", str(weekend_overcap)])

            if uniform_overcap:
                cmd.append("--uniform-overcap")
                if uniform_tol > 0:
                    cmd.extend(["--uniform-overcap-tol", str(uniform_tol)])

            if overcap_arg:
                cmd.extend(["--overcap", overcap_arg])
            if penalty_arg:
                cmd.extend(["--overcap-penalty", penalty_arg])

            tasks.append((skill_val, out_val, cmd))

        if not tasks:
            messagebox.showwarning("Attenzione", "Nessuna riga valida nel file batch.")
            return

        self.run_btn.config(state="disabled")
        self.batch_btn.config(state="disabled")
        self.merge_btn.config(state="disabled")
        if hasattr(self, "batch_manual_btn"):
            self.batch_manual_btn.config(state="disabled")

        self.log("\n" + "=" * 60 + "\n")
        self.log(f"BATCH PER SKILL: {len(tasks)} run in sequenza\n")
        self.log("-" * 60 + "\n")

        def runner():
            try:
                cwd = os.getcwd()
                for skill_name, out_name, cmd in tasks:
                    header = f"\n[BATCH] Skill: {skill_name} -> {os.path.basename(out_name)}\n"
                    self.root.after(0, self.log, header)
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1,
                        cwd=cwd,
                    )

                    for line in iter(process.stdout.readline, ""):
                        self.root.after(0, self.log, line)

                    process.stdout.close()
                    rc = process.wait()
                    self.root.after(0, self.log, f"\n[BATCH] Skill {skill_name} terminata (codice {rc})\n")
                    if rc != 0:
                        break
            except Exception as exc:
                self.root.after(0, self.log, f"\nERRORE BATCH: {exc}\n")
            finally:
                self.root.after(0, lambda: self.run_btn.config(state="normal"))
                self.root.after(0, lambda: self.batch_btn.config(state="normal"))
                self.root.after(0, lambda: self.merge_btn.config(state="normal"))
                if hasattr(self, "batch_manual_btn"):
                    self.root.after(0, lambda: self.batch_manual_btn.config(state="normal"))

        threading.Thread(target=runner, daemon=True).start()

    def run_batch_manual(self):
        messagebox.showinfo(
            "Info",
            "La modalita batch non e disponibile. Usa la selezione skill nella schermata principale.",
        )
        return
        if not self.input_path.get():
            messagebox.showerror("Errore", "Seleziona un file di Input!")
            return

        top = tk.Toplevel(self.root)
        top.title("Batch per skill (manuale)")
        top.geometry("780x700")

        outer = ttk.Frame(top)
        outer.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(outer, highlightthickness=0)
        vscroll = ttk.Scrollbar(outer, orient=tk.VERTICAL, command=canvas.yview)
        canvas.configure(yscrollcommand=vscroll.set)

        vscroll.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        body = ttk.Frame(canvas, padding="10")
        body_id = canvas.create_window((0, 0), window=body, anchor="nw")

        def _on_frame_configure(_event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _on_canvas_configure(event):
            canvas.itemconfigure(body_id, width=event.width)

        body.bind("<Configure>", _on_frame_configure)
        canvas.bind("<Configure>", _on_canvas_configure)

        rows = []

        def _bool_choice(value):
            s = str(value).strip().lower()
            if s in ("on", "1", "true", "yes", "si"):
                return "1"
            if s in ("off", "0", "false", "no"):
                return "0"
            return None

        def _add_row():
            idx = len(rows) + 1
            frame = ttk.LabelFrame(body, text=f"Skill {idx}", padding="8")
            frame.pack(fill=tk.X, pady=6)

            skill_var = tk.StringVar()
            output_var = tk.StringVar()
            coverage_var = tk.StringVar()
            preset_var = tk.StringVar()
            hourly_var = tk.StringVar()
            ot_var = tk.StringVar()
            weekend_guard_var = tk.StringVar(value="Auto")
            weekend_overcap_var = tk.StringVar()
            uniform_overcap_var = tk.StringVar(value="Auto")
            uniform_tol_var = tk.StringVar()
            grid_var = tk.StringVar()
            prefer_var = tk.StringVar()
            use_predef_var = tk.StringVar(value="Auto")

            frame.columnconfigure(1, weight=1)
            frame.columnconfigure(3, weight=1)

            ttk.Label(frame, text="Skill:").grid(row=0, column=0, sticky="w")
            ttk.Entry(frame, textvariable=skill_var, width=12).grid(row=0, column=1, sticky="w", padx=(4, 10))
            ttk.Label(frame, text="Output (opzionale):").grid(row=0, column=2, sticky="w")
            ttk.Entry(frame, textvariable=output_var).grid(row=0, column=3, sticky="ew", padx=(4, 10))

            def _remove():
                frame.destroy()
                rows[:] = [r for r in rows if r["frame"] is not frame]
                for i, r in enumerate(rows, start=1):
                    r["frame"].config(text=f"Skill {i}")

            ttk.Button(frame, text="Rimuovi", command=_remove).grid(row=0, column=4, padx=(4, 0))

            ttk.Label(frame, text="Copertura:").grid(row=1, column=0, sticky="w", pady=(6, 0))
            ttk.Combobox(
                frame,
                textvariable=coverage_var,
                values=["", "Efficienza", "Bilanciamento"],
                width=14,
                state="readonly",
            ).grid(row=1, column=1, sticky="w", pady=(6, 0))
            ttk.Label(frame, text="Overcap preset:").grid(row=1, column=2, sticky="w", pady=(6, 0))
            ttk.Combobox(
                frame,
                textvariable=preset_var,
                values=["", "Blanda", "Normale", "Rigida"],
                width=14,
                state="readonly",
            ).grid(row=1, column=3, sticky="w", pady=(6, 0))

            ttk.Label(frame, text="Rigidita oraria:").grid(row=2, column=0, sticky="w")
            ttk.Combobox(
                frame,
                textvariable=hourly_var,
                values=["", "Flessibile", "Rigida", "Obbligata"],
                width=14,
                state="readonly",
            ).grid(row=2, column=1, sticky="w")
            ttk.Label(frame, text="Straordinario:").grid(row=2, column=2, sticky="w")
            ttk.Combobox(
                frame,
                textvariable=ot_var,
                values=["", "Al Bisogno", "Massimizza"],
                width=14,
                state="readonly",
            ).grid(row=2, column=3, sticky="w")

            ttk.Label(frame, text="Weekend guard:").grid(row=3, column=0, sticky="w")
            ttk.Combobox(
                frame,
                textvariable=weekend_guard_var,
                values=["Auto", "On", "Off"],
                width=10,
                state="readonly",
            ).grid(row=3, column=1, sticky="w")
            ttk.Label(frame, text="Weekend overcap %:").grid(row=3, column=2, sticky="w")
            ttk.Entry(frame, textvariable=weekend_overcap_var, width=10).grid(row=3, column=3, sticky="w")

            ttk.Label(frame, text="Uniform overcap:").grid(row=4, column=0, sticky="w")
            ttk.Combobox(
                frame,
                textvariable=uniform_overcap_var,
                values=["Auto", "On", "Off"],
                width=10,
                state="readonly",
            ).grid(row=4, column=1, sticky="w")
            ttk.Label(frame, text="Uniform tol %:").grid(row=4, column=2, sticky="w")
            ttk.Entry(frame, textvariable=uniform_tol_var, width=10).grid(row=4, column=3, sticky="w")

            ttk.Label(frame, text="Grid:").grid(row=5, column=0, sticky="w")
            ttk.Entry(frame, textvariable=grid_var, width=10).grid(row=5, column=1, sticky="w")
            ttk.Label(frame, text="Prefer phase:").grid(row=5, column=2, sticky="w")
            ttk.Entry(frame, textvariable=prefer_var, width=12).grid(row=5, column=3, sticky="w")

            ttk.Label(frame, text="Usa turni predefiniti:").grid(row=6, column=0, sticky="w")
            ttk.Combobox(
                frame,
                textvariable=use_predef_var,
                values=["Auto", "On", "Off"],
                width=10,
                state="readonly",
            ).grid(row=6, column=1, sticky="w")

            rows.append({
                "frame": frame,
                "skill": skill_var,
                "output": output_var,
                "coverage": coverage_var,
                "preset": preset_var,
                "hourly": hourly_var,
                "ot": ot_var,
                "weekend_guard": weekend_guard_var,
                "weekend_overcap": weekend_overcap_var,
                "uniform_overcap": uniform_overcap_var,
                "uniform_tol": uniform_tol_var,
                "grid": grid_var,
                "prefer": prefer_var,
                "use_predefined": use_predef_var,
            })

        def _run():
            data = []
            for r in rows:
                skill_val = r["skill"].get().strip()
                if not skill_val:
                    continue
                row = {"skill": skill_val}
                if r["output"].get().strip():
                    row["output"] = r["output"].get().strip()
                if r["coverage"].get().strip():
                    row["coverage_strategy"] = r["coverage"].get().strip()
                if r["preset"].get().strip():
                    row["overcap_preset"] = r["preset"].get().strip()
                if r["hourly"].get().strip():
                    row["hourly_strategy"] = r["hourly"].get().strip()
                if r["ot"].get().strip():
                    row["ot_strategy"] = r["ot"].get().strip()
                if r["grid"].get().strip():
                    row["grid"] = r["grid"].get().strip()
                if r["prefer"].get().strip():
                    row["prefer_phase"] = r["prefer"].get().strip()

                w_guard = _bool_choice(r["weekend_guard"].get())
                if w_guard is not None:
                    row["weekend_guard"] = w_guard
                if r["weekend_overcap"].get().strip():
                    row["weekend_overcap_max"] = r["weekend_overcap"].get().strip()

                u_guard = _bool_choice(r["uniform_overcap"].get())
                if u_guard is not None:
                    row["uniform_overcap"] = u_guard
                if r["uniform_tol"].get().strip():
                    row["uniform_overcap_tol"] = r["uniform_tol"].get().strip()

                use_predef = _bool_choice(r["use_predefined"].get())
                if use_predef is not None:
                    row["use_predefined"] = use_predef

                data.append(row)

            if not data:
                messagebox.showwarning("Attenzione", "Inserisci almeno una skill valida.")
                return

            df = pd.DataFrame(data)
            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as tmp:
                    df.to_csv(tmp.name, index=False)
                    tmp_path = tmp.name
                self.run_batch(cfg_path=tmp_path)
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
            top.destroy()

        controls = ttk.Frame(top, padding="8")
        controls.pack(fill=tk.X)
        ttk.Button(controls, text="Aggiungi skill", command=_add_row).pack(side=tk.LEFT)
        ttk.Button(controls, text="Esegui batch", command=_run).pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(controls, text="Chiudi", command=top.destroy).pack(side=tk.RIGHT, padx=(5, 0))

        _add_row()

    def run_process(self):
        if not self.input_path.get():
            messagebox.showerror("Errore", "Seleziona un file di Input!")
            return

        base_output = self.output_path.get().strip()
        if not base_output:
            base_output = "risultato_turni.xlsx"
        root, ext = os.path.splitext(base_output)
        if not ext:
            base_output = f"{base_output}.xlsx"
        self.output_path.set(base_output)

        selected = [s.strip() for s in self.get_selected_skills() if s.strip()]
        tasks = []

        def add_task(label, out_path, skill_arg=None):
            tasks.append((label, out_path, self._build_command(out_path, skill_arg)))

        if not selected:
            add_task("ALL", base_output)
        else:
            cleaned = [s for s in selected if s.lower() != "generica"]
            if not cleaned:
                add_task("Generica", base_output)
            elif len(cleaned) == 1 and len(selected) == 1:
                add_task(cleaned[0], base_output, cleaned[0])
            else:
                if len(cleaned) != len(selected):
                    messagebox.showwarning(
                        "Attenzione",
                        "La skill 'Generica' non puo essere combinata con altre: verra ignorata.",
                    )
                for skill_name in cleaned:
                    out_path = self._build_output_with_skill(base_output, skill_name)
                    add_task(skill_name, out_path, skill_name)

        if not tasks:
            messagebox.showwarning("Attenzione", "Nessuna skill selezionata.")
            return

        h_strat = self.hourly_strategy_var.get()
        c_strat = self.coverage_strategy_var.get()
        ot_strat = self.ot_strategy_var.get()

        self.log("\n" + "=" * 60 + "\n")
        if len(tasks) == 1:
            skill_info = f"Skill: {tasks[0][0]}"
        else:
            skill_info = "Skill: " + ", ".join([t[0] for t in tasks])
        self.log(f"AVVIO ELABORAZIONE... ({skill_info})\n")
        self.log(f"Strategie: {h_strat} | {c_strat} | {ot_strat}\n")
        self.log("Comando parziale: wfm (motore interno) ...\n")
        self.log("-" * 60 + "\n")
        self.run_btn.config(state="disabled")
        self.merge_btn.config(state="disabled")

        def runner():
            class _GuiStream:
                def __init__(self, callback):
                    self.callback = callback
                    self.buffer = ""

                def write(self, data):
                    if not data:
                        return
                    self.buffer += data
                    while "\n" in self.buffer:
                        line, self.buffer = self.buffer.split("\n", 1)
                        self.callback(line + "\n")

                def flush(self):
                    if self.buffer:
                        self.callback(self.buffer)
                        self.buffer = ""

            success = True
            try:
                for idx, (label, out_path, args) in enumerate(tasks, start=1):
                    header = f"\n[RUN {idx}/{len(tasks)}] Skill: {label} -> {os.path.basename(out_path)}\n"
                    self.root.after(0, self.log, header)

                    stream = _GuiStream(lambda text: self.root.after(0, self.log, text))
                    old_stdout, old_stderr = sys.stdout, sys.stderr
                    sys.stdout = stream
                    sys.stderr = stream
                    try:
                        rc = engine.main(args)
                        if not isinstance(rc, int):
                            rc = 0
                    except SystemExit as exc:
                        rc = exc.code if isinstance(exc.code, int) else 1
                    except Exception as exc:
                        rc = 1
                        self.root.after(0, self.log, f"\nERRORE CRITICO: {exc}\n")
                    finally:
                        stream.flush()
                        sys.stdout = old_stdout
                        sys.stderr = old_stderr

                    self.root.after(0, self.log, f"\n[RUN {idx}/{len(tasks)}] Terminato (codice {rc})\n")
                    if rc != 0:
                        success = False
                        break

                if success:
                    msg = "Elaborazione terminata con successo!"
                    if len(tasks) > 1:
                        msg += "\nOutput separati creati. Usa 'UNISCI OUTPUT' per combinarli."
                    self.root.after(0, lambda: messagebox.showinfo("Completato", msg))
                else:
                    self.root.after(
                        0,
                        lambda: messagebox.showerror(
                            "Errore",
                            "L'elaborazione ha riportato degli errori. Controlla il log.",
                        ),
                    )
            except Exception as exc:
                self.root.after(0, self.log, f"\nERRORE CRITICO: {exc}\n")
                self.root.after(
                    0,
                    lambda: messagebox.showerror(
                        "Errore",
                        "Errore critico durante l'elaborazione. Controlla il log.",
                    ),
                )
            finally:
                self.root.after(0, lambda: self.run_btn.config(state="normal"))
                self.root.after(0, lambda: self.merge_btn.config(state="normal"))

        threading.Thread(target=runner, daemon=True).start()

if __name__ == "__main__":
    root = tk.Tk()
    app = WFMLauncher(root)
    root.mainloop()
