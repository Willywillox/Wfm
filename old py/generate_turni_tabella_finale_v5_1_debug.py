"""
generate_turni_tabella_finale_v5_1.py

Versione con diversificazione bilanciata dei minuti di inizio turno.
- DIVERSIFICAZIONE MINUTI: Bilancia automaticamente tra :00, :15, :30, :45
- NO TURNI PRIMA DELLA DOMANDA: Blocca turni che iniziano prima dell'orario di domanda
- FIX DOMENICA: Non assegna turni quando domanda = 0
- Ottimizzato per capacità limitata (<5% overcapacity)

Uso:
  python generate_turni_tabella_finale_v5_1.py --input "input.xlsm" --out "output.xlsx" --grid 15
  
  Opzioni:
  --balanced : attiva bilanciamento automatico minuti (default: True)
  --prefer_phase "15,45" : se vuoi comunque preferire certi minuti
  --force_phase : forza SOLO i minuti specificati (disabilita bilanciamento)
"""

import argparse
import unicodedata
from pathlib import Path
import re
import datetime as _dt
from collections import defaultdict
import math
import pandas as pd
import numpy as np


# ----------------------------- Util -----------------------------
def _to_minutes(x) -> int:
    if pd.isna(x):
        return 24 * 60
    if isinstance(x, _dt.time):
        return x.hour * 60 + x.minute
    if isinstance(x, pd.Timestamp):
        return x.hour * 60 + x.minute
    s = str(x).strip()
    m = re.match(r"^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$", s)
    if m:
        return int(m.group(1))*60 + int(m.group(2))
    return 24 * 60


def _from_minutes(m: int) -> str:
    m = int(m) % (24*60)
    h = m // 60
    mm = m % 60
    return f"{h:02d}:{mm:02d}"


def _find_col(df: pd.DataFrame, name: str):
    target = name.strip().lower()
    for c in df.columns:
        if str(c).strip().lower() == target:
            return c
    return None


def _parse_hours_cell(val) -> float:
    if pd.isna(val):
        return 4.0
    if isinstance(val, (int, float)):
        return float(val) if float(val) > 0 else 4.0
    s = str(val).strip().lower()
    m = re.search(r'(\d+[.,]?\d*)', s)
    if not m:
        return 4.0
    num = m.group(1).replace(',', '.')
    try:
        v = float(num)
        return v if v > 0 else 4.0
    except Exception:
        return 4.0


def _parse_rest_count_cell(val) -> int:
    if pd.isna(val):
        return 2
    if isinstance(val, (int, float)):
        n = int(round(val))
        return int(max(0, min(7, n)))
    s = str(val).strip().lower()
    m = re.search(r'(\d+)', s)
    if not m:
        return 2
    n = int(m.group(1))
    return int(max(0, min(7, n)))


def _normalize(s: str) -> str:
    s = str(s or '').strip().lower()
    s = ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))
    s = re.sub(r'[^a-z]', '', s)
    return s


DAY_SYNONYMS = {
    'Lun': ['lun', 'lunedi', 'lunedì', 'mon', 'monday'],
    'Mar': ['mar', 'martedi', 'martedì', 'tue', 'tuesday'],
    'Mer': ['mer', 'mercoledi', 'mercoledì', 'wed', 'wednesday'],
    'Gio': ['gio', 'giovedi', 'giovedì', 'thu', 'thursday'],
    'Ven': ['ven', 'venerdi', 'venerdì', 'fri', 'friday'],
    'Sab': ['sab', 'sabato', 'sat', 'saturday'],
    'Dom': ['dom', 'domenica', 'sun', 'sunday'],
}


def map_req_day_columns(req: pd.DataFrame):
    norm_cols = { _normalize(c): c for c in req.columns }
    mapping = {}
    for short, syns in DAY_SYNONYMS.items():
        found = None
        for syn in syns:
            if syn in norm_cols:
                found = norm_cols[syn]
                break
        if found is None and short.lower() in norm_cols:
            found = norm_cols[short.lower()]
        mapping[short] = found
    return mapping


# ----------------------------- I/O -----------------------------
def carica_dati(percorso_input: str):
    p = Path(percorso_input).expanduser()
    if not p.is_absolute():
        p = Path.cwd() / p
    if not p.exists():
        raise FileNotFoundError(f"File non trovato: {p}")
    if p.is_dir():
        raise IsADirectoryError(f"Il percorso è una CARTELLA, non un file: {p}")
    xls = pd.ExcelFile(p, engine='openpyxl')
    req = pd.read_excel(xls, 'Requisiti')
    turni = pd.read_excel(xls, 'Turni')  # opzionale
    ris = pd.read_excel(xls, 'Risorse')
    return req, turni, ris


# ----------------------------- Prep -----------------------------
def prepara_req(req: pd.DataFrame) -> pd.DataFrame:
    q = req.copy()
    fasce = q['fasce'].astype(str).str.strip()
    q['fascia_inizio_str'] = fasce.str[:5]
    q['fascia_fine_str'] = fasce.str[-5:]
    q['start_min'] = q['fascia_inizio_str'].apply(_to_minutes)
    q['end_min'] = q['fascia_fine_str'].apply(_to_minutes)
    return q


def infer_personal_params_from_risorse(ris: pd.DataFrame):
    if ris.shape[1] < 4:
        raise ValueError("Il foglio 'Risorse' deve avere almeno 4 colonne: A,B,C(ore/giorno),D(riposi/settimana).")
    hours_col = ris.columns[2]  # C
    rests_col = ris.columns[3]  # D
    durations_by_emp = {}
    rest_target_by_emp = {}
    for _, row in ris.iterrows():
        emp = row['id dipendente']
        ore = _parse_hours_cell(row[hours_col])
        durations_by_emp[emp] = int(round(ore * 60))
        rest_target_by_emp[emp] = _parse_rest_count_cell(row[rests_col])
    return durations_by_emp, rest_target_by_emp


def genera_turni_candidati(req_pre: pd.DataFrame, durations_set_min: set, grid_step_min: int, 
                          force_phase_minutes=None) -> pd.DataFrame:
    """
    Genera turni candidati con supporto per forzare specifici minuti di inizio.
    """
    min_start = int(req_pre['start_min'].min())
    max_end = int(req_pre['end_min'].max())
    rows = []
    
    for dmin in sorted(durations_set_min):
        start = min_start
        while start + dmin <= max_end:
            # Se force_phase_minutes è specificato, genera solo turni con quei minuti
            if force_phase_minutes is not None:
                start_minute = (start % 60)
                if start_minute not in force_phase_minutes:
                    start += grid_step_min
                    continue
            
            end = start + dmin
            rows.append({
                'id turno': f"AUTO_{_from_minutes(start)}-{_from_minutes(end)}_{dmin}",
                'entrata_str': _from_minutes(start),
                'uscita_str': _from_minutes(end),
                'start_min': start,
                'end_min': end,
                'durata_min': int(dmin),
            })
            start += grid_step_min
    
    return pd.DataFrame(rows)


def determina_turni_ammissibili(ris: pd.DataFrame, turni_cand: pd.DataFrame, durations_by_emp: dict):
    fin_col = _find_col(ris, 'Fine fascia')
    ini_col = _find_col(ris, 'Inizio fascia')
    if fin_col is None:
        raise ValueError("Nel foglio 'Risorse' manca la colonna 'Fine fascia'.")
    avail_ini_min = {
        row['id dipendente']: (_to_minutes(row[ini_col]) if ini_col is not None else 0)
        for _, row in ris.iterrows()
    }
    avail_end_min = {
        row['id dipendente']: _to_minutes(row[fin_col])
        for _, row in ris.iterrows()
    }
    shift_by_emp = {}
    for emp in ris['id dipendente']:
        smin = avail_ini_min.get(emp, 0)
        emin = avail_end_min.get(emp, 24*60)
        d_req = durations_by_emp.get(emp, 240)
        shift_by_emp[emp] = [
            row['id turno']
            for _, row in turni_cand.iterrows()
            if row['start_min'] >= smin and row['end_min'] <= emin and int(row['durata_min']) == int(d_req)
        ]
    return shift_by_emp


def compute_slot_size(req_pre: pd.DataFrame) -> int:
    if len(req_pre) >= 2:
        diffs = sorted(set(int(b) - int(a) for a, b in zip(req_pre['start_min'][:-1], req_pre['start_min'][1:]) if b > a))
        if diffs:
            return diffs[0]
    return int(req_pre['end_min'].iloc[0] - req_pre['start_min'].iloc[0]) if len(req_pre) > 0 else 15


# ----------------------------- Weekend constraints -----------------------------
def _parse_flag(val: str) -> str:
    if pd.isna(val):
        return ''
    s = str(val).strip().lower()
    if s in {'riposo', 'off', 'no', '0', 'r'}:
        return 'off'
    if s in {'lavoro', 'on', 'si', 'sì', '1', 'work', 'w'}:
        return 'on'
    return ''


def leggi_vincoli_weekend(ris: pd.DataFrame):
    forced_off = defaultdict(set)
    forced_on = defaultdict(set)
    sab_col = _find_col(ris, 'sabato')
    dom_col = _find_col(ris, 'domenica')

    for _, row in ris.iterrows():
        emp = row['id dipendente']
        if sab_col is not None:
            f = _parse_flag(row[sab_col])
            if f == 'off':
                forced_off[emp].add('Sab')
            elif f == 'on':
                forced_on[emp].add('Sab')
        if dom_col is not None:
            f = _parse_flag(row[dom_col])
            if f == 'off':
                forced_off[emp].add('Dom')
            elif f == 'on':
                forced_on[emp].add('Dom')
    return forced_off, forced_on


# ----------------------------- Algoritmo ottimizzato v5.1 -----------------------------
def compute_tight_capacity_targets(req_pre: pd.DataFrame, ris: pd.DataFrame, durations_by_emp: dict,
                                   rest_target_by_emp: dict, forced_off: dict):
    """
    Calcola target precisi per capacità limitata.
    """
    giorni = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom']
    day_colmap = map_req_day_columns(req_pre)
    slot_list = req_pre['start_min'].tolist()
    slot_size = compute_slot_size(req_pre)
    
    # Calcola la domanda esatta E l'intervallo di domanda
    demand_by_day = {}
    demand_by_slot = {g: {} for g in giorni}
    total_demand = 0.0
    zero_demand_days = set()
    min_demand_start = {}
    max_demand_end = {}
    
    for g in giorni:
        col = day_colmap.get(g)
        if col and col in req_pre.columns:
            day_demand = float(req_pre[col].sum())
            demand_by_day[g] = day_demand
            total_demand += day_demand
            
            if day_demand == 0:
                zero_demand_days.add(g)
                min_demand_start[g] = 24*60
                max_demand_end[g] = 0
            else:
                # Trova prima e ultima fascia con domanda
                fasce_con_domanda = req_pre[req_pre[col] > 0]
                if not fasce_con_domanda.empty:
                    min_demand_start[g] = int(fasce_con_domanda['start_min'].min())
                    max_demand_end[g] = int(fasce_con_domanda['end_min'].max())
                else:
                    min_demand_start[g] = 24*60
                    max_demand_end[g] = 0
            
            for i, slot in enumerate(slot_list):
                demand_by_slot[g][slot] = float(req_pre.loc[i, col])
        else:
            demand_by_day[g] = 0.0
            zero_demand_days.add(g)
            min_demand_start[g] = 24*60
            max_demand_end[g] = 0
            for slot in slot_list:
                demand_by_slot[g][slot] = 0.0
    
    # Calcola la capacità disponibile esatta
    total_capacity_slots = 0.0
    emp_work_days = {}
    for emp in ris['id dipendente']:
        D = int(rest_target_by_emp.get(emp, 2))
        forced = len(forced_off.get(emp, set()))
        work_days = max(0, 7 - max(D, forced))
        emp_work_days[emp] = work_days
        slots_per_shift = durations_by_emp.get(emp, 240) / slot_size
        total_capacity_slots += work_days * slots_per_shift
    
    # Calcola overcapacity ratio
    overcapacity_ratio = (total_capacity_slots - total_demand) / total_demand if total_demand > 0 else 0.0
    
    # Se overcapacity < 5%, usa target più stretti
    if overcapacity_ratio < 0.05:
        day_targets = {}
        for g in giorni:
            if g in zero_demand_days:
                day_targets[g] = 0
            elif total_demand > 0:
                day_targets[g] = int(round(total_capacity_slots * (demand_by_day[g] / total_demand)))
            else:
                day_targets[g] = 0
    else:
        day_targets = None
    
    return {
        'demand_by_day': demand_by_day,
        'demand_by_slot': demand_by_slot,
        'total_demand': total_demand,
        'total_capacity': total_capacity_slots,
        'overcapacity_ratio': overcapacity_ratio,
        'day_targets': day_targets,
        'emp_work_days': emp_work_days,
        'slot_list': slot_list,
        'slot_size': slot_size,
        'day_colmap': day_colmap,
        'zero_demand_days': zero_demand_days,
        'min_demand_start': min_demand_start,
        'max_demand_end': max_demand_end
    }


def assegnazione_tight_capacity(req: pd.DataFrame, turni_cand: pd.DataFrame, ris: pd.DataFrame,
                                shift_by_emp: dict, rest_target_by_emp: dict, durations_by_emp: dict,
                                forced_off: dict, forced_on: dict, prefer_phases=(15,45), balanced=True):
    """
    Algoritmo ottimizzato v5.1 con bilanciamento automatico minuti.
    """
    giorni = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom']
    
    # Calcola i target
    targets = compute_tight_capacity_targets(req, ris, durations_by_emp, rest_target_by_emp, forced_off)
    
    demand_by_slot = targets['demand_by_slot']
    demand_by_day = targets['demand_by_day']
    total_demand = targets['total_demand']
    total_capacity = targets['total_capacity']
    overcapacity_ratio = targets['overcapacity_ratio']
    day_targets = targets['day_targets']
    emp_work_days = targets['emp_work_days']
    slot_list = targets['slot_list']
    slot_size = targets['slot_size']
    day_colmap = targets['day_colmap']
    zero_demand_days = targets['zero_demand_days']
    min_demand_start = targets['min_demand_start']
    max_demand_end = targets['max_demand_end']
    
    # FIX DOMENICA: Aggiungi giorni con domanda zero a forced_off per TUTTI
    for emp in ris['id dipendente']:
        for day in zero_demand_days:
            forced_off[emp].add(day)
    
    print(f"DEBUG: Overcapacity ratio = {overcapacity_ratio:.1%}")
    print(f"DEBUG: Giorni con domanda zero: {zero_demand_days}")
    print(f"DEBUG: Modalità bilanciata: {balanced}")
    if day_targets:
        print(f"DEBUG: Using strict day targets: {day_targets}")
    
    # Debug intervalli domanda
    print("\nDEBUG: Intervalli domanda per giorno:")
    for g in giorni:
        if g not in zero_demand_days:
            print(f"  {g}: {_from_minutes(min_demand_start[g])} - {_from_minutes(max_demand_end[g])}")
    
    # work_need per persona
    work_need = {}
    infeasible = []
    for emp in ris['id dipendente']:
        work_need[emp] = emp_work_days[emp]
        zero_days_count = len(zero_demand_days)
        if work_need[emp] > (7 - zero_days_count):
            work_need[emp] = 7 - zero_days_count
    
    # Precompute shift -> slots
    shift_start_min = dict(zip(turni_cand['id turno'], turni_cand['start_min']))
    shift_end_min = dict(zip(turni_cand['id turno'], turni_cand['end_min']))
    shift_slots = {
        row['id turno']: [s for s in slot_list if row['start_min'] <= s < row['end_min']]
        for _, row in turni_cand.iterrows()
    }
    shift_len_slots = {sid: len(sl) for sid, sl in shift_slots.items()}
    
    # Tracciamento stato
    days_done = {emp: 0 for emp in ris['id dipendente']}
    assigned_once = set()
    assignments = []
    current_coverage = {g: {s: 0 for s in slot_list} for g in giorni}
    day_assignments_count = {g: 0 for g in giorni}
    
    # NUOVO: Tracciamento distribuzione minuti per bilanciamento
    minute_distribution = defaultdict(int)
    
    def fase_score_balanced(start_min: int) -> float:
        """
        v5.1: Score bilanciato per diversificare i minuti di inizio.
        """
        minutes = start_min % 60
        
        # Minuti validi per la diversificazione
        valid_minutes = [0, 15, 30, 45]
        
        if minutes not in valid_minutes:
            return -100.0  # Penalità forte per minuti non standard
        
        if not balanced:
            # Se non bilanciato, usa preferenze classiche
            if minutes in prefer_phases:
                return 20.0
            else:
                return -10.0
        
        # BILANCIAMENTO AUTOMATICO
        total_assigned = sum(minute_distribution.values())
        if total_assigned == 0:
            return 10.0  # All'inizio, tutti i minuti validi sono buoni
        
        # Calcola distribuzione attuale
        counts = {m: minute_distribution.get(m, 0) for m in valid_minutes}
        
        # Target: distribuzione uniforme (25% ciascuno)
        target_count = total_assigned / 4
        
        # Il minuto con meno assegnazioni ottiene il bonus maggiore
        min_count = min(counts.values())
        max_count = max(counts.values())
        current_count = counts.get(minutes, 0)
        
        if current_count == min_count:
            # Questo minuto è sotto-rappresentato: bonus forte
            return 30.0
        elif current_count == max_count and (max_count - min_count) > 5:
            # Questo minuto è sovra-rappresentato: penalità
            return -20.0 * (current_count - target_count) / max(1, target_count)
        else:
            # Situazione intermedia: bonus leggero
            deviation = abs(current_count - target_count)
            return 10.0 - deviation * 2
    
    def shift_value_strict(day, sid):
        """
        Scoring v5.1 con bilanciamento minuti e fix domanda.
        """
        # FIX DOMENICA: blocca giorni senza domanda
        if day in zero_demand_days:
            return -10000.0
        
        # FIX DOMANDA v5.1: Il turno NON può iniziare prima della domanda
        st = shift_start_min[sid]
        en = shift_end_min[sid]
        mds = min_demand_start.get(day, 24*60)
        mde = max_demand_end.get(day, 0)
        
        # Se il turno inizia prima dell'inizio domanda, BLOCCA
        if mds < 24*60 and st < mds:
            # Tolleranza massima di 15 minuti
            if st < mds - 15:
                return -10000.0  # BLOCCO TOTALE
            else:
                # Penalità forte ma non bloccante per 0-15 minuti prima
                return -500.0
        
        # Se il turno termina prima dell'inizio domanda, è completamente inutile
        if en <= mds:
            return -10000.0
        
        score = 0.0
        
        # Se abbiamo day_targets e siamo già sopra target, penalità
        if day_targets and day_assignments_count[day] >= day_targets[day]:
            return -1000.0
        
        # Valuta copertura slot
        for slot in shift_slots.get(sid, []):
            demand = demand_by_slot[day].get(slot, 0.0)
            current = current_coverage[day][slot]
            
            if demand > 0:
                if current < demand:
                    uncovered = demand - current
                    score += min(1.0, uncovered) * 100.0
                elif overcapacity_ratio < 0.05:
                    if current >= demand:
                        overcov_ratio = (current / demand) - 1.0
                        score -= overcov_ratio * 50.0
                else:
                    if current > demand * 1.2:
                        score -= ((current / demand) - 1.2) * 20.0
            else:
                score -= 100.0
        
        # Applica score bilanciato per minuti
        phase_score = fase_score_balanced(st)
        score += phase_score
        
        # Bilanciamento giornaliero
        if total_demand > 0:
            day_proportion = demand_by_day[day] / total_demand
            current_proportion = day_assignments_count[day] / max(1, sum(day_assignments_count.values()))
            balance_diff = abs(day_proportion - current_proportion)
            score -= balance_diff * 30.0
        
        return score
    
    # 1) Assegna FORCED_ON
    for day in giorni:
        if day in zero_demand_days:
            continue
        
        must = [emp for emp in ris['id dipendente'] if day in forced_on.get(emp, set()) and days_done[emp] < work_need[emp]]
        for emp in sorted(must, key=lambda e: days_done[e]):
            if (emp, day) in assigned_once:
                continue
            best = None; best_sid = None
            for sid in shift_by_emp.get(emp, []):
                value = shift_value_strict(day, sid)
                key = (value, -shift_start_min[sid])
                if (best is None) or (key > best):
                    best = key; best_sid = sid
            if best_sid is None:
                infeasible.append((emp, f"Nessun turno compatibile in {day} (FORCED_ON)"))
                continue
            assignments.append((emp, day, best_sid))
            days_done[emp] += 1
            assigned_once.add((emp, day))
            day_assignments_count[day] += 1
            minute_distribution[shift_start_min[best_sid] % 60] += 1
            for s in shift_slots[best_sid]:
                current_coverage[day][s] += 1
    
    # 2) Assegnazione principale
    def remaining_people():
        return [e for e in ris['id dipendente'] if days_done[e] < work_need[e]]
    
    giorni_validi = [g for g in giorni if g not in zero_demand_days]
    
    safety = 0
    while remaining_people() and safety < 10000:
        safety += 1
        
        # Trova gap critici
        critical_gaps = []
        for day in giorni_validi:
            if day_targets and day_assignments_count[day] >= day_targets[day]:
                continue
                
            for slot in slot_list:
                demand = demand_by_slot[day].get(slot, 0.0)
                current = current_coverage[day][slot]
                if demand > 0 and current < demand:
                    gap = demand - current
                    critical_gaps.append((gap, day, slot))
        
        if not critical_gaps:
            break
        
        critical_gaps.sort(reverse=True)
        
        progressed = False
        for gap_value, day, target_slot in critical_gaps[:10]:
            cands = []
            for emp in remaining_people():
                if (emp, day) in assigned_once or day in forced_off.get(emp, set()):
                    continue
                for sid in shift_by_emp.get(emp, []):
                    if target_slot in shift_slots.get(sid, []):
                        # Verifica che il turno sia valido (non prima della domanda)
                        if shift_value_strict(day, sid) > -1000:
                            cands.append((emp, sid))
                            break
            
            if not cands:
                continue
            
            # Scegli miglior candidato
            best = None; best_emp = None; best_sid = None
            for emp, sid in cands:
                value = shift_value_strict(day, sid)
                need_priority = work_need[emp] - days_done[emp]
                key = (value, need_priority)
                if (best is None) or (key > best):
                    best = key; best_emp = emp; best_sid = sid
            
            if best_emp and best[0] > -100:
                assignments.append((best_emp, day, best_sid))
                days_done[best_emp] += 1
                assigned_once.add((best_emp, day))
                day_assignments_count[day] += 1
                minute_distribution[shift_start_min[best_sid] % 60] += 1
                for s in shift_slots[best_sid]:
                    current_coverage[day][s] += 1
                progressed = True
                break
        
        if not progressed:
            # Approccio bilanciato
            best_overall = None; best_emp = None; best_day = None; best_sid = None
            
            for emp in remaining_people()[:10]:
                for day in giorni_validi:
                    if (emp, day) in assigned_once or day in forced_off.get(emp, set()):
                        continue
                    if day_targets and day_assignments_count[day] >= day_targets[day]:
                        continue
                    
                    # Ordina i turni per score
                    shift_scores = []
                    for sid in shift_by_emp.get(emp, []):
                        value = shift_value_strict(day, sid)
                        if value > -1000:  # Solo turni validi
                            shift_scores.append((value, sid))
                    
                    if shift_scores:
                        shift_scores.sort(reverse=True)
                        best_shift = shift_scores[0]
                        need = work_need[emp] - days_done[emp]
                        key = (best_shift[0], need)
                        if (best_overall is None) or (key > best_overall):
                            best_overall = key
                            best_emp = emp
                            best_day = day
                            best_sid = best_shift[1]
            
            if best_emp and best_overall[0] > -500:
                assignments.append((best_emp, best_day, best_sid))
                days_done[best_emp] += 1
                assigned_once.add((best_emp, best_day))
                day_assignments_count[best_day] += 1
                minute_distribution[shift_start_min[best_sid] % 60] += 1
                for s in shift_slots[best_sid]:
                    current_coverage[best_day][s] += 1
            else:
                # Ultima risorsa
                for emp in remaining_people()[:1]:
                    for day in sorted(giorni_validi, key=lambda g: day_assignments_count[g]):
                        if (emp, day) not in assigned_once and day not in forced_off.get(emp, set()):
                            # Trova turno valido con bilanciamento minuti
                            valid_shifts = []
                            for sid in shift_by_emp.get(emp, []):
                                st = shift_start_min[sid]
                                mds = min_demand_start.get(day, 24*60)
                                if st >= mds - 15:  # Turno valido
                                    phase_score = fase_score_balanced(st)
                                    valid_shifts.append((phase_score, st, sid))
                            
                            if valid_shifts:
                                valid_shifts.sort(reverse=True)
                                best_sid = valid_shifts[0][2]
                                assignments.append((emp, day, best_sid))
                                days_done[emp] += 1
                                assigned_once.add((emp, day))
                                day_assignments_count[day] += 1
                                minute_distribution[shift_start_min[best_sid] % 60] += 1
                                for s in shift_slots[best_sid]:
                                    current_coverage[day][s] += 1
                                break
                    break
                break
    
    # 3) Completa assegnazioni mancanti
    if overcapacity_ratio < 0.05 and remaining_people():
        for emp in remaining_people():
            days_by_coverage = sorted(giorni_validi, key=lambda g: day_assignments_count[g])
            for day in days_by_coverage:
                if (emp, day) not in assigned_once and day not in forced_off.get(emp, set()):
                    # Scegli turno valido con bilanciamento
                    valid_shifts = []
                    for sid in shift_by_emp.get(emp, []):
                        st = shift_start_min[sid]
                        mds = min_demand_start.get(day, 24*60)
                        if st >= mds - 15:  # Turno valido
                            phase_score = fase_score_balanced(st)
                            valid_shifts.append((phase_score, st, sid))
                    
                    if valid_shifts:
                        valid_shifts.sort(reverse=True)
                        best_sid = valid_shifts[0][2]
                        assignments.append((emp, day, best_sid))
                        days_done[emp] += 1
                        assigned_once.add((emp, day))
                        day_assignments_count[day] += 1
                        minute_distribution[shift_start_min[best_sid] % 60] += 1
                        for s in shift_slots[best_sid]:
                            current_coverage[day][s] += 1
                        if days_done[emp] >= work_need[emp]:
                            break
    
    # Riepilogo
    riposi_info = []
    for emp in ris['id dipendente']:
        D = int(rest_target_by_emp.get(emp, 2))
        actual_work = days_done[emp]
        actual_rest = 7 - actual_work
        dev = actual_rest - D
        riposi_info.append((emp, D, actual_rest, dev))
    
    # Debug output
    print(f"\nDEBUG: Assegnazioni finali per giorno:")
    for g in giorni:
        if g not in zero_demand_days and demand_by_day[g] > 0:
            expected_proportion = demand_by_day[g] / total_demand if total_demand > 0 else 0
            actual_proportion = day_assignments_count[g] / max(1, sum(day_assignments_count.values()))
            print(f"  {g}: {day_assignments_count[g]} turni (expected {expected_proportion:.1%}, actual {actual_proportion:.1%})")
    
    # Debug minuti con statistiche di bilanciamento
    print(f"\nDEBUG: Distribuzione minuti di inizio turno:")
    total_assignments = len(assignments)
    valid_minutes = [0, 15, 30, 45]
    for minutes in sorted(minute_distribution.keys()):
        count = minute_distribution[minutes]
        pct = count / total_assignments * 100 if total_assignments else 0
        if minutes in valid_minutes:
            target_pct = 25.0  # Target per distribuzione uniforme
            deviation = abs(pct - target_pct)
            status = "✅" if deviation < 5 else "⚠️" if deviation < 10 else "❌"
            print(f"  :{minutes:02d} -> {count} turni ({pct:.1f}%) [target: {target_pct:.1f}%] {status}")
        else:
            print(f"  :{minutes:02d} -> {count} turni ({pct:.1f}%)")
    
    # Calcola indice di bilanciamento
    if balanced and total_assignments > 0:
        counts = [minute_distribution.get(m, 0) for m in valid_minutes]
        if counts:
            avg_count = sum(counts) / len(counts)
            variance = sum((c - avg_count) ** 2 for c in counts) / len(counts)
            std_dev = math.sqrt(variance)
            balance_index = 100 * (1 - std_dev / max(1, avg_count))
            print(f"\n📊 Indice di bilanciamento minuti: {balance_index:.1f}% (100% = perfettamente bilanciato)")
    
    return assignments, riposi_info, infeasible, day_colmap, slot_list, slot_size, shift_slots


# ----------------------------- Output -----------------------------
def crea_output(assignments, turni_cand: pd.DataFrame, ris: pd.DataFrame,
                req_pre_original: pd.DataFrame, day_colmap, slot_list, slot_size, shift_slots):
    name_map = dict(zip(ris['id dipendente'], ris['Nome']))
    tstart = dict(zip(turni_cand['id turno'], turni_cand['entrata_str']))
    tend = dict(zip(turni_cand['id turno'], turni_cand['uscita_str']))
    tdur = dict(zip(turni_cand['id turno'], turni_cand['durata_min']))
    day_name_map = {'Lun':'Lunedì','Mar':'Martedì','Mer':'Mercoledì','Gio':'Giovedì','Ven':'Venerdì','Sab':'Sabato','Dom':'Domenica'}

    rows = []
    for emp, day, turno in assignments:
        rows.append({
            'emp_id': emp,
            'Nome': name_map.get(emp, emp),
            'Key': f"{name_map.get(emp, emp)} [{emp}]",
            'Giorno': day_name_map[day],
            'Inizio': tstart[turno],
            'Fine': tend[turno],
            'Durata_min': tdur[turno],
            'TurnoID': turno
        })
    df_ass = pd.DataFrame(rows)

    all_days_long = ['Lunedì','Martedì','Mercoledì','Giovedì','Venerdì','Sabato','Domenica']
    pivot_source = df_ass.copy()
    if not pivot_source.empty:
        pivot_source['Fascia'] = pivot_source['Inizio'] + '-' + pivot_source['Fine']
    else:
        pivot_source = pd.DataFrame(columns=['emp_id', 'Nome', 'Key', 'Giorno', 'Fascia'])

    # 'Riposo' dove non assegnato
    extra_rows = []
    all_keys = [f"{name_map.get(emp, emp)} [{emp}]" for emp in ris['id dipendente']]
    key_to_name = {f"{name_map.get(emp, emp)} [{emp}]": name_map.get(emp, emp) for emp in ris['id dipendente']}
    for key in all_keys:
        emp_days = set(pivot_source[pivot_source['Key'] == key]['Giorno'])
        for d in all_days_long:
            if d not in emp_days:
                extra_rows.append({'Key': key, 'Nome': key_to_name[key], 'Giorno': d, 'Fascia': 'Riposo'})
    if extra_rows:
        pivot_source = pd.concat([pivot_source, pd.DataFrame(extra_rows)], ignore_index=True)

    pivot_source = pivot_source.sort_values(['Key','Giorno']).drop_duplicates(['Key','Giorno'], keep='first')
    pivot = pivot_source.pivot(index='Key', columns='Giorno', values='Fascia')
    pivot = pivot[all_days_long]
    pivot.sort_index(inplace=True)

    # Copertura
    giorni_short = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom']
    req_view = req_pre_original.copy()
    if not all(d in req_view.columns for d in giorni_short):
        for d in giorni_short:
            col = day_colmap.get(d)
            if col and col in req_view.columns:
                req_view[d] = req_view[col]
            elif d not in req_view.columns:
                req_view[d] = 0

    req_by_day_orig = {
        day: {slot: float(req_view.loc[i, day]) if day in req_view.columns else 0.0 for i, slot in enumerate(slot_list)}
        for day in giorni_short
    }
    cover = {day: {slot: 0 for slot in slot_list} for day in giorni_short}
    for emp, day, turno in assignments:
        for s in shift_slots.get(turno, []):
            cover[day][s] += 1

    rows_cov = []
    long_map = {'Lun':'Lunedì','Mar':'Martedì','Mer':'Mercoledì','Gio':'Giovedì','Ven':'Venerdì','Sab':'Sabato','Dom':'Domenica'}
    for day in giorni_short:
        for s in slot_list:
            rows_cov.append({
                'Giorno': long_map[day],
                'Fascia': f"{_from_minutes(s)}-{_from_minutes(s + slot_size)}",
                'Richiesta': req_by_day_orig[day][s],
                'Coperta': cover[day][s],
                'Gap': req_by_day_orig[day][s] - cover[day][s]
            })
    df_cov = pd.DataFrame(rows_cov)

    return pivot, df_ass, df_cov


# ----------------------------- CLI -----------------------------
def main():
    parser = argparse.ArgumentParser(description='Pianificazione v5.1 - Bilanciamento automatico minuti.')
    parser.add_argument('--input', required=True, help='Percorso al file Excel di input (.xlsx/.xlsm)')
    parser.add_argument('--out', default='tabella_turni.xlsx', help='File Excel di output')
    parser.add_argument('--grid', type=int, default=15, help='Griglia minuti per start turno (default 15)')
    parser.add_argument('--prefer_phase', type=str, default="15,45", 
                       help='Minuti preferiti (usato solo se --no-balanced)')
    parser.add_argument('--force_phase', action='store_true', 
                       help='Genera SOLO turni che iniziano ai minuti preferiti')
    parser.add_argument('--no-balanced', dest='balanced', action='store_false',
                       default=True, help='Disabilita bilanciamento automatico minuti')
    args = parser.parse_args()

    req, turni, ris = carica_dati(args.input)
    req_pre = prepara_req(req)

    durations_by_emp, rest_target_by_emp = infer_personal_params_from_risorse(ris)
    durations_set_min = set(durations_by_emp.values())

    # Parse preferred minutes
    prefer_phases = tuple(int(x.strip()) for x in args.prefer_phase.split(',') if x.strip() != '')
    
    # Se force_phase è attivo, genera solo turni con quei minuti
    force_minutes = prefer_phases if args.force_phase else None
    
    if args.force_phase:
        print(f"INFO: FORZANDO generazione solo turni ai minuti: {force_minutes}")
    elif args.balanced:
        print(f"INFO: Modalità BILANCIATA attiva - distribuzione uniforme tra :00, :15, :30, :45")
    else:
        print(f"INFO: Preferenza minuti: {prefer_phases}")
    
    turni_cand = genera_turni_candidati(req_pre, durations_set_min, 
                                       grid_step_min=args.grid,
                                       force_phase_minutes=force_minutes)
    
    print(f"INFO: Generati {len(turni_cand)} turni candidati")
    
    shift_by_emp = determina_turni_ammissibili(ris, turni_cand, durations_by_emp)

    forced_off, forced_on = leggi_vincoli_weekend(ris)

    # USA LA NUOVA FUNZIONE v5.1
    assignments, riposi_info, infeasible, day_colmap, slot_list, slot_size, shift_slots = assegnazione_tight_capacity(
        req_pre.copy(), turni_cand, ris, shift_by_emp, rest_target_by_emp, durations_by_emp,
        forced_off, forced_on, prefer_phases=prefer_phases, balanced=args.balanced
    )

    pivot, df_ass, df_cov = crea_output(assignments, turni_cand, ris, req_pre, day_colmap, slot_list, slot_size, shift_slots)

    # Warnings
    warn_rows = []
    for emp, D, actual_rest, dev in riposi_info:
        note = ""
        if dev != 0:
            note = "ATTENZIONE: deviazione riposi (dovrebbe essere 0)."
        warn_rows.append({
            "emp_id": emp, "TargetRiposi": D, "RiposiEffettivi": actual_rest, "Deviazione": dev, "Note": note
        })
    for emp, msg in infeasible:
        warn_rows.append({"emp_id": emp, "TargetRiposi": "", "RiposiEffettivi": "", "Deviazione": "", "Note": f"Infeasible: {msg}"})

    df_warn = pd.DataFrame(warn_rows)

    out_path = Path(args.out).expanduser()
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_path) as writer:
        pivot.to_excel(writer, sheet_name='Pianificazione')
        df_ass.to_excel(writer, sheet_name='Assegnazioni', index=False)
        df_cov.to_excel(writer, sheet_name='Copertura', index=False)
        df_warn.to_excel(writer, sheet_name='Warnings', index=False)

    print(f'\n✅ Salvato in {out_path}')


if __name__ == '__main__':
    main()