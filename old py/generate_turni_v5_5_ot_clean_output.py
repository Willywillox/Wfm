"""
generate_turni_tabella_finale_v5_0.py

Versione definitiva con TUTTI i fix:
- NO TURNI PRIMA DELLA DOMANDA: Blocca turni che iniziano prima dell'orario di domanda
- MINUTI FORZATI: PenalitÃ  MASSIVE per turni non :15/:45
- FIX DOMENICA: Non assegna turni quando domanda = 0
- Ottimizzato per capacitÃ  limitata (<5% overcapacity)

Uso:
  python generate_turni_tabella_finale_v5_0.py --input "input.xlsm" --out "output.xlsx" --grid 15 --prefer_phase "15,45"
  
  Opzioni:
  --force_phase : genera SOLO turni che iniziano ai minuti specificati
  --strict_phase : penalitÃ  ESTREMA per turni non ai minuti preferiti (consigliato)
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
    'Lun': ['lun', 'lunedi', 'lunedÃ¬', 'mon', 'monday'],
    'Mar': ['mar', 'martedi', 'martedÃ¬', 'tue', 'tuesday'],
    'Mer': ['mer', 'mercoledi', 'mercoledÃ¬', 'wed', 'wednesday'],
    'Gio': ['gio', 'giovedi', 'giovedÃ¬', 'thu', 'thursday'],
    'Ven': ['ven', 'venerdi', 'venerdÃ¬', 'fri', 'friday'],
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
        raise IsADirectoryError(f"Il percorso Ã¨ una CARTELLA, non un file: {p}")
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
    ot_disp_col = _find_col(ris, 'OT disp')
    ot_split_col = _find_col(ris, 'OT spezz')
    ot_day_labels = {
        'Lun': 'OT lun',
        'Mar': 'OT mar',
        'Mer': 'OT mer',
        'Gio': 'OT gio',
        'Ven': 'OT ven',
        'Sab': 'OT sab',
        'Dom': 'OT dom',
    }
    ot_day_cols = {}
    for day, label in ot_day_labels.items():
        col = _find_col(ris, label)
        if col is None:
            col = _find_col(ris, label.replace(' ', '_'))
        if col is None:
            col = _find_col(ris, label.upper())
        ot_day_cols[day] = col

    durations_by_emp = {}
    rest_target_by_emp = {}
    ot_minutes_by_emp = {}
    ot_split_by_emp = {}
    ot_daily_minutes_by_emp = {}

    for _, row in ris.iterrows():
        emp = row['id dipendente']
        ore = _parse_hours_cell(row[hours_col])
        durations_by_emp[emp] = int(round(ore * 60))
        rest_target_by_emp[emp] = _parse_rest_count_cell(row[rests_col])

        raw_ot = row.get(ot_disp_col) if ot_disp_col else None
        ot_minutes = 0
        if ot_disp_col is not None and pd.notna(raw_ot) and str(raw_ot).strip() != '':
            try:
                ot_minutes = int(round(float(str(raw_ot).replace(',', '.')) * 60.0))
            except Exception:
                ot_minutes = 0

        day_minutes_map = {}
        total_day_minutes = 0
        for day, col in ot_day_cols.items():
            val = row.get(col) if col else None
            minutes = 0.0
            if col is not None and pd.notna(val) and str(val).strip() != '':
                try:
                    minutes = int(round(float(str(val).replace(',', '.')) * 60.0))
                except Exception:
                    minutes = 0.0
            minutes = max(0, minutes)
            day_minutes_map[day] = minutes
            total_day_minutes += minutes

        if total_day_minutes > 0:
            ot_minutes_by_emp[emp] = total_day_minutes
        else:
            ot_minutes_by_emp[emp] = max(0.0, ot_minutes)

        ot_daily_minutes_by_emp[emp] = day_minutes_map

        split_val = row.get(ot_split_col) if ot_split_col else None
        split_flag = False
        if ot_split_col is not None and pd.notna(split_val):
            normalized = ''.join(ch for ch in unicodedata.normalize('NFKD', str(split_val).strip().lower()) if not unicodedata.combining(ch))
            split_flag = normalized in {'ok', 'si', 'yes', 'true', '1', 'y'}
        ot_split_by_emp[emp] = split_flag

    return durations_by_emp, rest_target_by_emp, ot_minutes_by_emp, ot_split_by_emp, ot_daily_minutes_by_emp
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
            # Se force_phase_minutes Ã¨ specificato, genera solo turni con quei minuti
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
    if s in {'lavoro', 'on', 'si', 'sÃ¬', '1', 'work', 'w'}:
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


# ----------------------------- Algoritmo ottimizzato v5.0 -----------------------------

def compute_tight_capacity_targets(req_pre: pd.DataFrame, ris: pd.DataFrame, durations_by_emp: dict,
                                   rest_target_by_emp: dict, forced_off: dict):
    """Calcola domanda, disponibilita' e vincoli giornalieri."""
    giorni = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom']
    day_colmap = map_req_day_columns(req_pre)
    slot_list = req_pre['start_min'].tolist()
    slot_size = compute_slot_size(req_pre)

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
                min_demand_start[g] = 24 * 60
                max_demand_end[g] = 0
            else:
                fasce_con_domanda = req_pre[req_pre[col] > 0]
                if not fasce_con_domanda.empty:
                    min_demand_start[g] = int(fasce_con_domanda['start_min'].min())
                    max_demand_end[g] = int(fasce_con_domanda['end_min'].max())
                else:
                    min_demand_start[g] = 24 * 60
                    max_demand_end[g] = 0

            for i, slot in enumerate(slot_list):
                demand_by_slot[g][slot] = float(req_pre.loc[i, col])
        else:
            demand_by_day[g] = 0.0
            zero_demand_days.add(g)
            min_demand_start[g] = 24 * 60
            max_demand_end[g] = 0
            for slot in slot_list:
                demand_by_slot[g][slot] = 0.0

    total_capacity_slots = 0.0
    emp_work_days = {}
    for emp in ris['id dipendente']:
        target_rest = int(rest_target_by_emp.get(emp, 2))
        forced = len(forced_off.get(emp, set()))
        if forced > target_rest:
            raise ValueError(f"Vincoli incoerenti per {emp}: riposi richiesti={target_rest}, ma giorni forzati OFF={forced}.")
        work_days = max(0, 7 - target_rest)
        emp_work_days[emp] = work_days
        slots_per_shift = durations_by_emp.get(emp, 240) / slot_size
        total_capacity_slots += work_days * slots_per_shift

    overcapacity_ratio = (total_capacity_slots - total_demand) / total_demand if total_demand > 0 else 0.0

    day_weights = {}
    weight_tot = sum(max(demand_by_day[g], 0.0) for g in giorni if g not in zero_demand_days)
    if weight_tot <= 0:
        active_days = [g for g in giorni if g not in zero_demand_days] or giorni
        equal = 1.0 / len(active_days)
        for g in giorni:
            day_weights[g] = equal if g in active_days else 0.0
    else:
        for g in giorni:
            day_weights[g] = max(demand_by_day[g], 0.0) / weight_tot if g not in zero_demand_days else 0.0

    return {
        'demand_by_day': demand_by_day,
        'demand_by_slot': demand_by_slot,
        'total_demand': total_demand,
        'total_capacity': total_capacity_slots,
        'overcapacity_ratio': overcapacity_ratio,
        'day_weights': day_weights,
        'emp_work_days': emp_work_days,
        'slot_list': slot_list,
        'slot_size': slot_size,
        'day_colmap': day_colmap,
        'zero_demand_days': zero_demand_days,
        'min_demand_start': min_demand_start,
        'max_demand_end': max_demand_end
    }




def assegnazione_tight_capacity(
    req: pd.DataFrame,
    turni_cand: pd.DataFrame,
    ris: pd.DataFrame,
    shift_by_emp: dict,
    rest_target_by_emp: dict,
    durations_by_emp: dict,
    forced_off: dict,
    forced_on: dict,
    ot_minutes_by_emp: dict,
    ot_split_by_emp: dict,
    ot_daily_minutes_by_emp: dict,
    allow_ot_overcap: bool = False,
    prefer_phases=(15,
    45),
    strict_phase=False
):
    '''Assegna i turni rispettando domanda, riposi contrattuali e privilegiando i minuti preferiti.'''
    giorni = ['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom']

    targets = compute_tight_capacity_targets(req, ris, durations_by_emp, rest_target_by_emp, forced_off)
    demand_by_slot = targets['demand_by_slot']
    demand_by_day = targets['demand_by_day']
    total_demand = targets['total_demand']
    overcapacity_ratio = targets['overcapacity_ratio']
    day_weights = targets['day_weights']
    emp_work_days = targets['emp_work_days']
    slot_list = targets['slot_list']
    slot_size = targets['slot_size']
    day_colmap = targets['day_colmap']
    zero_demand_days = targets['zero_demand_days']
    min_demand_start = targets['min_demand_start']
    max_demand_end = targets['max_demand_end']

    slot_coverage = max(1, int(round(next(iter(durations_by_emp.values())) / slot_size)))
    dom_demand = demand_by_day.get('Dom', 0.0)
    if dom_demand > 0:
        dom_needed = int((dom_demand + slot_coverage - 1) // slot_coverage)
        domenica_col = ris.get('domenica')
        if domenica_col is not None:
            avail_dom = [emp for emp, flag in zip(ris['id dipendente'], domenica_col)
                          if str(flag).strip().lower() != 'riposo']
        else:
            avail_dom = list(ris['id dipendente'])
        for emp in avail_dom[:dom_needed]:
            forced_on.setdefault(emp, set()).add('Dom')

    weekend_days = {'Sab', 'Dom'}
    prefer_set = set(prefer_phases)

    print(f"DEBUG: Overcapacity ratio = {overcapacity_ratio:.1%}")
    print(f"DEBUG: Giorni con domanda zero: {zero_demand_days}")
    print(f"DEBUG: Modalita fase rigida: {strict_phase}")

    print("\nDEBUG: Intervalli domanda per giorno:")
    for g in giorni:
        if g not in zero_demand_days:
            print(f"  {g}: {_from_minutes(min_demand_start[g])} - {_from_minutes(max_demand_end[g])}")

    shift_start_min = dict(zip(turni_cand['id turno'], turni_cand['start_min']))
    shift_end_min = dict(zip(turni_cand['id turno'], turni_cand['end_min']))
    shift_slots = {
        row['id turno']: [s for s in slot_list if row['start_min'] <= s < row['end_min']]
        for _, row in turni_cand.iterrows()
    }

    assignment_details = {}

    days_done = {emp: 0 for emp in ris['id dipendente']}
    work_need = emp_work_days.copy()
    forced_off = {emp: set(days) for emp, days in forced_off.items()}

    for emp in ris['id dipendente']:
        for day in zero_demand_days:
            forced_off.setdefault(emp, set()).add(day)

    infeasible = []
    assignments = []
    assigned_once = set()
    assignments_by_emp = {emp: {} for emp in ris['id dipendente']}
    current_coverage = {g: {s: 0 for s in slot_list} for g in giorni}
    day_assignments_count = {g: 0 for g in giorni}
    minute_distribution = defaultdict(int)
    weekend_work = {emp: set() for emp in ris['id dipendente']}

    default_phase_weights = {0: 0.18, 15: 0.32, 30: 0.18, 45: 0.32}
    phase_targets = {minute: default_phase_weights.get(minute, 0.1) for minute in (0, 15, 30, 45)}
    total_weight = sum(phase_targets.values()) or 1.0
    phase_targets = {minute: weight / total_weight for minute, weight in phase_targets.items()}
    valid_minutes = set(phase_targets.keys())

    def minute_preference_score(start_min: int) -> float:
        minute = start_min % 60
        total_assigned = sum(minute_distribution.values())
        expected = phase_targets.get(minute, 0.1) * total_assigned if total_assigned else 0.0
        actual = minute_distribution.get(minute, 0)
        diff = expected - actual
        scale = 90.0 if strict_phase else 45.0
        bonus = 25.0 if minute in prefer_set else -12.0
        if minute not in valid_minutes:
            return -160.0 if strict_phase else -70.0
        return diff * scale + bonus

    def day_balance_bonus(day: str) -> float:
        total_assigned = sum(day_assignments_count.values())
        if total_assigned == 0:
            return 0.0
        desired = day_weights.get(day, 0.0)
        actual = day_assignments_count[day] / total_assigned
        scale = 140.0 if strict_phase else 70.0
        return (desired - actual) * scale

    def weekend_bonus(emp: str, day: str) -> float:
        if day not in weekend_days:
            return 0.0
        already = len(weekend_work[emp])
        if already == 0:
            return 45.0
        others = any(
            len(weekend_work[o]) == 0 and days_done[o] < work_need[o] and day not in forced_off.get(o, set())
            for o in ris['id dipendente'] if o != emp
        )
        if others:
            return -180.0 if strict_phase else -110.0
        return -60.0

    def shift_value(emp: str, day: str, sid: str, allow_overcapacity: bool = False) -> float:
        if day in zero_demand_days:
            return -1e4
        st = shift_start_min[sid]
        en = shift_end_min[sid]
        mds = min_demand_start.get(day, 24 * 60)
        if mds < 24 * 60 and st < mds - 15:
            return -1e4
        if en <= mds:
            return -1e4
        score = 0.0
        for slot in shift_slots.get(sid, []):
            demand = demand_by_slot[day].get(slot, 0.0)
            current = current_coverage[day][slot]
            gap = demand - current
            if gap > 0:
                score += min(gap, 1.0) * 140.0
            else:
                penalty = abs(gap)
                damp = 18.0 if allow_overcapacity else 42.0
                if demand <= 0:
                    damp *= 3.0
                score -= penalty * damp
        score += minute_preference_score(st)
        score += day_balance_bonus(day)
        score += weekend_bonus(emp, day)
        if day == "Dom":
            score += 220.0
        return score

    def apply_assignment(emp: str, day: str, sid: str) -> None:
        assignments.append((emp, day, sid))
        assignments_by_emp.setdefault(emp, {})[day] = sid
        assignment_details[(emp, day)] = {
            'sid': sid,
            'base_start': shift_start_min[sid],
            'base_end': shift_end_min[sid],
            'actual_start_min': shift_start_min[sid],
            'actual_end_min': shift_end_min[sid],
            'ot_start_slots': 0,
            'ot_end_slots': 0,
            'ot_minutes_start': 0,
            'ot_minutes_end': 0,
            'total_ot_minutes': 0,
            'extra_slots': set(),
            'ot_direction': None,
        }
        days_done[emp] += 1
        assigned_once.add((emp, day))
        day_assignments_count[day] += 1
        minute_distribution[shift_start_min[sid] % 60] += 1
        if day in weekend_days:
            weekend_work[emp].add(day)
        for slot in shift_slots.get(sid, []):
            current_coverage[day][slot] += 1

    def remove_assignment(emp: str, day: str):
        sid = assignments_by_emp.get(emp, {}).pop(day, None)
        if sid is None:
            return None
        meta = assignment_details.pop((emp, day), None)
        for idx, (e, d, s) in enumerate(assignments):
            if e == emp and d == day and s == sid:
                assignments.pop(idx)
                break
        days_done[emp] -= 1
        day_assignments_count[day] -= 1
        start_minute = meta['actual_start_min'] if meta else shift_start_min[sid]
        minute_distribution[start_minute % 60] -= 1
        if day in weekend_days and day in weekend_work.get(emp, set()):
            weekend_work[emp].discard(day)
        for slot in shift_slots.get(sid, []):
            current_coverage[day][slot] -= 1
        if meta:
            for extra_slot in meta.get('extra_slots', set()):
                if extra_slot in current_coverage[day]:
                    current_coverage[day][extra_slot] -= 1
        return sid

    def remaining_people():
        return [e for e in ris['id dipendente'] if days_done[e] < work_need[e]]

    def min_weekend_remaining():
        rem = [e for e in ris['id dipendente'] if days_done[e] < work_need[e]]
        if not rem:
            return 0
        return min(len(weekend_work[e]) for e in rem)

    giorni_validi = [g for g in giorni if g not in zero_demand_days]

    for day in giorni_validi:
        must = [emp for emp in ris['id dipendente'] if day in forced_on.get(emp, set())]
        for emp in sorted(must, key=lambda e: days_done[e]):
            if days_done[emp] >= work_need[emp]:
                infeasible.append((emp, f"FORCED_ON su {day} supera i giorni lavorativi consentiti"))
                continue
            best = None
            best_sid = None
            for sid in shift_by_emp.get(emp, []):
                val = shift_value(emp, day, sid)
                key = (val, -shift_start_min[sid])
                if best is None or key > best:
                    best = key
                    best_sid = sid
            if best_sid is None:
                infeasible.append((emp, f"Nessun turno compatibile in {day} (FORCED_ON)"))
                continue
            apply_assignment(emp, day, best_sid)

    safety = 0
    while remaining_people() and safety < 12000:
        safety += 1
        critical_gaps = []
        for day in giorni_validi:
            for slot in slot_list:
                demand = demand_by_slot[day].get(slot, 0.0)
                current = current_coverage[day][slot]
                if demand > 0 and current < demand:
                    critical_gaps.append((demand - current, day, slot))
        critical_gaps.sort(reverse=True)

        progressed = False
        for _, day, target_slot in critical_gaps[:20]:
            candidates = []
            for emp in remaining_people():
                if (emp, day) in assigned_once or day in forced_off.get(emp, set()):
                    continue
                for sid in shift_by_emp.get(emp, []):
                    if target_slot not in shift_slots.get(sid, []):
                        continue
                    val = shift_value(emp, day, sid)
                    if val <= -1e4:
                        continue
                    need = work_need[emp] - days_done[emp]
                    candidates.append((val, need, -shift_start_min[sid], emp, sid))
            if day in weekend_days and candidates:
                min_weekend = min(len(weekend_work[c[3]]) for c in candidates)
                candidates = [c for c in candidates if len(weekend_work[c[3]]) == min_weekend]
            if not candidates:
                continue
            _, _, _, emp, sid = max(candidates)
            apply_assignment(emp, day, sid)
            progressed = True
            break

        if progressed:
            continue

        assigned_flag = False
        for emp in remaining_people():
            min_week = min_weekend_remaining()
            best = None
            best_day = None
            best_sid = None
            for day in giorni_validi:
                if (emp, day) in assigned_once or day in forced_off.get(emp, set()):
                    continue
                if day in weekend_days and len(weekend_work[emp]) > min_week:
                    continue
                for sid in shift_by_emp.get(emp, []):
                    val = shift_value(emp, day, sid, allow_overcapacity=True)
                    if val <= -1e4:
                        continue
                    key = (val, -shift_start_min[sid])
                    if best is None or key > best:
                        best = key
                        best_day = day
                        best_sid = sid
            if best_sid is not None:
                apply_assignment(emp, best_day, best_sid)
                assigned_flag = True
            else:
                infeasible.append((emp, 'Impossibile assegnare il numero di turni richiesto dalle risorse.'))
            if assigned_flag:
                break

        if not assigned_flag:
            break

    def rebalance_weekends():
        changed = True
        while changed:
            changed = False
            double = [emp for emp in ris['id dipendente'] if len(weekend_work[emp]) >= 2]
            zero = [emp for emp in ris['id dipendente'] if len(weekend_work[emp]) == 0 and assignments_by_emp[emp]]
            if not double or not zero:
                break
            for emp in double:
                swapped = False
                weekend_days_emp = sorted([d for d in weekend_days if d in assignments_by_emp[emp]])
                for wday in weekend_days_emp:
                    sid_w = assignments_by_emp[emp][wday]
                    for cand in zero:
                        if len(weekend_work[cand]) != 0:
                            continue
                        if wday in forced_off.get(cand, set()):
                            continue
                        if sid_w not in shift_by_emp.get(cand, []):
                            continue
                        for day_cand, sid_cand in list(assignments_by_emp[cand].items()):
                            if day_cand in weekend_days:
                                continue
                            if day_cand in forced_off.get(emp, set()):
                                continue
                            if sid_cand not in shift_by_emp.get(emp, []):
                                continue
                            remove_assignment(emp, wday)
                            remove_assignment(cand, day_cand)
                            apply_assignment(cand, wday, sid_w)
                            apply_assignment(emp, day_cand, sid_cand)
                            swapped = True
                            changed = True
                            break
                        if swapped:
                            break
                    if swapped:
                        break
                if changed:
                    break

    rebalance_weekends()
    def allocate_overtime():
        nonlocal assignments, assignments_by_emp
        slot_set = set(slot_list)
        split_flags = {emp: bool(ot_split_by_emp.get(emp, False)) for emp in ris['id dipendente']}

        def snapshot_meta(meta: dict):
            if meta is None:
                return None
            return {
                'actual_start_min': meta.get('actual_start_min'),
                'actual_end_min': meta.get('actual_end_min'),
                'ot_start_slots': int(meta.get('ot_start_slots', 0)),
                'ot_end_slots': int(meta.get('ot_end_slots', 0)),
                'ot_minutes_start': int(meta.get('ot_minutes_start', 0) or 0),
                'ot_minutes_end': int(meta.get('ot_minutes_end', 0) or 0),
                'total_ot_minutes': int(meta.get('total_ot_minutes', 0) or 0),
                'extra_slots': set(meta.get('extra_slots', set())),
                'ot_direction': meta.get('ot_direction'),
                'ot_detached_start_min': meta.get('ot_detached_start_min'),
                'ot_detached_end_min': meta.get('ot_detached_end_min'),
                'base_start': meta.get('base_start'),
                'base_end': meta.get('base_end'),
            }

        def slot_has_demand(day: str, slot: int) -> bool:
            return demand_by_slot[day].get(slot, 0.0) > 1e-6

        required_slots = {}
        id_list = list(ris['id dipendente'])
        for emp in id_list:
            daily_map = ot_daily_minutes_by_emp.get(emp, {})
            for day, minutes in daily_map.items():
                minutes = int(minutes or 0)
                if minutes <= 0:
                    continue
                if (emp, day) not in assignment_details and not allow_ot_overcap:
                    continue
                if slot_size <= 0:
                    continue
                if minutes % slot_size != 0:
                    raise ValueError(f"Straordinario per {emp} su {day} ({minutes} minuti) non allineato alla griglia {slot_size} minuti")
                slots_needed = minutes // slot_size
                if slots_needed <= 0:
                    continue
                required_slots[(emp, day)] = slots_needed

        if not required_slots:
            for meta in assignment_details.values():
                meta['actual_start_min'] = meta['base_start']
                meta['actual_end_min'] = meta['base_end']
                meta['ot_minutes_start'] = 0
                meta['ot_minutes_end'] = 0
                meta['total_ot_minutes'] = 0
                if isinstance(meta.get('extra_slots'), set):
                    meta['extra_slots'] = sorted(meta['extra_slots'])
            return {emp: 0 for emp in ris['id dipendente']}

        def can_extend_start(emp: str, meta: dict, day: str, steps: int) -> bool:
            if steps <= 0:
                return False
            if not split_flags.get(emp, False) and meta.get('ot_end_slots', 0) > 0:
                return False
            for i in range(1, steps + 1):
                new_slot = meta['actual_start_min'] - i * slot_size
                if new_slot not in slot_set or not slot_has_demand(day, new_slot):
                    return False
            return True

        def can_extend_end(emp: str, meta: dict, day: str, steps: int) -> bool:
            if steps <= 0:
                return False
            if not split_flags.get(emp, False) and meta.get('ot_start_slots', 0) > 0:
                return False
            for i in range(steps):
                new_slot = meta['actual_end_min'] + i * slot_size
                if new_slot not in slot_set or not slot_has_demand(day, new_slot):
                    return False
            return True

        def perform_extend(emp: str, day: str, direction: str, steps: int) -> bool:
            if steps <= 0:
                return False
            meta = assignment_details[(emp, day)]
            meta.setdefault('extra_slots', set())
            if direction == 'start':
                if not can_extend_start(emp, meta, day, steps):
                    return False
                for _ in range(steps):
                    new_slot = meta['actual_start_min'] - slot_size
                    if not slot_has_demand(day, new_slot):
                        return False
                    meta['actual_start_min'] = new_slot
                    meta['ot_start_slots'] += 1
                    meta['ot_minutes_start'] = meta['ot_start_slots'] * slot_size
                    meta['total_ot_minutes'] = meta['ot_minutes_start'] + meta['ot_minutes_end']
                    meta['ot_direction'] = 'start' if meta.get('ot_direction') in (None, 'start') else 'both'
                    meta['extra_slots'].add(new_slot)
                    if new_slot in current_coverage[day]:
                        current_coverage[day][new_slot] += 1
                return True
            if direction == 'end':
                if not can_extend_end(emp, meta, day, steps):
                    return False
                for _ in range(steps):
                    new_slot = meta['actual_end_min']
                    if not slot_has_demand(day, new_slot):
                        return False
                    meta['actual_end_min'] = meta['actual_end_min'] + slot_size
                    meta['ot_end_slots'] += 1
                    meta['ot_minutes_end'] = meta['ot_end_slots'] * slot_size
                    meta['total_ot_minutes'] = meta['ot_minutes_start'] + meta['ot_minutes_end']
                    meta['ot_direction'] = 'end' if meta.get('ot_direction') in (None, 'end') else 'both'
                    meta['extra_slots'].add(new_slot)
                    if new_slot in current_coverage[day]:
                        current_coverage[day][new_slot] += 1
                return True
            return False

        def plan_cover_slot(emp: str, meta: dict, day: str, slot: int, remaining: int):
            if slot < meta['actual_start_min']:
                delta = meta['actual_start_min'] - slot
                if delta % slot_size != 0:
                    return None
                steps = delta // slot_size
                if steps <= 0 or steps > remaining:
                    return None
                for i in range(steps):
                    check_slot = meta['actual_start_min'] - (i + 1) * slot_size
                    if not slot_has_demand(day, check_slot):
                        return None
                if not can_extend_start(emp, meta, day, steps):
                    return None
                return 'start', steps
            if slot >= meta['actual_end_min']:
                delta = slot - meta['actual_end_min']
                if delta % slot_size != 0:
                    return None
                steps = delta // slot_size + 1
                if steps <= 0 or steps > remaining:
                    return None
                for i in range(steps):
                    check_slot = meta['actual_end_min'] + i * slot_size
                    if not slot_has_demand(day, check_slot):
                        return None
                if not can_extend_end(emp, meta, day, steps):
                    return None
                return 'end', steps
            return None

        def fallback_allocate(day: str, day_assigns: list, day_required: dict) -> bool:
            best = None
            for emp, meta in day_assigns:
                remaining = day_required.get(emp, 0)
                if remaining <= 0:
                    continue
                if can_extend_start(emp, meta, day, 1):
                    new_slot = meta['actual_start_min'] - slot_size
                    if slot_has_demand(day, new_slot):
                        score = demand_by_slot[day].get(new_slot, 0.0)
                        candidate = (score, -new_slot, emp, 'start')
                        if best is None or candidate > best:
                            best = candidate
                if can_extend_end(emp, meta, day, 1):
                    new_slot = meta['actual_end_min']
                    if slot_has_demand(day, new_slot):
                        score = demand_by_slot[day].get(new_slot, 0.0)
                        candidate = (score, -new_slot, emp, 'end')
                        if best is None or candidate > best:
                            best = candidate
            if not best:
                return False
            _, _, emp, direction = best
            success = perform_extend(emp, day, direction, 1)
            if not success:
                return False
            day_required[emp] -= 1
            return True

        unmet_requirements = []

        for day in ['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom']:
            targets = {emp: required_slots[(emp, day)] for emp in ris['id dipendente'] if (emp, day) in required_slots}
            if not targets:
                continue

            day_assigns = [(emp, assignment_details[(emp, day)]) for emp in ris['id dipendente'] if (emp, day) in assignment_details]
            new_meta_created = set()
            if allow_ot_overcap:
                created_meta = False
                for emp, slots_needed in targets.items():
                    if (emp, day) in assignment_details:
                        continue
                    if not split_flags.get(emp, False):
                        continue
                    demand_slots = [slot for slot in slot_list if demand_by_slot[day].get(slot, 0.0) > 1e-6]
                    if not demand_slots:
                        unmet_requirements.append((emp, day, slots_needed * slot_size))
                        continue
                    start_slot = demand_slots[0]
                    meta = {
                        'sid': None,
                        'base_start': start_slot,
                        'base_end': start_slot,
                        'actual_start_min': start_slot,
                        'actual_end_min': start_slot,
                        'ot_start_slots': 0,
                        'ot_end_slots': 0,
                        'ot_minutes_start': 0,
                        'ot_minutes_end': 0,
                        'total_ot_minutes': 0,
                        'extra_slots': set(),
                        'ot_direction': None,
                    }
                    assignment_details[(emp, day)] = meta
                    assignments.append((emp, day, None))
                    assignments_by_emp.setdefault(emp, {})[day] = None
                    day_assigns.append((emp, meta))
                    new_meta_created.add((emp, day))
                    created_meta = True
            original_meta_state = {}
            for emp_tmp, meta_tmp in day_assigns:
                original_meta_state[emp_tmp] = snapshot_meta(meta_tmp)
            if not day_assigns:
                for emp, slots_needed in targets.items():
                    unmet_requirements.append((emp, day, slots_needed * slot_size))
                continue

            day_required = targets.copy()

            for emp, slots_needed in list(day_required.items()):
                if slots_needed <= 0:
                    day_required[emp] = 0
                    continue
                if (emp, day) not in assignment_details:
                    unmet_requirements.append((emp, day, slots_needed * slot_size))
                    day_required[emp] = 0

            # First, place DETACHED single-block OT for employees marked with 'OT spezz'
            for emp, meta in list(day_assigns):
                steps = day_required.get(emp, 0)
                if steps <= 0:
                    continue
                if not split_flags.get(emp, False):
                    continue
                meta.setdefault('extra_slots', set())
                base_start = int(meta['base_start'])
                base_end = int(meta['base_end'])
                base_slots = set(range(base_start, base_end, slot_size))
                best_score = None
                best_start = None
                for s in slot_list:
                    end_s = s + steps * slot_size
                    if end_s > slot_list[-1] + slot_size:
                        break
                    ok = True
                    score = 0.0
                    cur = s
                    for _ in range(steps):
                        if cur not in slot_set:
                            ok = False
                            break
                        if cur in base_slots or not slot_has_demand(day, cur):
                            ok = False
                            break
                        score += demand_by_slot[day].get(cur, 0.0) - current_coverage[day][cur]
                        cur += slot_size
                    if not ok:
                        continue
                    candidate = (score, -s)
                    if (best_score is None) or (candidate > best_score):
                        best_score = candidate
                        best_start = s
                if best_start is not None:
                    cur = best_start
                    for _ in range(steps):
                        meta['extra_slots'].add(cur)
                        if slot_has_demand(day, cur) and cur in current_coverage[day]:
                            current_coverage[day][cur] += 1
                        cur += slot_size
                    meta['ot_detached_start_min'] = best_start
                    meta['ot_detached_end_min'] = best_start + steps * slot_size
                    meta['ot_minutes_start'] = 0
                    meta['ot_minutes_end'] = 0
                    meta['ot_start_slots'] = 0
                    meta['ot_end_slots'] = 0
                    meta['total_ot_minutes'] = steps * slot_size
                    day_required[emp] = 0

            if allow_ot_overcap:
                for emp, meta in day_assigns:
                    remaining = day_required.get(emp, 0)
                    while remaining > 0:
                        if can_extend_end(emp, meta, day, 1):
                            if perform_extend(emp, day, 'end', 1):
                                remaining -= 1
                                day_required[emp] = remaining
                                continue
                        if can_extend_start(emp, meta, day, 1):
                            if perform_extend(emp, day, 'start', 1):
                                remaining -= 1
                                day_required[emp] = remaining
                                continue
                        break
            else:
                while sum(day_required.values()) > 0:
                    progress = False
                    gap_slots = []
                    for slot in slot_list:
                        gap = demand_by_slot[day].get(slot, 0.0) - current_coverage[day][slot]
                        if gap > 1e-6:
                            gap_slots.append((gap, slot))
                    gap_slots.sort(reverse=True)

                    for gap, slot in gap_slots:
                        if not slot_has_demand(day, slot):
                            continue
                        best_plan = None
                        for emp, meta in day_assigns:
                            remaining = day_required.get(emp, 0)
                            if remaining <= 0:
                                continue
                            plan = plan_cover_slot(emp, meta, day, slot, remaining)
                            if not plan:
                                continue
                            direction, steps = plan
                            candidate = (gap, -steps, emp, direction, steps)
                            if best_plan is None or candidate > best_plan:
                                best_plan = candidate
                        if best_plan:
                            _, _, emp, direction, steps = best_plan
                            if perform_extend(emp, day, direction, steps):
                                day_required[emp] -= steps
                                progress = True
                                break
                    if progress:
                        continue
                    if fallback_allocate(day, day_assigns, day_required):
                        continue
                    break

            for emp, remaining in day_required.items():
                if remaining > 0:
                    unmet_requirements.append((emp, day, remaining * slot_size))

            for emp in targets.keys():
                meta = assignment_details.get((emp, day))
                expected_minutes = int(ot_daily_minutes_by_emp.get(emp, {}).get(day, 0) or 0)
                actual_minutes = int(meta.get('total_ot_minutes', 0) or 0) if meta else 0
                residual = expected_minutes - actual_minutes
                if residual > 0:
                    snapshot = original_meta_state.get(emp)
                    if (emp, day) in new_meta_created:
                        meta_created = assignment_details.pop((emp, day), None)
                        if meta_created:
                            for slot in meta_created.get('extra_slots', set()):
                                if slot in current_coverage[day]:
                                    current_coverage[day][slot] = max(0, current_coverage[day][slot] - 1)
                        assignments = [item for item in assignments if not (item[0] == emp and item[1] == day and item[2] is None)]
                        assignments_by_emp.get(emp, {}).pop(day, None)
                    elif meta and snapshot:
                        current_slots = set(meta.get('extra_slots', []))
                        original_slots = snapshot.get('extra_slots', set())
                        for slot in current_slots - original_slots:
                            if slot in current_coverage[day]:
                                current_coverage[day][slot] = max(0, current_coverage[day][slot] - 1)
                        meta['actual_start_min'] = snapshot.get('actual_start_min')
                        meta['actual_end_min'] = snapshot.get('actual_end_min')
                        meta['ot_start_slots'] = snapshot.get('ot_start_slots', 0)
                        meta['ot_end_slots'] = snapshot.get('ot_end_slots', 0)
                        meta['ot_minutes_start'] = snapshot.get('ot_minutes_start', 0)
                        meta['ot_minutes_end'] = snapshot.get('ot_minutes_end', 0)
                        meta['total_ot_minutes'] = snapshot.get('total_ot_minutes', 0)
                        meta['extra_slots'] = set(snapshot.get('extra_slots', set()))
                        if snapshot.get('ot_detached_start_min') is None:
                            meta.pop('ot_detached_start_min', None)
                            meta.pop('ot_detached_end_min', None)
                        else:
                            meta['ot_detached_start_min'] = snapshot.get('ot_detached_start_min')
                            meta['ot_detached_end_min'] = snapshot.get('ot_detached_end_min')
                        meta['ot_direction'] = snapshot.get('ot_direction')
                    unmet_requirements.append((emp, day, residual))

        unmet_by_key = {}
        for emp, day, minutes_left in unmet_requirements:
            unmet_by_key[(emp, day)] = unmet_by_key.get((emp, day), 0) + int(minutes_left)

        for (emp, day), minutes_left in unmet_by_key.items():
            if minutes_left > 0:
                infeasible.append((emp, f"Straordinario non allocato su {day}: {minutes_left} minuti"))

        ot_minutes_used = {emp: 0 for emp in ris['id dipendente']}
        for (emp, day), meta in assignment_details.items():
            meta.setdefault('actual_start_min', meta['base_start'])
            meta.setdefault('actual_end_min', meta['base_end'])
            meta.setdefault('ot_minutes_start', 0)
            meta.setdefault('ot_minutes_end', 0)
            meta['total_ot_minutes'] = meta.get('ot_minutes_start', 0) + meta.get('ot_minutes_end', 0)
            if isinstance(meta.get('extra_slots'), set):
                meta['extra_slots'] = sorted(meta['extra_slots'])
            ot_minutes_used[emp] += int(meta.get('total_ot_minutes', 0) or 0)

        return ot_minutes_used
    def fill_sunday_gap(max_iter=200):
        target_day = 'Dom'
        for _ in range(max_iter):
            gaps = [slot for slot in slot_list if demand_by_slot[target_day].get(slot, 0.0) - current_coverage[target_day][slot] > 0.1]
            if not gaps:
                break
            slot = gaps[0]
            candidate = None
            for emp in sorted(ris['id dipendente'], key=lambda e: (len(weekend_work[e]), days_done[e])):
                if (emp, target_day) in assigned_once:
                    continue
                if target_day in forced_off.get(emp, set()):
                    continue
                if len(weekend_work[emp]) >= 1:
                    continue
                # find removable weekday shift
                removable = None
                for day_existing, sid_existing in list(assignments_by_emp[emp].items()):
                    if day_existing == target_day or day_existing in weekend_days:
                        continue
                    # ensure removing does not create deficit
                    if any(current_coverage[day_existing][s] - 1 < demand_by_slot[day_existing].get(s, 0.0) - 0.6 for s in shift_slots[sid_existing]):
                        continue
                    removable = (day_existing, sid_existing)
                    break
                if removable is None:
                    continue
                day_remove, sid_remove = removable
                # try sunday shift covering slot
                for sid_new in shift_by_emp.get(emp, []):
                    if slot not in shift_slots.get(sid_new, []):
                        continue
                    candidate = (emp, day_remove, sid_remove, sid_new)
                    break
                if candidate:
                    break
            if not candidate:
                break
            emp, day_remove, sid_remove, sid_new = candidate
            remove_assignment(emp, day_remove)
            apply_assignment(emp, target_day, sid_new)

    fill_sunday_gap()

    ot_minutes_used = allocate_overtime()

    minute_distribution.clear()
    for emp, day, sid in assignments:
        meta = assignment_details.get((emp, day))
        if meta:
            base_start = meta.get('actual_start_min', shift_start_min.get(sid, 0))
        else:
            base_start = shift_start_min.get(sid, 0)
        minute_distribution[int(base_start) % 60] += 1

    if any(meta.get('total_ot_minutes', 0) > 0 for meta in assignment_details.values()):
        print("\nDEBUG: Overtime assegnato:")
        for (emp, day), meta in sorted(assignment_details.items()):
            total_ot = meta.get('total_ot_minutes', 0)
            if total_ot <= 0:
                continue
            details = []
            if meta.get('ot_minutes_start'):
                details.append(f"inizio -{meta['ot_minutes_start']}m")
            if meta.get('ot_minutes_end'):
                details.append(f"fine +{meta['ot_minutes_end']}m")
            detail_str = '; '.join(details) if details else 'n/a'
            print(f"  {emp} {day}: {detail_str} (tot {total_ot}m)")
        used_summary = [emp for emp in ris['id dipendente'] if ot_minutes_used.get(emp, 0) > 0]
        if used_summary:
            print("  Consumo OT per risorsa:")
            for emp in used_summary:
                print(f"    {emp}: {ot_minutes_used.get(emp, 0)} min")


    riposi_info = []
    for emp in ris['id dipendente']:
        target_rest = int(rest_target_by_emp.get(emp, 2))
        actual_work = days_done[emp]
        actual_rest = 7 - actual_work
        dev = actual_rest - target_rest
        if dev != 0:
            msg = f"Deviazione riposi: target={target_rest}, ottenuti={actual_rest}"
            if (emp, msg) not in infeasible:
                infeasible.append((emp, msg))
        riposi_info.append((emp, target_rest, actual_rest, dev))

    print("\nDEBUG: Assegnazioni finali per giorno:")
    for g in giorni:
        if g not in zero_demand_days and demand_by_day[g] > 0:
            expected_proportion = demand_by_day[g] / total_demand if total_demand > 0 else 0
            actual_proportion = day_assignments_count[g] / max(1, sum(day_assignments_count.values()))
            print(f"  {g}: {day_assignments_count[g]} turni (expected {expected_proportion:.1%}, actual {actual_proportion:.1%})")

    print("\nDEBUG: Distribuzione minuti di inizio turno:")
    total_assignments = len(assignments)
    for minute in sorted(minute_distribution.keys()):
        count = minute_distribution[minute]
        pct = count / total_assignments * 100 if total_assignments else 0
        marker = '*' if minute in prefer_set else ''
        print(f"  :{minute:02d} -> {count} turni ({pct:.1f}%) {marker}")
    preferred = sum(minute_distribution.get(m, 0) for m in prefer_set)
    if total_assignments:
        print(f"Turni ai minuti preferiti {tuple(sorted(prefer_set))}: {preferred}/{total_assignments} ({preferred/total_assignments*100:.1f}%)")

    return assignments, riposi_info, infeasible, day_colmap, slot_list, slot_size, shift_slots, assignment_details, ot_minutes_used

# ----------------------------- Warning Summary -----------------------------

def build_warning_summary(riposi_info, infeasible, ris, assignments, turno_map, prefer_phases):
    '''Costruisce un riepilogo sintetico dei rischi principali.'''
    name_map = dict(zip(ris['id dipendente'], ris['Nome']))
    weekend_days = {'Sab', 'Dom'}

    rows = []

    devianti = []
    for emp, target_riposi, actual_rest, dev in riposi_info:
        if dev != 0:
            nome = name_map.get(emp, emp)
            devianti.append(f"{nome} (target {target_riposi}, ottenuti {actual_rest})")
    if devianti:
        rows.append({
            'Categoria': 'Deviazione riposi',
            'Messaggio': 'Differenza tra riposi target e assegnati',
            'Dipendenti': ', '.join(devianti),
            'Dettagli': ''
        })

    grouped = {}
    for emp, msg in infeasible:
        if 'Deviazione riposi' in msg:
            continue
        grouped.setdefault(msg, set()).add(name_map.get(emp, emp))
    for msg, nomi in grouped.items():
        rows.append({
            'Categoria': 'Vincolo non rispettato',
            'Messaggio': msg,
            'Dipendenti': ', '.join(sorted(nomi)),
            'Dettagli': ''
        })

    weekend_load = {emp: 0 for emp in ris['id dipendente']}
    for emp, day, _ in assignments:
        if day in weekend_days:
            weekend_load[emp] += 1
    heavy = [name_map.get(emp, emp) for emp, cnt in weekend_load.items() if cnt >= 2]
    no_weekend = [name_map.get(emp, emp) for emp, cnt in weekend_load.items() if cnt == 0]
    if heavy and no_weekend:
        rows.append({
            'Categoria': 'Weekend sbilanciati',
            'Messaggio': 'Ridurre consecutivi sabato+domenica sulle stesse risorse',
            'Dipendenti': ', '.join(sorted(heavy)),
            'Dettagli': f"Disponibili senza weekend: {len(no_weekend)}"
        })

    prefer_set = set(prefer_phases)
    if turno_map and assignments and prefer_set:
        total = len(assignments)
        prefer_count = sum(1 for _, _, sid in assignments if turno_map.get(sid, 0) % 60 in prefer_set)
        prefer_pct = prefer_count / total * 100
        if prefer_pct < 45.0:
            rows.append({
                'Categoria': 'Minuti preferiti bassi',
                'Messaggio': f"Quota minuti preferiti {prefer_pct:.1f}%",
                'Dipendenti': '',
                'Dettagli': 'Verificare possibile ri-distribuzione turni'
            })

    if not rows:
        rows.append({
            'Categoria': 'OK',
            'Messaggio': 'Nessun warning',
            'Dipendenti': '',
            'Dettagli': ''
        })

    return pd.DataFrame(rows)


# ----------------------------- Output -----------------------------
# ----------------------------- Output -----------------------------
# ----------------------------- Output -----------------------------


def crea_output(
    assignments,
    turni_cand: pd.DataFrame,
    ris: pd.DataFrame,
    req_pre_original: pd.DataFrame,
    day_colmap,
    slot_list,
    slot_size,
    shift_slots,
    assignment_details
):
    details_map = assignment_details or {}
    name_map = dict(zip(ris['id dipendente'], ris['Nome']))
    key_to_name = {str(emp): name_map.get(emp, emp) for emp in ris['id dipendente']}

    base_start_min_map = dict(zip(turni_cand['id turno'], turni_cand['start_min']))
    base_end_min_map = dict(zip(turni_cand['id turno'], turni_cand['end_min']))
    day_name_map = {'Lun':'Lunedì','Mar':'Martedì','Mer':'Mercoledì','Gio':'Giovedì','Ven':'Venerdì','Sab':'Sabato','Dom':'Domenica'}

    rows = []
    for emp, day, turno in assignments:
        meta = details_map.get((emp, day), {}) or {}
        has_turno = turno in base_start_min_map if turno is not None else False
        if has_turno:
            base_start = int(base_start_min_map.get(turno, 0))
            base_end = int(base_end_min_map.get(turno, base_start))
        else:
            base_start = int(meta.get('base_start', meta.get('actual_start_min', 0)))
            base_end = int(meta.get('base_end', base_start))

        # actual may include OT
        actual_start = int(meta.get('actual_start_min', base_start))
        actual_end = int(meta.get('actual_end_min', base_end))

        ot_minutes_start = int(meta.get('ot_minutes_start', 0) or 0)  # OT before shift
        ot_minutes_end = int(meta.get('ot_minutes_end', 0) or 0)      # OT after shift
        ot_total = int(meta.get('total_ot_minutes', ot_minutes_start + ot_minutes_end))

        # Compute a single OT pair (detached preferred if present)
        ot1_start = ""
        ot1_end = ""
        det_s = meta.get('ot_detached_start_min')
        det_e = meta.get('ot_detached_end_min')
        if det_s is not None and det_e is not None:
            ot1_start = _from_minutes(int(det_s))
            ot1_end = _from_minutes(int(det_e))
        elif ot_minutes_start > 0:
            # segment before base shift
            ot1_start = _from_minutes(actual_start)
            ot1_end = _from_minutes(base_start)

        elif ot_minutes_end > 0:
            ot1_start = _from_minutes(base_end)
            ot1_end = _from_minutes(actual_end)
        # Base (ordinario) fascia MUST ignore OT, mostra orario base
        if has_turno:
            fascia_base = f"{_from_minutes(base_start)}-{_from_minutes(base_end)}"
            base_inizio = _from_minutes(base_start)
            base_fine = _from_minutes(base_end)
        else:
            fascia_base = 'Riposo'
            base_inizio = ''
            base_fine = ''

        rows.append({
            'emp_id': emp,
            'CF': str(emp),
            'Operatore': name_map.get(emp, emp),
            'Giorno': day_name_map[day],
            'Fascia_Ordinario': fascia_base,
            'Base_Inizio': base_inizio,
            'Base_Fine': base_fine,
            'Actual_Inizio': _from_minutes(actual_start),
            'Actual_Fine': _from_minutes(actual_end),
            'OT_min': ot_total,
            'OT_Inizio': ot1_start,
            'OT_Fine': ot1_end,
            'TurnoID': turno
        })
    df_ass = pd.DataFrame(rows)

    # ---- Build 'Pianificazione' with per-day columns ----
    all_days_long = ['Lunedì','Martedì','Mercoledì','Giovedì','Venerdì','Sabato','Domenica']

    if not df_ass.empty:
        plan_src = df_ass[['CF','Operatore','Giorno','Fascia_Ordinario','OT_Inizio','OT_Fine']].copy()
    else:
        plan_src = pd.DataFrame(columns=['CF','Operatore','Giorno','Fascia_Ordinario','OT_Inizio','OT_Fine'])

    # Ensure each employee has a row per day (Riposo if missing)
    all_cfs = [str(emp) for emp in ris['id dipendente']]
    extra_rows = []
    for cf in all_cfs:
        emp_days = set(plan_src[plan_src['CF'] == cf]['Giorno'])
        for d in all_days_long:
            if d not in emp_days:
                extra_rows.append({'CF': cf, 'Operatore': key_to_name.get(cf, cf), 'Giorno': d, 'Fascia_Ordinario': 'Riposo', 'OT_Inizio': '', 'OT_Fine': ''})
    if extra_rows:
        plan_src = pd.concat([plan_src, pd.DataFrame(extra_rows)], ignore_index=True)

    key_to_name = {str(emp): name_map.get(emp, emp) for emp in ris['id dipendente']}
    plan_src = plan_src.sort_values(['CF','Giorno']).drop_duplicates(['CF','Giorno'], keep='first')
    plan_src['Operatore'] = plan_src.apply(lambda r: r['Operatore'] if str(r['Operatore']).strip() != '' else key_to_name.get(str(r['CF']), str(r['CF'])), axis=1)

    pivot = plan_src.pivot_table(index=['CF','Operatore'], columns='Giorno', values=['Fascia_Ordinario','OT_Inizio','OT_Fine'], aggfunc='first')

    # Reorder columns Day -> fields
    if isinstance(pivot.columns, pd.MultiIndex) and pivot.columns.nlevels == 2:
        days = ['Lunedì','Martedì','Mercoledì','Giovedì','Venerdì','Sabato','Domenica']
        fascia_cols = [('Fascia_Ordinario', d) for d in days if ('Fascia_Ordinario', d) in pivot.columns]
        ot_cols = []
        for d in days:
            if ('OT_Inizio', d) in pivot.columns:
                ot_cols.append(('OT_Inizio', d))
            if ('OT_Fine', d) in pivot.columns:
                ot_cols.append(('OT_Fine', d))
        ordered_cols = fascia_cols + ot_cols
        pivot = pivot.reindex(columns=ordered_cols)
        def _col_name(t):
            val, day = t
            if val == 'Fascia_Ordinario':
                return f"{day} - Fascia"
            return f"{day} - {'OT Inizio' if val=='OT_Inizio' else 'OT Fine'}"
        pivot.columns = [_col_name(t) for t in pivot.columns]
    pivot.sort_index(inplace=True)

    # ---- Coverage view (unchanged logic) ----
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
        meta = details_map.get((emp, day))
        if meta:
            for extra_slot in meta.get('extra_slots', []):
                if extra_slot in cover[day]:
                    cover[day][extra_slot] += 1

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
    parser = argparse.ArgumentParser(description='Pianificazione v5.2 - riposi rigidi e bilanciamento minuti.')
    parser.add_argument('--input', required=True, help='Percorso al file Excel di input (.xlsx/.xlsm)')
    parser.add_argument('--out', default='tabella_turni.xlsx', help='File Excel di output')
    parser.add_argument('--grid', type=int, default=15, help='Griglia minuti per start turno (default 15)')
    parser.add_argument('--prefer_phase', type=str, default='15,45',
                       help="Minuti preferiti per l'inizio turno (es. '15,45' o '0,15,30,45')")
    parser.add_argument('--force_phase', action='store_true',
                       help='Consente solo i minuti specificati in --prefer_phase')
    parser.add_argument('--strict-phase', action='store_true',
                       help='Enfatizza ulteriormente i minuti preferiti')
    parser.add_argument('--force-ot', action='store_true',
                       help='Assegna comunque lo straordinario disponibile anche in overcoverage (se minuti coerenti)')
    args = parser.parse_args()

    req, turni, ris = carica_dati(args.input)
    req_pre = prepara_req(req)

    durations_by_emp, rest_target_by_emp, ot_minutes_by_emp, ot_split_by_emp, ot_daily_minutes_by_emp = infer_personal_params_from_risorse(ris)
    durations_set_min = set(durations_by_emp.values())

    prefer_tokens = [t.strip() for t in str(args.prefer_phase).split(',') if t.strip()]
    prefer_values = []
    for tok in prefer_tokens:
        try:
            minute = int(tok)
        except ValueError:
            raise SystemExit(f"Parametro --prefer_phase non valido: '{tok}' non e' un intero.")
        if minute < 0 or minute >= 60:
            raise SystemExit(f"Parametro --prefer_phase non valido: {minute} deve essere compreso tra 0 e 59.")
        prefer_values.append(minute)
    prefer_phases = tuple(sorted(set(prefer_values)))

    if args.grid <= 0:
        raise SystemExit("Parametro --grid non valido: deve essere un intero positivo.")

    force_minutes = prefer_phases if args.force_phase else None
    if args.force_phase and not force_minutes:
        raise SystemExit("Usa --force_phase insieme a --prefer_phase valorizzato (es. --prefer_phase 0,15,30,45).")

    if prefer_phases:
        print(f"INFO: Preferenza minuti: {prefer_phases}")
    else:
        print("INFO: Nessuna preferenza minuti specificata")
    if force_minutes:
        print(f"INFO: Forzo generazione solo ai minuti: {force_minutes}")
    if args.strict_phase:
        print("INFO: Modalita STRICT PHASE attiva")

    try:
        turni_cand = genera_turni_candidati(
            req_pre,
            durations_set_min,
            grid_step_min=args.grid,
            force_phase_minutes=force_minutes
        )
    except ValueError as exc:
        raise SystemExit(str(exc))

    print(f"INFO: Generati {len(turni_cand)} turni candidati")
    if force_minutes:
        distrib = {}
        for start in turni_cand['start_min']:
            minute = start % 60
            distrib[minute] = distrib.get(minute, 0) + 1
        print("INFO: Distribuzione minuti candidati:")
        for minute in sorted(distrib):
            print(f"  :{minute:02d} -> {distrib[minute]}")

    shift_by_emp = determina_turni_ammissibili(ris, turni_cand, durations_by_emp)
    forced_off, forced_on = leggi_vincoli_weekend(ris)

    prefer_tuple = prefer_phases if prefer_phases else tuple(sorted({0, 15, 30, 45}))

    assignments, riposi_info, infeasible, day_colmap, slot_list, slot_size, shift_slots, assignment_details, ot_minutes_used = assegnazione_tight_capacity(
        req_pre.copy(), turni_cand, ris, shift_by_emp, rest_target_by_emp, durations_by_emp,
        forced_off, forced_on, ot_minutes_by_emp, ot_split_by_emp, ot_daily_minutes_by_emp,
        allow_ot_overcap=args.force_ot,
        prefer_phases=prefer_tuple, strict_phase=args.strict_phase
    )

    pivot, df_ass, df_cov = crea_output(assignments, turni_cand, ris, req_pre, day_colmap, slot_list, slot_size, shift_slots, assignment_details)

    turno_map = dict(zip(turni_cand['id turno'], turni_cand['start_min']))
    df_warn = build_warning_summary(riposi_info, infeasible, ris, assignments, turno_map, prefer_tuple)

    out_path = Path(args.out).expanduser()
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_path) as writer:
        plan_out = pivot.reset_index()
        if 'CF' in plan_out.columns:
            plan_out = plan_out.sort_values(['CF','Operatore']).drop_duplicates(['CF'], keep='first')
        # Optional sort by Operatore then CF
        if 'Operatore' in plan_out.columns and 'CF' in plan_out.columns:
            plan_out = plan_out.sort_values(['Operatore','CF'])
        plan_out.to_excel(writer, sheet_name='Pianificazione', index=False)
        df_ass.to_excel(writer, sheet_name='Assegnazioni', index=False)
        df_cov.to_excel(writer, sheet_name='Copertura', index=False)
        df_warn.to_excel(writer, sheet_name='Warnings', index=False)

    print(f"Salvato in: {out_path}")
if __name__ == '__main__':
    main()








