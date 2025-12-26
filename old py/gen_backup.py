"""
generate_turni_tabella_finale_v5_0.py

Versione definitiva con TUTTI i fix:
- NO TURNI PRIMA DELLA DOMANDA: Blocca turni che iniziano prima dell'orario di domanda
- MINUTI FORZATI: Penalità MASSIVE per turni non :15/:45
- FIX DOMENICA: Non assegna turni quando domanda = 0
- Ottimizzato per capacità limitata (<5% overcapacity)

Uso:
  python generate_turni_tabella_finale_v5_0.py --input "input.xlsm" --out "output.xlsx" --grid 15 --prefer_phase "15,45"
  
  Opzioni:
  --force_phase : genera SOLO turni che iniziano ai minuti specificati
  --strict_phase : penalità ESTREMA per turni non ai minuti preferiti (consigliato)
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




def assegnazione_tight_capacity(req: pd.DataFrame, turni_cand: pd.DataFrame, ris: pd.DataFrame,
                                shift_by_emp: dict, rest_target_by_emp: dict, durations_by_emp: dict,
                                forced_off: dict, forced_on: dict, prefer_phases=(15, 45), strict_phase=False):
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
        for idx, (e, d, s) in enumerate(assignments):
            if e == emp and d == day and s == sid:
                assignments.pop(idx)
                break
        days_done[emp] -= 1
        day_assignments_count[day] -= 1
        minute_distribution[shift_start_min[sid] % 60] -= 1
        if day in weekend_days and day in weekend_work.get(emp, set()):
            weekend_work[emp].discard(day)
        for slot in shift_slots.get(sid, []):
            current_coverage[day][slot] -= 1
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

    return assignments, riposi_info, infeasible, day_colmap, slot_list, slot_size, shift_slots



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
    args = parser.parse_args()

    req, turni, ris = carica_dati(args.input)
    req_pre = prepara_req(req)

    durations_by_emp, rest_target_by_emp = infer_personal_params_from_risorse(ris)
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

    assignments, riposi_info, infeasible, day_colmap, slot_list, slot_size, shift_slots = assegnazione_tight_capacity(
        req_pre.copy(), turni_cand, ris, shift_by_emp, rest_target_by_emp, durations_by_emp,
        forced_off, forced_on, prefer_phases=prefer_tuple, strict_phase=args.strict_phase
    )

    pivot, df_ass, df_cov = crea_output(assignments, turni_cand, ris, req_pre, day_colmap, slot_list, slot_size, shift_slots)

    turno_map = dict(zip(turni_cand['id turno'], turni_cand['start_min']))
    df_warn = build_warning_summary(riposi_info, infeasible, ris, assignments, turno_map, prefer_tuple)

    out_path = Path(args.out).expanduser()
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_path) as writer:
        pivot.to_excel(writer, sheet_name='Pianificazione')
        df_ass.to_excel(writer, sheet_name='Assegnazioni', index=False)
        df_cov.to_excel(writer, sheet_name='Copertura', index=False)
        df_warn.to_excel(writer, sheet_name='Warnings', index=False)

    print(f"Salvato in: {out_path}")
if __name__ == '__main__':
    main()
