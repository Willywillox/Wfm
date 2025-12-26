"""
generate_turni_tabella_finale_v4_7.py

Versione corretta che distribuisce l'overcapacity seguendo esattamente la curva della domanda.
- Riposi (col. D) **perentori**: ogni risorsa lavora **esattamente** 7-D giorni.
- L'overcapacity viene distribuita **proporzionalmente alla domanda reale** dei Requisiti
- Non più distribuzione uniforme ma seguendo le curve di domanda giornaliere e orarie

Output: Pianificazione, Assegnazioni, Copertura, Warnings.

Uso:
  python generate_turni_tabella_finale_v4_7.py --input "input wfmmacro rbm.xlsm" --out "tabella_turni.xlsx" --grid 15 --prefer_phase "15,45"
"""

import argparse
import unicodedata
from pathlib import Path
import re
import datetime as _dt
from collections import defaultdict
import math
import pandas as pd


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


def genera_turni_candidati(req_pre: pd.DataFrame, durations_set_min: set, grid_step_min: int) -> pd.DataFrame:
    min_start = int(req_pre['start_min'].min())
    max_end = int(req_pre['end_min'].max())
    rows = []
    for dmin in sorted(durations_set_min):
        start = min_start
        while start + dmin <= max_end:
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


# ----------------------------- NUOVO: Calcolo quote proporzionali alla domanda -----------------------------
def build_demand_proportional_quotas(req_pre: pd.DataFrame, ris: pd.DataFrame, durations_by_emp: dict,
                                     rest_target_by_emp: dict, forced_off: dict):
    """
    Calcola le quote in modo proporzionale alla domanda REALE, non uniformemente.
    """
    giorni = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom']
    day_colmap = map_req_day_columns(req_pre)
    slot_list = req_pre['start_min'].tolist()
    slot_size = compute_slot_size(req_pre)
    
    # Calcola la domanda totale per giorno e per slot
    demand_by_day = {}
    demand_by_slot = {g: {} for g in giorni}
    total_demand = 0.0
    
    for g in giorni:
        col = day_colmap.get(g)
        if col and col in req_pre.columns:
            day_demand = float(req_pre[col].sum())
            demand_by_day[g] = day_demand
            total_demand += day_demand
            
            # Domanda per slot
            for i, slot in enumerate(slot_list):
                demand_by_slot[g][slot] = float(req_pre.loc[i, col])
        else:
            demand_by_day[g] = 0.0
            for slot in slot_list:
                demand_by_slot[g][slot] = 0.0
    
    # Calcola la capacità totale disponibile (in slot units)
    total_capacity_slots = 0.0
    for emp in ris['id dipendente']:
        D = int(rest_target_by_emp.get(emp, 2))
        forced = len(forced_off.get(emp, set()))
        work_days = max(0, 7 - max(D, forced))
        slots_per_shift = durations_by_emp.get(emp, 240) / slot_size
        total_capacity_slots += work_days * slots_per_shift
    
    # Calcola il fattore di scala: quanta capacità abbiamo rispetto alla domanda
    scale_factor = total_capacity_slots / total_demand if total_demand > 0 else 1.0
    
    # Distribuisci la capacità proporzionalmente alla domanda
    slot_quota = {}
    for g in giorni:
        slot_quota[g] = {}
        for slot in slot_list:
            # La quota è la domanda moltiplicata per il fattore di scala
            slot_quota[g][slot] = demand_by_slot[g][slot] * scale_factor
    
    # Calcola anche le quote giornaliere
    day_quota_units = {g: demand_by_day[g] * scale_factor for g in giorni}
    
    return slot_quota, day_quota_units, slot_list, slot_size, day_colmap, demand_by_slot


# ----------------------------- Core migliorato con distribuzione proporzionale -----------------------------
def assegnazione_with_demand_proportional(req: pd.DataFrame, turni_cand: pd.DataFrame, ris: pd.DataFrame,
                                          shift_by_emp: dict, rest_target_by_emp: dict, durations_by_emp: dict,
                                          forced_off: dict, forced_on: dict, prefer_phases=(15,45)):
    """
    Assegnazione che segue rigorosamente la curva della domanda.
    """
    giorni = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom']
    
    # Usa il nuovo calcolo delle quote proporzionali
    slot_quota, day_quota_units, slot_list, slot_size, day_colmap, demand_by_slot = build_demand_proportional_quotas(
        req, ris, durations_by_emp, rest_target_by_emp, forced_off
    )

    # work_need esatto per persona
    work_need = {}
    infeasible = []
    for emp in ris['id dipendente']:
        D = int(rest_target_by_emp.get(emp, 2))
        w = max(0, 7 - D)
        if len(forced_off.get(emp, set())) > D:
            infeasible.append((emp, f"FORCED_OFF={len(forced_off[emp])} > Riposi={D}"))
            w = max(0, 7 - len(forced_off[emp]))
        if len(forced_on.get(emp, set())) > w:
            infeasible.append((emp, f"FORCED_ON={len(forced_on[emp])} > LavoroRichiesto={w}"))
            w = len(forced_on[emp])
        work_need[emp] = w

    # Precompute shift -> slots
    shift_start_min = dict(zip(turni_cand['id turno'], turni_cand['start_min']))
    shift_end_min = dict(zip(turni_cand['id turno'], turni_cand['end_min']))
    shift_slots = {
        row['id turno']: [s for s in slot_list if row['start_min'] <= s < row['end_min']]
        for _, row in turni_cand.iterrows()
    }
    shift_len_slots = {sid: len(sl) for sid, sl in shift_slots.items()}

    days_done = {emp: 0 for emp in ris['id dipendente']}
    assigned_once = set()
    assignments = []
    
    # Traccia la copertura corrente per evitare overcoverage eccessivo
    current_coverage = {g: {s: 0 for s in slot_list} for g in giorni}

    def fase_score(start_min: int) -> int:
        return 1 if (start_min % 60) in prefer_phases else 0

    def shift_value_score(day, sid):
        """
        Calcola il valore di un turno basato su:
        1. Quanto copre la domanda non ancora soddisfatta
        2. Evita overcoverage eccessivo
        """
        score = 0.0
        for slot in shift_slots.get(sid, []):
            demand = demand_by_slot[day].get(slot, 0.0)
            current = current_coverage[day][slot]
            remaining_quota = slot_quota[day].get(slot, 0.0)
            
            # Punteggio alto se c'è domanda non coperta
            if demand > 0:
                if current < demand:
                    # Priorità massima: coprire la domanda base
                    score += min(1.0, demand - current) * 10.0
                elif remaining_quota > 0:
                    # Priorità media: usare la quota rimanente dove c'è domanda
                    score += min(1.0, remaining_quota) * 5.0
                else:
                    # Penalità per overcoverage eccessivo (>150% della domanda)
                    if current > demand * 1.5:
                        score -= (current / demand - 1.5) * 2.0
            else:
                # Forte penalità per assegnare turni dove non c'è domanda
                score -= 5.0
        
        return score

    # 1) Assegna FORCED_ON rispettando la domanda
    for day in giorni:
        must = [emp for emp in ris['id dipendente'] if day in forced_on.get(emp, set()) and days_done[emp] < work_need[emp]]
        for emp in sorted(must, key=lambda e: days_done[e]):
            if (emp, day) in assigned_once:
                continue
            best = None; best_sid = None
            for sid in shift_by_emp.get(emp, []):
                value = shift_value_score(day, sid)
                key = (value, fase_score(shift_start_min[sid]), -shift_start_min[sid])
                if (best is None) or (key > best):
                    best = key; best_sid = sid
            if best_sid is None:
                infeasible.append((emp, f"Nessun turno compatibile in {day} (FORCED_ON)"))
                continue
            assignments.append((emp, day, best_sid))
            days_done[emp] += 1
            assigned_once.add((emp, day))
            # Aggiorna copertura e quota
            for s in shift_slots[best_sid]:
                current_coverage[day][s] += 1
                slot_quota[day][s] -= 1.0
            day_quota_units[day] -= shift_len_slots[best_sid]

    # 2) Loop principale: assegna seguendo la curva della domanda
    def remaining_people():
        return [e for e in ris['id dipendente'] if days_done[e] < work_need[e]]

    safety = 0
    while remaining_people() and safety < 20000:
        safety += 1
        progressed = False
        
        # Ordina i giorni per gap di copertura maggiore (domanda - copertura attuale)
        day_gaps = []
        for day in giorni:
            total_demand = sum(demand_by_slot[day].values())
            total_coverage = sum(current_coverage[day].values())
            gap = total_demand - total_coverage
            # Priorità extra per giorni feriali se hanno gap simile al weekend
            priority_boost = 1.0 if day in ['Lun','Mar','Mer','Gio','Ven'] else 0.0
            day_gaps.append((gap + priority_boost, day))
        
        day_gaps.sort(reverse=True)
        
        for _, day in day_gaps:
            cands = [e for e in remaining_people() if (e, day) not in assigned_once and day not in forced_off.get(e, set())]
            if not cands:
                continue
            
            best = None; best_emp = None; best_sid = None
            for emp in cands:
                for sid in shift_by_emp.get(emp, []):
                    value = shift_value_score(day, sid)
                    # Aggiungi fairness: chi ha più bisogno di lavorare ha priorità
                    need_priority = work_need[emp] - days_done[emp]
                    key = (value, need_priority, fase_score(shift_start_min[sid]), -shift_start_min[sid])
                    if (best is None) or (key > best):
                        best = key; best_emp = emp; best_sid = sid
            
            if best_emp is not None and best[0] > -50:  # Non assegnare se il punteggio è troppo negativo
                assignments.append((best_emp, day, best_sid))
                days_done[best_emp] += 1
                assigned_once.add((best_emp, day))
                # Aggiorna copertura e quota
                for s in shift_slots[best_sid]:
                    current_coverage[day][s] += 1
                    slot_quota[day][s] -= 1.0
                day_quota_units[day] -= shift_len_slots[best_sid]
                progressed = True
                if not remaining_people():
                    break
        
        if not progressed:
            # Se non riusciamo a progredire, forza l'assegnazione dove c'è più domanda
            for _, day in day_gaps[:3]:  # Prova solo i primi 3 giorni con più gap
                cands = [e for e in remaining_people() if (e, day) not in assigned_once and day not in forced_off.get(e, set())]
                if not cands:
                    continue
                # Prendi il primo disponibile con il turno migliore
                for emp in cands[:1]:
                    best_sid = None
                    best_score = -float('inf')
                    for sid in shift_by_emp.get(emp, []):
                        score = shift_value_score(day, sid)
                        if score > best_score:
                            best_score = score
                            best_sid = sid
                    if best_sid is not None:
                        assignments.append((emp, day, best_sid))
                        days_done[emp] += 1
                        assigned_once.add((emp, day))
                        for s in shift_slots[best_sid]:
                            current_coverage[day][s] += 1
                            slot_quota[day][s] -= 1.0
                        day_quota_units[day] -= shift_len_slots[best_sid]
                        progressed = True
                        break
                if progressed:
                    break
            
            if not progressed:
                # Ultima risorsa: assegna ovunque sia possibile
                for emp in remaining_people()[:1]:
                    for day in giorni:
                        if (emp, day) not in assigned_once and day not in forced_off.get(emp, set()):
                            if shift_by_emp.get(emp):
                                sid = shift_by_emp[emp][0]
                                assignments.append((emp, day, sid))
                                days_done[emp] += 1
                                assigned_once.add((emp, day))
                                for s in shift_slots[sid]:
                                    current_coverage[day][s] += 1
                                progressed = True
                                break
                    if progressed:
                        break
                if not progressed:
                    break

    # Riepilogo riposi/lavoro
    riposi_info = []
    for emp in ris['id dipendente']:
        D = int(rest_target_by_emp.get(emp, 2))
        actual_work = days_done[emp]
        actual_rest = 7 - actual_work
        dev = actual_rest - D
        riposi_info.append((emp, D, actual_rest, dev))

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
    parser = argparse.ArgumentParser(description='Pianificazione con distribuzione proporzionale alla domanda.')
    parser.add_argument('--input', required=True, help='Percorso al file Excel di input (.xlsx/.xlsm)')
    parser.add_argument('--out', default='tabella_turni.xlsx', help='File Excel di output')
    parser.add_argument('--grid', type=int, default=15, help='Griglia minuti per start turno (default 15)')
    parser.add_argument('--prefer_phase', type=str, default="15,45", help='Minuti preferiti per start (es. "15,45")')
    args = parser.parse_args()

    req, turni, ris = carica_dati(args.input)
    req_pre = prepara_req(req)

    durations_by_emp, rest_target_by_emp = infer_personal_params_from_risorse(ris)
    durations_set_min = set(durations_by_emp.values())

    turni_cand = genera_turni_candidati(req_pre, durations_set_min, grid_step_min=args.grid)
    shift_by_emp = determina_turni_ammissibili(ris, turni_cand, durations_by_emp)

    forced_off, forced_on = leggi_vincoli_weekend(ris)
    prefer_phases = tuple(int(x.strip()) for x in args.prefer_phase.split(',') if x.strip() != '')

    # USA LA NUOVA FUNZIONE
    assignments, riposi_info, infeasible, day_colmap, slot_list, slot_size, shift_slots = assegnazione_with_demand_proportional(
        req_pre.copy(), turni_cand, ris, shift_by_emp, rest_target_by_emp, durations_by_emp,
        forced_off, forced_on, prefer_phases=prefer_phases
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

    print(f'Salvato in {out_path}')


if __name__ == '__main__':
    main()