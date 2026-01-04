"""
generate_turni_tabella_finale_v5_0.py

Versione definitiva con TUTTI i fix:
- NO TURNI PRIMA DELLA DOMANDA: Blocca turni che iniziano prima dell'orario di domanda
- MINUTI FORZATI: PenalitÃƒÂ  MASSIVE per turni non :15/:45
- FIX DOMENICA: Non assegna turni quando domanda = 0
- Ottimizzato per capacitÃƒÂ  limitata (<5% overcapacity)

Uso:
  python generate_turni_tabella_finale_v5_0.py --input "input.xlsm" --out "output.xlsx" --grid 15 --prefer_phase "15,45"
  
  Opzioni:
  --force_phase : genera SOLO turni che iniziano ai minuti specificati
  --strict_phase : penalitÃƒÂ  ESTREMA per turni non ai minuti preferiti (consigliato)
"""

import argparse
import unicodedata
from pathlib import Path
import re
import datetime as _dt
from collections import defaultdict, Counter
import math
from typing import Dict, Optional, Set, Tuple
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
        return 0.0
    if isinstance(val, (int, float)):
        return float(val) if float(val) > 0 else 0.0
    s = str(val).strip().lower()
    m = re.search(r'(\d+[.,]?\d*)', s)
    if not m:
        return 0.0
    num = m.group(1).replace(',', '.')
    try:
        v = float(num)
        return v if v > 0 else 0.0
    except Exception:
        return 0.0


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


def _parse_weekly_hours_pattern(val) -> Optional[list[int]]:
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        v = float(val)
        return [int(round(v * 60))] if v > 0 else None
    s = str(val).strip()
    if not s:
        return None
    tokens = re.split(r"[-_/;\s]+", s)
    hours: list[int] = []
    for tok in tokens:
        if not tok:
            continue
        try:
            v = float(tok.replace(',', '.'))
        except ValueError:
            continue
        if v <= 0:
            continue
        hours.append(int(round(v * 60)))
    return hours or None


def _parse_positive_int(val) -> int:
    if pd.isna(val):
        return 0
    try:
        num = int(round(float(str(val).replace(',', '.'))))
        return max(0, num)
    except Exception:
        return 0


def _normalize(s: str) -> str:
    s = str(s or '').strip().lower()
    s = ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))
    s = re.sub(r'[^a-z]', '', s)
    return s


DAY_SYNONYMS = {
    'Lun': ['lun', 'lunedi', 'lunedÃƒÂ¬', 'mon', 'monday'],
    'Mar': ['mar', 'martedi', 'martedÃƒÂ¬', 'tue', 'tuesday'],
    'Mer': ['mer', 'mercoledi', 'mercoledÃƒÂ¬', 'wed', 'wednesday'],
    'Gio': ['gio', 'giovedi', 'giovedÃƒÂ¬', 'thu', 'thursday'],
    'Ven': ['ven', 'venerdi', 'venerdÃƒÂ¬', 'fri', 'friday'],
    'Sab': ['sab', 'sabato', 'sat', 'saturday'],
    'Dom': ['dom', 'domenica', 'sun', 'sunday'],
}

DAY_OVERCAP_PERCENT = {
    'Lun': 0.10,
    'Mar': 0.10,
    'Mer': 0.10,
    'Gio': 0.10,
    'Ven': 0.10,
    'Sab': 0.05,
    'Dom': 0.0,
}

DAY_OVERCAP_PENALTY = {
    'Lun': 1.0,
    'Mar': 1.0,
    'Mer': 1.0,
    'Gio': 1.0,
    'Ven': 1.0,
    'Sab': 1.2,
    'Dom': 1.8,
}

DEFAULT_OVERCAP_PERCENT = 0.10
DEFAULT_OVERCAP_PENALTY = 1.0


def _resolve_day_key(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    token = _normalize(s)
    if not token:
        return None
    for short, syns in DAY_SYNONYMS.items():
        if token == _normalize(short):
            return short
        for syn in syns:
            if token == _normalize(syn):
                return short
    return None


def _parse_overcap_spec(spec: Optional[str], value_name: str) -> Dict[str, float]:
    if not spec:
        return {}
    mapping: dict[str, float] = {}
    parts = [token.strip() for token in str(spec).split(',')]
    for token in parts:
        if not token:
            continue
        if '=' not in token:
            raise ValueError(f"Formato non valido per {value_name}: '{token}'. Atteso 'Gio=0.12'.")
        day_raw, value_raw = token.split('=', 1)
        key = _resolve_day_key(day_raw)
        if key is None:
            raise ValueError(f"Giorno sconosciuto in {value_name}: '{day_raw}'.")
        try:
            val = float(str(value_raw).replace(',', '.'))
        except ValueError as exc:
            raise ValueError(f"Valore numerico non valido in {value_name}: '{value_raw}'.") from exc
        mapping[key] = val
    return mapping


def load_overcap_settings(cfg_df: Optional[pd.DataFrame],
                          percent_override: Optional[Dict[str, float]] = None,
                          penalty_override: Optional[Dict[str, float]] = None) -> Tuple[Dict[str, float], Dict[str, float]]:
    percent_map: Dict[str, float] = dict(DAY_OVERCAP_PERCENT)
    penalty_map: Dict[str, float] = dict(DAY_OVERCAP_PENALTY)

    if cfg_df is not None and not cfg_df.empty:
        lower_cols = {str(c).strip().lower(): c for c in cfg_df.columns}
        day_col = None
        for name in ('giorno', 'day', 'giorni'):
            if name in lower_cols:
                day_col = lower_cols[name]
                break
        percent_col = None
        for name in ('overcap_percent', 'percentuale', 'percent', 'overcap'):
            if name in lower_cols:
                percent_col = lower_cols[name]
                break
        penalty_col = None
        for name in ('overcap_penalty', 'penalita', 'penalty'):
            if name in lower_cols:
                penalty_col = lower_cols[name]
                break

        if day_col is not None:
            for _, row in cfg_df.iterrows():
                key = _resolve_day_key(row.get(day_col))
                if not key:
                    continue
                if percent_col is not None:
                    val = row.get(percent_col)
                    if pd.notna(val):
                        try:
                            percent_map[key] = float(str(val).replace(',', '.'))
                        except ValueError:
                            pass
                if penalty_col is not None:
                    val = row.get(penalty_col)
                    if pd.notna(val):
                        try:
                            penalty_map[key] = float(str(val).replace(',', '.'))
                        except ValueError:
                            pass

    if percent_override:
        percent_map.update(percent_override)
    if penalty_override:
        penalty_map.update(penalty_override)
    return percent_map, penalty_map


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
def trova_fogli_requisiti(
    xls: pd.ExcelFile,
    skill_preferita: Optional[str] = None,
) -> list[Tuple[str, Optional[str]]]:
    """
    Identifica tutti i fogli requisiti validi.
    Ritorna una lista di tuple: (nome_foglio, nome_skill).
    Se skill_preferita è specificata, filtra solo quella.
    """
    skill_norm = _normalize(skill_preferita) if skill_preferita else None
    
    found = []
    
    # Cerca foglio esatto "Requisiti" (skill generica o unica)
    for name in xls.sheet_names:
        if name.strip().lower() == 'requisiti':
            if skill_norm:
                # Se l'utente voleva una skill specifica ma c'è solo "Requisiti" generico,
                # e non ci sono altri fogli, questo potrebbe essere un problema o un fallback.
                # Per ora lo ignoriamo se cerchiamo una skill specifica che non matcha "requisiti_skill"
                pass 
            else:
                found.append((name, None))
    
    # Cerca fogli "Requisiti_<skill>"
    for name in xls.sheet_names:
        lowered = name.strip().lower()
        suffix = None
        if lowered.startswith('requisiti'):
            suffix = lowered[len('requisiti'):]
        elif lowered.startswith('requisit'):
            suffix = lowered[len('requisit'):]
        
        if suffix:
            suffix_clean = suffix.lstrip(" _-").strip()
            if suffix_clean:
                # Abbiamo trovato una skill specifica
                if skill_norm:
                    if _normalize(suffix_clean) == skill_norm:
                        found.append((name, suffix_clean))
                else:
                    found.append((name, suffix_clean))

    # Se abbiamo trovato sia "Requisiti" generico che "Requisiti_X", e non è stata chiesta una skill specifica,
    # diamo priorità ai fogli specifici se esistono, oppure li ritorniamo tutti?
    # Logica: Se esistono skill specifiche, "Requisiti" potrebbe essere un residuo o una skill default.
    # Nel dubbio li ritorniamo tutti, sarà il main a gestire.
    
    if not found:
        # Nessun match
        available = ", ".join(xls.sheet_names)
        if skill_preferita:
            raise ValueError(
                f"Nessun foglio requisiti corrisponde alla skill richiesta '{skill_preferita}'. "
                f"Fogli disponibili: {available}"
            )
        else:
             raise ValueError(
                "Nessun foglio requisiti trovato (es. 'Requisiti' o 'Requisiti_Skill'). "
                f"Fogli disponibili: {available}"
            )

    # Rimuovi duplicati se capitasse
    found_unique = sorted(list(set(found)), key=lambda x: x[0])
    return found_unique


def _filtra_risorse_per_skill(ris: pd.DataFrame, skill_name: Optional[str]) -> pd.DataFrame:
    if not skill_name:
        return ris
    skill_col = _find_col(ris, 'skill')
    if skill_col is None and len(ris.columns) >= 19:
        skill_col = ris.columns[18]  # Colonna S
    if skill_col is None:
        raise ValueError("Nel foglio 'Risorse' manca la colonna 'skill' (colonna S).")
    target = _normalize(skill_name)
    mask = ris[skill_col].apply(lambda v: _normalize(v) == target)
    ris_filtrato = ris.loc[mask].copy()
    if ris_filtrato.empty:
        raise ValueError(f"Nessuna risorsa con skill '{skill_name}' nel foglio 'Risorse'.")
    return ris_filtrato


def _estrai_skill_unica_da_risorse(ris: pd.DataFrame) -> Optional[str]:
    skill_col = _find_col(ris, 'skill')
    if skill_col is None and len(ris.columns) >= 19:
        skill_col = ris.columns[18]  # Colonna S
    if skill_col is None:
        return None
    valori = (
        ris[skill_col]
        .dropna()
        .astype(str)
        .map(str.strip)
    )
    valori = valori[valori != '']
    if valori.empty:
        return None
    normalizzati = valori.map(_normalize)
    unici = normalizzati.unique()
    if len(unici) == 1:
        return valori.iloc[0]
    return None


def carica_dati_base(percorso_input: str):
    """
    Carica Risorse, Turni e Config, mantenendo aperto l'Excel per leggere i Requisiti dopo.
    Restituisce: (xls, turni, ris, cfg)
    """
    p = Path(percorso_input).expanduser()
    if not p.is_absolute():
        p = Path.cwd() / p
    if not p.exists():
        raise FileNotFoundError(f"File non trovato: {p}")
    if p.is_dir():
        raise IsADirectoryError(f"Il percorso è una CARTELLA, non un file: {p}")

    xls = pd.ExcelFile(p, engine='openpyxl')
    
    try:
        turni = pd.read_excel(xls, 'Turni')
    except (ValueError, KeyError):
        # Se manca il foglio Turni e non usiamo predefiniti, potrebbe non essere un problema gravissimo
        # ma di solito serve. Creiamo vuoto se serve? Meglio avvisare se crasha dopo.
        turni = pd.DataFrame()

    try:
        ris = pd.read_excel(xls, 'Risorse')
    except ValueError:
        raise ValueError("Foglio 'Risorse' mancante nel file Excel.")

    try:
        cfg = pd.read_excel(xls, 'config')
    except ValueError:
        cfg = None

    return xls, turni, ris, cfg


def carica_turni_predefiniti(turni: pd.DataFrame, slot_size: int = 15) -> pd.DataFrame:
    """
    Carica turni predefiniti dal foglio Turni, gestendo sia turni normali che spezzati.

    Struttura attesa foglio Turni:
    - Col A: id turno
    - Col B: entrata (inizio prima parte)
    - Col C: uscita (fine prima parte)
    - Col D: Inizio spezzato (inizio seconda parte, vuoto se non spezzato)
    - Col E: Fine spezzato (fine seconda parte, vuoto se non spezzato)
    - Col F: durata (ore totali)

    Returns:
        DataFrame con colonne: id turno, start_min, end_min, durata_min,
                              is_spezzato, spezzato_start_min, spezzato_end_min
    """
    rows = []

    # Trova colonne
    id_col = _find_col(turni, 'id turno')
    entrata_col = _find_col(turni, 'entrata')
    uscita_col = _find_col(turni, 'uscita')
    durata_col = _find_col(turni, 'durata')

    # Colonne spezzato (cerca varianti)
    spezz_ini_col = _find_col(turni, 'Inizio spezzato') or _find_col(turni, 'Inzio spezzato')
    spezz_fin_col = _find_col(turni, 'Fine spezzato')

    if id_col is None or entrata_col is None or uscita_col is None:
        raise ValueError("Foglio Turni deve avere colonne: 'id turno', 'entrata', 'uscita'")

    for _, row in turni.iterrows():
        turno_id = row[id_col]
        if pd.isna(turno_id) or str(turno_id).strip() == '':
            continue

        # Prima parte del turno
        start_min = _to_minutes(row[entrata_col])
        end_min = _to_minutes(row[uscita_col])

        # Durata
        if durata_col is not None and not pd.isna(row[durata_col]):
            durata_min = int(float(row[durata_col]) * 60)
        else:
            durata_min = end_min - start_min

        # Verifica se è spezzato
        is_spezzato = False
        spezz_start = None
        spezz_end = None

        if spezz_ini_col is not None and spezz_fin_col is not None:
            spezz_ini_val = row.get(spezz_ini_col)
            spezz_fin_val = row.get(spezz_fin_col)

            if not pd.isna(spezz_ini_val) and not pd.isna(spezz_fin_val):
                spezz_start = _to_minutes(spezz_ini_val)
                spezz_end = _to_minutes(spezz_fin_val)
                if spezz_start < 24*60 and spezz_end < 24*60:  # Validi
                    is_spezzato = True

        # end_min finale è la fine dell'ultima parte
        final_end = spezz_end if is_spezzato else end_min

        rows.append({
            'id turno': str(turno_id).strip(),
            'entrata_str': _from_minutes(start_min),
            'uscita_str': _from_minutes(final_end),
            'start_min': start_min,
            'end_min': final_end,
            'durata_min': durata_min,
            'is_spezzato': is_spezzato,
            'parte1_start': start_min,
            'parte1_end': end_min,
            'parte2_start': spezz_start if is_spezzato else None,
            'parte2_end': spezz_end if is_spezzato else None,
        })

    df = pd.DataFrame(rows)
    print(f"INFO: Caricati {len(df)} turni predefiniti ({sum(df['is_spezzato'])} spezzati)")
    return df


def calcola_slots_turno(turno_row, slot_size: int = 15) -> list:
    """
    Calcola la lista di slot coperti da un turno (considerando eventuali pause per spezzati).

    Per turno normale 09:00-13:00 con slot_size=15: [540, 555, 570, ..., 765]
    Per turno spezzato 09:00-13:00 + 14:00-18:00: [540, 555, ..., 765] + [840, 855, ..., 1065]
    """
    slots = []

    # Prima parte
    start1 = int(turno_row['parte1_start'])
    end1 = int(turno_row['parte1_end'])

    slot = start1
    while slot < end1:
        slots.append(slot)
        slot += slot_size

    # Seconda parte (se spezzato)
    if turno_row['is_spezzato'] and turno_row['parte2_start'] is not None:
        start2 = int(turno_row['parte2_start'])
        end2 = int(turno_row['parte2_end'])

        slot = start2
        while slot < end2:
            slots.append(slot)
            slot += slot_size

    return slots


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
    """
    Estrae i parametri personali dal foglio "Risorse", incluse le combinazioni
    di ore/giorno (colonna Q), le ore settimanali (colonna C) e il numero di
    giorni consentiti fuori fascia (colonna R), trasformandoli in vincoli
    utilizzabili durante la generazione dei turni.

    Restituisce durate base, riposi target, straordinari disponibili, flag di
    spezzamento OT, straordinari per giorno, pattern di durata e budget
    "fuori fascia" per ciascun dipendente.
    """
    if ris.shape[1] < 4:
        raise ValueError("Il foglio 'Risorse' deve avere almeno 4 colonne: A,B,C(ore/giorno),D(riposi/settimana).")
    hours_col = ris.columns[2]  # C
    rests_col = ris.columns[3]  # D
    ot_disp_col = _find_col(ris, 'OT disp')
    ot_split_col = _find_col(ris, 'OT spezz')
    pattern_col = _find_col(ris, 'combinazione')
    if pattern_col is None and len(ris.columns) >= 17:
        pattern_col = ris.columns[16]  # Colonna Q
    flex_col = _find_col(ris, 'fuori fascia')
    if flex_col is None and len(ris.columns) >= 18:
        flex_col = ris.columns[17]  # Colonna R
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
    duration_patterns_by_emp: Dict[str, list[int]] = {}
    out_of_range_allowance_by_emp: Dict[str, int] = {}
    weekly_mismatch = []

    for _, row in ris.iterrows():
        emp = row['id dipendente']
        weekly_hours = _parse_hours_cell(row[hours_col])
        target_rest = _parse_rest_count_cell(row[rests_col])
        rest_target_by_emp[emp] = target_rest
        work_days = max(0, 7 - target_rest)
        pattern_val = row.get(pattern_col) if pattern_col is not None else None
        pattern_hours = _parse_weekly_hours_pattern(pattern_val)
        if pattern_hours:
            duration_patterns_by_emp[emp] = pattern_hours
            base_duration = pattern_hours[0]
            if weekly_hours > 0:
                pattern_week = sum(pattern_hours) / 60.0
                if abs(pattern_week - weekly_hours) > 0.25:
                    weekly_mismatch.append((emp, weekly_hours, pattern_week))
            else:
                weekly_hours = sum(pattern_hours) / 60.0
        else:
            if weekly_hours <= 0:
                weekly_hours = 4.0 * max(1, work_days)
            if work_days <= 0:
                base_duration = int(round(weekly_hours * 60.0))
            else:
                base_duration = int(round((weekly_hours * 60.0) / work_days))
        if base_duration <= 0:
            base_duration = 240
        durations_by_emp[emp] = base_duration

        flex_val = row.get(flex_col) if flex_col is not None else None
        out_of_range_allowance_by_emp[emp] = _parse_positive_int(flex_val)

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
            split_flag = normalized in {'ok'}
        ot_split_by_emp[emp] = split_flag

    if weekly_mismatch:
        print("[WARN] Ore settimanali (colonna C) non coerenti con combinazione (colonna Q):")
        for emp, weekly_hours, pattern_week in weekly_mismatch:
            print(f"  {emp}: C={weekly_hours}h vs Q={pattern_week:.2f}h")

    return (
        durations_by_emp,
        rest_target_by_emp,
        ot_minutes_by_emp,
        ot_split_by_emp,
        ot_daily_minutes_by_emp,
        duration_patterns_by_emp,
        out_of_range_allowance_by_emp,
    )
def genera_turni_candidati(req_pre: pd.DataFrame, durations_set_min: set, grid_step_min: int, 
                          force_phase_minutes=None) -> pd.DataFrame:
    """
    Genera turni candidati con supporto per forzare specifici minuti di inizio.
    """
    min_start = int(req_pre['start_min'].min())
    max_end = int(req_pre['end_min'].max())
    step = int(grid_step_min) if grid_step_min and grid_step_min > 0 else 15
    if step <= 0:
        step = 15
    rows = []
    
    for dmin in sorted(durations_set_min):
        start = min_start
        # Allinea l'inizio al primo passo di griglia >= min_start
        rem = start % step
        if rem != 0:
            start += (step - rem)
        while start + dmin <= max_end:
            # Se force_phase_minutes ÃƒÂ¨ specificato, genera solo turni con quei minuti
            if force_phase_minutes is not None:
                start_minute = (start % 60)
                if start_minute not in force_phase_minutes:
                    start += step
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
            start += step
    
    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=['id turno', 'entrata_str', 'uscita_str', 'start_min', 'end_min', 'durata_min'])


def determina_turni_ammissibili(
    ris: pd.DataFrame,
    turni_cand: pd.DataFrame,
    durations_by_emp: dict,
    allowed_durations_by_emp: Optional[Dict[str, Set[int]]] = None,
    out_of_range_allowance_by_emp: Optional[Dict[str, int]] = None,
):
    days = ['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom']
    day_abbr = {
        'Lun': 'lun',
        'Mar': 'mar',
        'Mer': 'mer',
        'Gio': 'gio',
        'Ven': 'ven',
        'Sab': 'sab',
        'Dom': 'dom',
    }

    def _find_day_col(prefix: str, day: str) -> Optional[str]:
        name = f"{prefix} fascia {day_abbr[day]}"
        col = _find_col(ris, name)
        if col is not None:
            return col
        target = re.sub(r'[\s_]+', ' ', name.strip().lower())
        for c in ris.columns:
            norm = re.sub(r'[\s_]+', ' ', str(c).strip().lower())
            if norm == target:
                return c
        return None

    fin_col = _find_col(ris, 'Fine fascia')
    ini_col = _find_col(ris, 'Inizio fascia')
    day_cols = {day: (_find_day_col('inizio', day), _find_day_col('fine', day)) for day in days}
    has_day_cols = any(col for pair in day_cols.values() for col in pair)
    if fin_col is None and not has_day_cols:
        raise ValueError("Nel foglio 'Risorse' manca la colonna 'Fine fascia' e non ci sono override giornalieri.")

    turni_meta = {
        row['id turno']: (int(row['start_min']), int(row['end_min']), int(row['durata_min']))
        for _, row in turni_cand.iterrows()
    }
    avail_ini_min = {}
    avail_end_min = {}
    avail_ini_min_day = {}
    avail_end_min_day = {}
    shift_by_emp = {}
    shift_by_emp_day = {}
    shift_out_of_range = {}
    shift_out_of_range_day = {}
    forced_off_day = defaultdict(set)
    allowed_durations_by_emp = allowed_durations_by_emp or {}
    out_of_range_allowance_by_emp = out_of_range_allowance_by_emp or {}

    for _, row in ris.iterrows():
        emp = row['id dipendente']
        generic_start = _to_minutes(row[ini_col]) if ini_col is not None else 0
        generic_end = _to_minutes(row[fin_col]) if fin_col is not None else 24 * 60
        avail_ini_min[emp] = generic_start
        avail_end_min[emp] = generic_end
        durations_allowed = {int(d) for d in allowed_durations_by_emp.get(emp, {durations_by_emp.get(emp, 240)})}
        include_outside = out_of_range_allowance_by_emp.get(emp, 0) > 0

        union_list = []
        union_set = set()

        for day in days:
            day_ini_col, day_fin_col = day_cols[day]
            day_ini_val = row.get(day_ini_col) if day_ini_col is not None else None
            day_fin_val = row.get(day_fin_col) if day_fin_col is not None else None

            if (day_ini_col is not None and _parse_flag(day_ini_val) == 'off') or (
                day_fin_col is not None and _parse_flag(day_fin_val) == 'off'
            ):
                forced_off_day[emp].add(day)
                avail_ini_min_day[(emp, day)] = generic_start
                avail_end_min_day[(emp, day)] = generic_end
                shift_by_emp_day[(emp, day)] = []
                continue

            day_start = None
            day_end = None
            if day_ini_col is not None and pd.notna(day_ini_val) and str(day_ini_val).strip() != '':
                day_start = _to_minutes(day_ini_val)
            if day_fin_col is not None and pd.notna(day_fin_val) and str(day_fin_val).strip() != '':
                day_end = _to_minutes(day_fin_val)

            if day_start is None:
                day_start = generic_start
            if day_end is None:
                day_end = generic_end

            avail_ini_min_day[(emp, day)] = day_start
            avail_end_min_day[(emp, day)] = day_end

            allowed_list = []
            for sid, (st_min, en_min, duration) in turni_meta.items():
                if duration not in durations_allowed:
                    continue
                inside = st_min >= day_start and en_min <= day_end
                if include_outside or inside:
                    allowed_list.append(sid)
                    shift_out_of_range_day[(emp, day, sid)] = not inside
                    if sid not in union_set:
                        union_set.add(sid)
                        union_list.append(sid)

            shift_by_emp_day[(emp, day)] = allowed_list

        shift_by_emp[emp] = union_list
        for sid in union_list:
            st_min, en_min, _ = turni_meta.get(sid, (0, 0, 0))
            inside = st_min >= generic_start and en_min <= generic_end
            shift_out_of_range[(emp, sid)] = not inside

    return (
        shift_by_emp,
        shift_by_emp_day,
        shift_out_of_range,
        shift_out_of_range_day,
        avail_ini_min,
        avail_end_min,
        avail_ini_min_day,
        avail_end_min_day,
        forced_off_day,
    )


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
    if s in {'lavoro', 'on', 'si', 'sÃƒÂ¬', '1', 'work', 'w'}:
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

def compute_tight_capacity_targets(
    req_pre: pd.DataFrame,
    ris: pd.DataFrame,
    durations_by_emp: dict,
    rest_target_by_emp: dict,
    forced_off: dict,
    overcap_percent_map: Dict[str, float],
    overcap_penalty_map: Dict[str, float],
    duration_patterns_by_emp: Optional[Dict[str, list[int]]] = None,
):
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
            col_series = pd.to_numeric(req_pre[col], errors='coerce').fillna(0.0)
            day_demand = float(col_series.sum())
            demand_by_day[g] = day_demand
            total_demand += day_demand

            if day_demand == 0:
                zero_demand_days.add(g)
                min_demand_start[g] = 24 * 60
                max_demand_end[g] = 0
            else:
                fasce_con_domanda = req_pre[col_series > 0]
                if not fasce_con_domanda.empty:
                    min_demand_start[g] = int(fasce_con_domanda['start_min'].min())
                    max_demand_end[g] = int(fasce_con_domanda['end_min'].max())
                else:
                    min_demand_start[g] = 24 * 60
                    max_demand_end[g] = 0

            for i, slot in enumerate(slot_list):
                demand_by_slot[g][slot] = float(col_series.iat[i]) if i < len(col_series) else 0.0
        else:
            demand_by_day[g] = 0.0
            zero_demand_days.add(g)
            min_demand_start[g] = 24 * 60
            max_demand_end[g] = 0
            for slot in slot_list:
                demand_by_slot[g][slot] = 0.0

    total_capacity_slots = 0.0
    emp_work_days = {}
    patterns = duration_patterns_by_emp or {}
    for emp in ris['id dipendente']:
        target_rest = int(rest_target_by_emp.get(emp, 2))
        forced = len(forced_off.get(emp, set()))
        if forced > target_rest:
            raise ValueError(f"Vincoli incoerenti per {emp}: riposi richiesti={target_rest}, ma giorni forzati OFF={forced}.")
        work_days = len(patterns[emp]) if emp in patterns else max(0, 7 - target_rest)
        emp_work_days[emp] = work_days
        if emp in patterns:
            total_capacity_slots += sum(d / slot_size for d in patterns[emp])
        else:
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

    overcap_limit = {}
    overcap_penalty = {}
    for g in giorni:
        pct = overcap_percent_map.get(g, DEFAULT_OVERCAP_PERCENT)
        limit = max(0.0, demand_by_day.get(g, 0.0) * pct) if g not in zero_demand_days else 0.0
        overcap_limit[g] = limit
        overcap_penalty[g] = overcap_penalty_map.get(g, DEFAULT_OVERCAP_PENALTY)

    return {
        'demand_by_day': demand_by_day,
        'demand_by_slot': demand_by_slot,
        'total_demand': total_demand,
        'total_capacity': total_capacity_slots,
        'overcapacity_ratio': overcapacity_ratio,
        'day_weights': day_weights,
        'overcap_limit': overcap_limit,
        'overcap_penalty': overcap_penalty,
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
    shift_by_emp_day: dict,
    rest_target_by_emp: dict,
    durations_by_emp: dict,
    forced_off: dict,
    forced_on: dict,
    ot_minutes_by_emp: dict,
    ot_split_by_emp: dict,
    ot_daily_minutes_by_emp: dict,
    allow_ot_overcap: bool = False,
    prefer_phases=(15, 45),
    strict_phase=False,
    force_balance: bool = False,
    overcap_percent_map: Optional[Dict[str, float]] = None,
    overcap_penalty_map: Optional[Dict[str, float]] = None,
    duration_patterns_by_emp: Optional[Dict[str, list[int]]] = None,
    allowed_durations_by_emp: Optional[Dict[str, Set[int]]] = None,
    shift_out_of_range: Optional[Dict[Tuple[str, str], bool]] = None,
    shift_out_of_range_day: Optional[Dict[Tuple[str, str, str], bool]] = None,
    out_of_range_allowance_by_emp: Optional[Dict[str, int]] = None,
    weekend_guard: bool = False,
    weekend_overcap_max: Optional[float] = None,
    uniform_overcap: bool = False,
    uniform_overcap_tol: Optional[float] = None,
):
    '''Assegna i turni rispettando domanda, riposi contrattuali e privilegiando i minuti preferiti.'''
    giorni = ['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom']
    force_balance = bool(force_balance)
    weekend_guard = bool(weekend_guard)
    uniform_overcap = bool(uniform_overcap)
    if weekend_overcap_max is not None:
        try:
            weekend_overcap_max = float(weekend_overcap_max)
        except (TypeError, ValueError):
            weekend_overcap_max = None
    if weekend_overcap_max is not None and weekend_overcap_max <= 0:
        weekend_overcap_max = None
    if uniform_overcap_tol is not None:
        try:
            uniform_overcap_tol = float(uniform_overcap_tol)
        except (TypeError, ValueError):
            uniform_overcap_tol = None
    if uniform_overcap_tol is not None and uniform_overcap_tol <= 0:
        uniform_overcap_tol = None
    demand_overlap_guard = weekend_guard
    percent_map = dict(overcap_percent_map) if overcap_percent_map else dict(DAY_OVERCAP_PERCENT)
    penalty_map = dict(overcap_penalty_map) if overcap_penalty_map else dict(DAY_OVERCAP_PENALTY)
    forced_records: Set[Tuple[str, str]] = set()

    targets = compute_tight_capacity_targets(
        req,
        ris,
        durations_by_emp,
        rest_target_by_emp,
        forced_off,
        percent_map,
        penalty_map,
        duration_patterns_by_emp,
    )
    demand_by_slot = targets['demand_by_slot']
    demand_by_day = targets['demand_by_day']
    total_demand = targets['total_demand']
    overcapacity_ratio = targets['overcapacity_ratio']
    uniform_overcap_active = uniform_overcap and overcapacity_ratio > 0
    uniform_target_ratio = 1.0 + max(0.0, overcapacity_ratio)
    day_weights = targets['day_weights']
    overcap_limit = targets.get('overcap_limit', {g: 0.0 for g in giorni})
    overcap_penalty = targets.get('overcap_penalty', {g: 1.0 for g in giorni})
    emp_work_days = targets['emp_work_days']
    slot_list = targets['slot_list']
    slot_size = targets['slot_size']
    day_colmap = targets['day_colmap']
    zero_demand_days = targets['zero_demand_days']
    min_demand_start = targets['min_demand_start']
    max_demand_end = targets['max_demand_end']

    duration_patterns_by_emp = duration_patterns_by_emp or {}
    allowed_durations_by_emp = allowed_durations_by_emp or {
        emp: {durations_by_emp.get(emp, 240)} for emp in ris['id dipendente']
    }
    all_durations = sorted({dur for durs in allowed_durations_by_emp.values() for dur in durs})
    typical_duration = all_durations[0] if all_durations else next(iter(durations_by_emp.values()))
    slot_coverage = max(1, int(round(typical_duration / slot_size)))
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
    shift_duration_min = dict(zip(turni_cand['id turno'], turni_cand['durata_min']))

    # Calcola slot coperti per ogni turno (gestisce turni spezzati)
    shift_slots = {}
    for _, row in turni_cand.iterrows():
        turno_id = row['id turno']
        if 'is_spezzato' in row and row['is_spezzato']:
            # Turno spezzato: usa calcola_slots_turno che gestisce le pause
            all_slots = calcola_slots_turno(row, slot_size)
            # Filtra solo gli slot presenti nella slot_list
            shift_slots[turno_id] = [s for s in all_slots if s in slot_list]
        else:
            # Turno continuo: logica originale
            shift_slots[turno_id] = [s for s in slot_list if row['start_min'] <= s < row['end_min']]

    shift_by_emp_day = shift_by_emp_day or {}
    shift_out_of_range = shift_out_of_range or {}
    shift_out_of_range_day = shift_out_of_range_day or {}
    out_of_range_allowance_by_emp = out_of_range_allowance_by_emp or {}

    def get_shifts(emp: str, day: Optional[str] = None):
        if day is not None and shift_by_emp_day:
            return shift_by_emp_day.get((emp, day), [])
        return shift_by_emp.get(emp, [])

    def is_out_of_range(emp: str, day: Optional[str], sid: str) -> bool:
        if day is not None and shift_out_of_range_day:
            return shift_out_of_range_day.get((emp, day, sid), False)
        return shift_out_of_range.get((emp, sid), False)

    def shift_overlap_info(day: str, sid: str) -> Tuple[float, int, int]:
        slots = shift_slots.get(sid, [])
        if not slots:
            return 0.0, 0, 0
        with_demand = sum(1 for s in slots if demand_by_slot[day].get(s, 0.0) > 0)
        total = len(slots)
        ratio = with_demand / total if total else 0.0
        return ratio, with_demand, total

    def pick_shift_covering_slot(
        emp: str,
        day: str,
        target_slot: int,
        min_overlap: float = 0.3,
        require_demand: bool = True,
    ) -> Optional[str]:
        best_sid = None
        best_score = None
        for sid in get_shifts(emp, day):
            if target_slot not in shift_slots.get(sid, []):
                continue
            ratio, with_demand, total = shift_overlap_info(day, sid)
            if require_demand:
                if with_demand == 0:
                    continue
                if total > 0 and ratio < min_overlap:
                    continue
            # Prefer shifts mostly inside demand and aligned with the target slot.
            score = (ratio, with_demand, -abs(shift_start_min[sid] - target_slot), -shift_start_min[sid])
            if best_score is None or score > best_score:
                best_score = score
                best_sid = sid
        return best_sid

    def pick_shift_best_overlap(
        emp: str,
        day: str,
        prefer_sid: Optional[str] = None,
        require_demand: bool = True,
    ) -> Optional[str]:
        shifts = get_shifts(emp, day)
        if prefer_sid and prefer_sid in shifts:
            ratio, with_demand, _ = shift_overlap_info(day, prefer_sid)
            if with_demand > 0 or not require_demand:
                return prefer_sid
        best_sid = None
        best_score = None
        for sid in shifts:
            ratio, with_demand, total = shift_overlap_info(day, sid)
            if require_demand and with_demand == 0:
                continue
            score = (ratio, with_demand, -shift_start_min[sid])
            if best_score is None or score > best_score:
                best_score = score
                best_sid = sid
        return best_sid

    assignment_details = {}

    days_done = {emp: 0 for emp in ris['id dipendente']}
    work_need = emp_work_days.copy()
    forced_off = {emp: set(days) for emp, days in forced_off.items()}
    remaining_duration_counts: Dict[str, Counter] = {}
    for emp in ris['id dipendente']:
        pattern = duration_patterns_by_emp.get(emp)
        if pattern:
            # Il pattern non impone l'ordine dei giorni: qui memorizziamo solo quante
            # volte va usata ciascuna durata (es. un 5h e quattro 4h), mentre la
            # scelta di quando usarla avverrà più avanti in base ai gap di domanda
            # e ai punteggi di turno.
            remaining_duration_counts[emp] = Counter(pattern)
        else:
            remaining_duration_counts[emp] = Counter({durations_by_emp.get(emp, 240): work_need.get(emp, 0)})
    expected_duration_counts = {emp: counts.copy() for emp, counts in remaining_duration_counts.items()}
    out_of_range_allowance = {emp: int(out_of_range_allowance_by_emp.get(emp, 0)) for emp in ris['id dipendente']}
    out_of_range_used = defaultdict(int)

    for emp in ris['id dipendente']:
        for day in zero_demand_days:
            forced_off.setdefault(emp, set()).add(day)

    infeasible = []
    assignments = []
    assigned_once = set()
    assignments_by_emp = {emp: {} for emp in ris['id dipendente']}
    current_coverage = {g: {s: 0 for s in slot_list} for g in giorni}
    slot_overcap = {g: {s: 0.0 for s in slot_list} for g in giorni}
    day_overcap_used = {g: 0.0 for g in giorni}
    day_assignments_count = {g: 0 for g in giorni}
    minute_distribution = defaultdict(int)
    weekend_work = {emp: set() for emp in ris['id dipendente']}
    split_flags = {emp: bool(ot_split_by_emp.get(emp, False)) for emp in ris['id dipendente']}
    slot_set = set(slot_list)

    def day_total_coverage(day: str) -> float:
        return float(sum(current_coverage[day].values()))

    def day_coverage_ratio(day: str, extra_slots: int = 0) -> float:
        demand = demand_by_day.get(day, 0.0)
        if demand <= 0:
            return 0.0
        return (day_total_coverage(day) + extra_slots) / demand

    def update_coverage(day: str, slot: int, delta: float) -> None:
        if slot not in current_coverage[day]:
            current_coverage[day][slot] = 0
        if slot not in slot_overcap[day]:
            slot_overcap[day][slot] = 0.0
        prev_cov = current_coverage[day][slot]
        new_cov = prev_cov + delta
        current_coverage[day][slot] = new_cov
        demand = demand_by_slot[day].get(slot, 0.0)
        prev_over = slot_overcap[day][slot]
        new_over = max(0.0, new_cov - demand)
        slot_overcap[day][slot] = new_over
        day_overcap_used[day] += (new_over - prev_over)
        if day_overcap_used[day] < 1e-8 and day_overcap_used[day] > -1e-8:
            day_overcap_used[day] = 0.0

    def day_has_positive_gap(day: str) -> bool:
        for slot in slot_list:
            demand = demand_by_slot[day].get(slot, 0.0)
            if demand - current_coverage[day][slot] > 0.1:
                return True
        return False

    def weekend_overcovered(day: str) -> bool:
        if not weekend_guard:
            return False
        if day not in weekend_days:
            return False
        if demand_by_day.get(day, 0.0) <= 0:
            return True
        if day_has_positive_gap(day):
            return False
        # Avoid piling extra shifts on weekend once fully covered.
        total_demand = demand_by_day.get(day, 0.0)
        total_coverage = sum(current_coverage[day].values())
        if weekend_overcap_max is not None:
            allowed_ratio = 1.0 + max(0.0, weekend_overcap_max) + 0.05
        else:
            allowed_ratio = 1.0 + max(0.0, percent_map.get(day, DEFAULT_OVERCAP_PERCENT)) + 0.05
            if uniform_overcap_active:
                tol = uniform_overcap_tol if uniform_overcap_tol is not None else 0.05
                allowed_ratio = max(allowed_ratio, uniform_target_ratio + tol)
        return total_coverage > total_demand * allowed_ratio

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

    def weekend_bonus(emp: str, day: str, projected_overcap: float) -> float:
        if day not in weekend_days:
            return 0.0
        limit = overcap_limit.get(day, 0.0)
        if limit <= 0.0:
            capacity_factor = 0.0 if projected_overcap > 0.1 else 1.0
        else:
            capacity_factor = max(0.0, min(1.0, (limit - projected_overcap) / limit))
        if capacity_factor <= 0.0:
            return 0.0
        already = len(weekend_work[emp])
        if already == 0:
            base = 45.0
        else:
            # VINCOLO HARD: Se emp ha già 1+ weekend, verifica se ci sono alternative
            others = any(
                len(weekend_work[o]) == 0 and days_done[o] < work_need[o] and day not in forced_off.get(o, set())
                for o in ris['id dipendente'] if o != emp
            )
            if others:
                # PENALITÀ MASSICCIA: Blocco quasi totale del secondo weekend
                # Aumentato da -180/-110 a -5000 per evitare weekend consecutivi
                base = -5000.0
            elif already >= 2:
                # Già ha entrambi i weekend: blocco totale
                base = -10000.0
            else:
                # Nessun altro disponibile MA ha già un weekend
                base = -500.0  # Aumentato da -60
        return base * capacity_factor

    def can_use_shift(emp: str, sid: str, day: Optional[str] = None) -> bool:
        duration = int(shift_duration_min.get(sid, shift_end_min[sid] - shift_start_min[sid]))
        if remaining_duration_counts.get(emp, Counter()).get(duration, 0) <= 0:
            return False
        if is_out_of_range(emp, day, sid):
            return out_of_range_used[emp] < out_of_range_allowance.get(emp, 0)
        return True

    def shift_value(emp: str, day: str, sid: str, allow_overcapacity: bool = False, force_any: bool = False) -> float:
        if day in zero_demand_days and not force_any:
            return -1e4
        st = shift_start_min[sid]
        en = shift_end_min[sid]
        mds = min_demand_start.get(day, 24 * 60)
        if not force_any and mds < 24 * 60 and st < mds - 15:
            return -1e4
        if not force_any and en <= mds:
            return -1e4

        # VINCOLO HARD: BLOCCA turni che non si estendono fino alla fine del periodo con domanda
        # Questo impedisce che turni tipo 16:45-20:45 vengano selezionati quando ci sono requisiti fino alle 21:00
        mde = max_demand_end.get(day, 24 * 60)
        score = 0.0
        if is_out_of_range(emp, day, sid):
            remaining_flex = out_of_range_allowance.get(emp, 0) - out_of_range_used.get(emp, 0)
            if remaining_flex <= 0 and not force_any:
                return -1e4
            score -= 140.0
        if not force_any and mde < 24 * 60:
            if en >= mde:
                score += 90.0  # piccolo bonus per chi copre l'ultima fascia
            else:
                missing = mde - en
                if 0 < missing <= slot_size:
                    score -= 220.0  # forte penalità per mancare l'ultima fascia
                elif missing <= 2 * slot_size:
                    score -= 80.0

        # VINCOLO HARD: BLOCCA completamente turni che coprono anche 1 solo slot a zero requisiti
        if not force_any:
            for slot in shift_slots.get(sid, []):
                demand = demand_by_slot[day].get(slot, 0.0)
                if demand <= 0.0:
                    # BLOCCO ASSOLUTO: MAI assegnare turni su slot a zero requisiti
                    return -1e10

        extra_overcap = 0.0
        effective_allow = allow_overcapacity or force_any
        for slot in shift_slots.get(sid, []):
            demand = demand_by_slot[day].get(slot, 0.0)
            current = current_coverage[day][slot]
            prev_over = slot_overcap[day].get(slot, 0.0)
            projected_over = max(0.0, (current + 1) - demand)
            extra_overcap += (projected_over - prev_over)
            gap = demand - current
            if gap > 0:
                score += min(gap, 1.0) * 140.0
            else:
                penalty = abs(gap)
                damp = 18.0 if effective_allow else 42.0
                score -= penalty * damp
        projected_overcap = day_overcap_used[day] + extra_overcap
        limit = overcap_limit.get(day, 0.0)
        if limit <= 0.0:
            excess = projected_overcap
        else:
            excess = max(0.0, projected_overcap - limit)
        if excess > 1e-6:
            penalty_factor = overcap_penalty.get(day, DEFAULT_OVERCAP_PENALTY)
            damp_factor = 0.45 if effective_allow else 1.0
            if weekend_guard and day in weekend_days and not day_has_positive_gap(day):
                damp_factor = 1.0
            score -= excess * 220.0 * penalty_factor * damp_factor
        elif limit > 0.0:
            buffer_room = max(0.0, min(limit, limit - projected_overcap))
            if buffer_room > 0:
                score += min(buffer_room, 2.0) * 6.0
        score += minute_preference_score(st)
        score += day_balance_bonus(day)
        if uniform_overcap_active and day not in zero_demand_days:
            extra_slots = len(shift_slots.get(sid, []))
            projected_ratio = day_coverage_ratio(day, extra_slots)
            tol = uniform_overcap_tol if uniform_overcap_tol is not None else 0.05
            diff = projected_ratio - uniform_target_ratio
            if diff > tol:
                score -= (diff - tol) * 180.0
            elif diff < -tol:
                score += (-diff - tol) * 180.0
        score += weekend_bonus(emp, day, projected_overcap)
        if force_any and score <= -1e4 + 1e-3:
            score = -9000.0
        return score

    def apply_assignment(emp: str, day: str, sid: str, forced: bool = False) -> None:
        # SAFETY CHECK: VINCOLO HARD - Non violare riposi obbligatori
        if days_done[emp] >= work_need[emp]:
            error_msg = f"ERRORE CRITICO: Tentativo di assegnare {emp} al giorno {day} ma ha già raggiunto i giorni massimi ({work_need[emp]}). Riposi obbligatori violati!"
            print(f"\n[!]  {error_msg}")
            infeasible.append((emp, error_msg))
            return  # NON assegnare

        if demand_overlap_guard and demand_by_day.get(day, 0.0) > 0:
            ratio, with_demand, _ = shift_overlap_info(day, sid)
            if with_demand == 0:
                alt_sid = pick_shift_best_overlap(emp, day, require_demand=True)
                if alt_sid is None or not can_use_shift(emp, alt_sid, day):
                    infeasible.append((emp, f"Nessun turno con domanda utile per {day}"))
                    return
                sid = alt_sid

        duration_needed = int(shift_duration_min.get(sid, shift_end_min[sid] - shift_start_min[sid]))
        if remaining_duration_counts.get(emp, Counter()).get(duration_needed, 0) <= 0:
            infeasible.append((emp, f"Durata non disponibile per il pattern settimanale: {duration_needed}min"))
            return

        out_of_range_flag = is_out_of_range(emp, day, sid)
        if out_of_range_flag and out_of_range_used[emp] >= out_of_range_allowance.get(emp, 0):
            infeasible.append((emp, 'Limite giorni fuori fascia superato'))
            return

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
            'forced': forced,
            'out_of_range': out_of_range_flag,
        }
        if forced:
            forced_records.add((emp, day))
        remaining_duration_counts[emp][duration_needed] -= 1
        if out_of_range_flag:
            out_of_range_used[emp] += 1
        days_done[emp] += 1
        assigned_once.add((emp, day))
        day_assignments_count[day] += 1
        minute_distribution[shift_start_min[sid] % 60] += 1
        if day in weekend_days:
            weekend_work[emp].add(day)
        for slot in shift_slots.get(sid, []):
            update_coverage(day, slot, 1)

    def remove_assignment(emp: str, day: str):
        sid = assignments_by_emp.get(emp, {}).pop(day, None)
        if sid is None:
            return None
        meta = assignment_details.pop((emp, day), None)
        if meta and meta.get('forced'):
            forced_records.discard((emp, day))
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
        duration = int(shift_duration_min.get(sid, shift_end_min[sid] - shift_start_min[sid]))
        remaining_duration_counts[emp][duration] += 1
        if meta and meta.get('out_of_range'):
            out_of_range_used[emp] = max(0, out_of_range_used[emp] - 1)
        for slot in shift_slots.get(sid, []):
            update_coverage(day, slot, -1)
        if meta:
            for extra_slot in meta.get('extra_slots', set()):
                if extra_slot in current_coverage[day]:
                    update_coverage(day, extra_slot, -1)
        return sid

    def remaining_people():
        return [e for e in ris['id dipendente'] if days_done[e] < work_need[e]]

    def min_weekend_remaining():
        rem = [e for e in ris['id dipendente'] if days_done[e] < work_need[e]]
        if not rem:
            return 0
        return min(len(weekend_work[e]) for e in rem)

    def pick_force_assignment(emp: str):
        if not force_balance:
            return None, None
        best = None
        best_day = None
        best_sid = None
        weekdays = [g for g in giorni if g not in weekend_days and g not in zero_demand_days]
        weekends = [g for g in giorni if g in weekend_days and g not in zero_demand_days]
        ordered_days = weekdays + weekends + [g for g in giorni if g in zero_demand_days]
        force_on_days = forced_on.get(emp, set())
        for day in ordered_days:
            if day in forced_off.get(emp, set()):
                continue
            if (emp, day) in assigned_once:
                continue
            if weekend_overcovered(day):
                continue
            for sid in get_shifts(emp, day):
                if not can_use_shift(emp, sid, day):
                    continue
                ratio, with_demand, _ = shift_overlap_info(day, sid)
                if demand_overlap_guard and with_demand == 0:
                    continue
                val = shift_value(emp, day, sid, allow_overcapacity=True, force_any=force_balance)
                if demand_overlap_guard:
                    # Prefer shifts that overlap demand even in forced balancing.
                    val -= (1.0 - ratio) * 120.0
                if val <= -1e4:
                    continue
                if day in force_on_days:
                    val += 220.0
                key = (val, -shift_start_min[sid])
                if best is None or key > best:
                    best = key
                    best_day = day
                    best_sid = sid
        return best_day, best_sid

    giorni_validi = [g for g in giorni if g not in zero_demand_days]

    # Validazione input: verifica che ogni slot con domanda > 0 abbia almeno un dipendente disponibile
    validation_warnings = []
    for day in giorni_validi:
        for slot in slot_list:
            demand = demand_by_slot[day].get(slot, 0.0)
            if demand <= 0:
                continue

            # Verifica se almeno un dipendente ha un turno che copre questo slot
            has_coverage_potential = False
            for emp in ris['id dipendente']:
                if day in forced_off.get(emp, set()):
                    continue
                for sid in get_shifts(emp, day):
                    if slot in shift_slots.get(sid, []):
                        has_coverage_potential = True
                        break
                if has_coverage_potential:
                    break

            if not has_coverage_potential:
                slot_time = _from_minutes(slot)
                validation_warnings.append(
                    f"CRITICO: {day} fascia {slot_time} ha domanda {demand:.1f} ma NESSUN dipendente disponibile in quella fascia oraria"
                )

    if validation_warnings:
        print("\n[!]  VALIDAZIONE INPUT - PROBLEMI RILEVATI:")
        for warn in validation_warnings:
            print(f"  * {warn}")
        print("\n[INFO] SUGGERIMENTO: Verifica il foglio Risorse e assicurati che ci siano dipendenti con disponibilità oraria (Inizio/Fine fascia) che coprano TUTTE le fasce con requisiti > 0\n")

    for day in giorni_validi:
        must = [emp for emp in ris['id dipendente'] if day in forced_on.get(emp, set())]
        for emp in sorted(must, key=lambda e: days_done[e]):
            if days_done[emp] >= work_need[emp]:
                infeasible.append((emp, f"FORCED_ON su {day} supera i giorni lavorativi consentiti"))
                continue
            best = None
            best_sid = None
            for sid in get_shifts(emp, day):
                if not can_use_shift(emp, sid, day):
                    continue
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
                    # PRIORITÀ PROPORZIONALE: chi è più scoperto in % ha priorità
                    coverage_ratio = current / demand  # 0.0 = scoperto, 1.0 = coperto
                    gap_absolute = demand - current

                    # Bonus weekend per parità di copertura
                    day_bonus = 0.15 if day == 'Dom' else 0.10 if day == 'Sab' else 0.0

                    # Priorità inversamente proporzionale alla copertura
                    # (1 - coverage_ratio) = quanto manca in percentuale
                    priority = (1.0 - coverage_ratio) + day_bonus

                    critical_gaps.append((priority, gap_absolute, day, slot))
        critical_gaps.sort(reverse=True)

        progressed = False
        # Aumentato da 20 a 100 per considerare più gap critici (es. ultima fascia con domanda bassa)
        for _, _, day, target_slot in critical_gaps[:100]:
            candidates = []
            for emp in remaining_people():
                if (emp, day) in assigned_once or day in forced_off.get(emp, set()):
                    continue
                for sid in get_shifts(emp, day):
                    if not can_use_shift(emp, sid, day):
                        continue
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
                if weekend_overcovered(day):
                    continue
                for sid in get_shifts(emp, day):
                    if not can_use_shift(emp, sid, day):
                        continue
                    val = shift_value(emp, day, sid, allow_overcapacity=True)
                    if val <= -1e4:
                        continue
                    # Calcola priorità basata sulla copertura percentuale del giorno
                    day_demand = sum(demand_by_slot[day].values())
                    day_current = sum(current_coverage[day].values())
                    coverage_ratio = day_current / day_demand if day_demand > 0 else 1.0

                    # Bonus per giorno più scoperto + piccolo bonus weekend
                    day_bonus = (1.0 - coverage_ratio) * 100
                    if day == 'Dom':
                        day_bonus += 20
                    elif day == 'Sab':
                        day_bonus += 15

                    key = (val + day_bonus, -shift_start_min[sid])
                    if best is None or key > best:
                        best = key
                        best_day = day
                        best_sid = sid
            if best_sid is not None:
                apply_assignment(emp, best_day, best_sid)
                assigned_flag = True
            else:
                if force_balance:
                    fallback_day, fallback_sid = pick_force_assignment(emp)
                    if fallback_sid is not None:
                        apply_assignment(emp, fallback_day, fallback_sid, forced=True)
                        assigned_flag = True
                    else:
                        infeasible.append((emp, 'Impossibile assegnare il numero di turni richiesto dalle risorse.'))
                else:
                    infeasible.append((emp, 'Impossibile assegnare il numero di turni richiesto dalle risorse.'))
            if assigned_flag:
                break

        if not assigned_flag:
            break

    def rebalance_weekends():
        """
        Riequilibra weekend spostando da chi ha Sab+Dom a chi ha 0 weekend.
        VERSIONE FLESSIBILE: Cerca QUALSIASI turno compatibile, non solo lo stesso.
        """
        changed = True
        iterations = 0
        max_iterations = 100
        total_swaps = 0

        while changed and iterations < max_iterations:
            changed = False
            iterations += 1

            double = [emp for emp in ris['id dipendente'] if len(weekend_work[emp]) >= 2]
            zero = [emp for emp in ris['id dipendente'] if len(weekend_work[emp]) == 0 and assignments_by_emp[emp]]

            if iterations == 1:
                print(f"   -> {len(double)} operatori con >=2 weekend, {len(zero)} con 0 weekend")

            if not double or not zero:
                if iterations == 1:
                    print(f"   [!]  Impossibile bilanciare: double={len(double)}, zero={len(zero)}")
                break

            # DEBUG: Contatori per capire perché gli swap falliscono
            fail_reasons = {
                'forced_off': 0,
                'no_weekend_shifts': 0,
                'no_weekday_shifts': 0,
                'no_valid_swap': 0
            }

            for emp in double:
                swapped = False
                weekend_days_emp = sorted([d for d in weekend_days if d in assignments_by_emp[emp]])

                for wday in weekend_days_emp:
                    sid_w = assignments_by_emp[emp][wday]

                    for cand in zero:
                        if len(weekend_work[cand]) != 0:
                            continue
                        if wday in forced_off.get(cand, set()):
                            fail_reasons['forced_off'] += 1
                            continue

                        # FLESSIBILITÀ: Cerca QUALSIASI turno disponibile per cand nel giorno weekend
                        # Non richiede più che sia esattamente sid_w
                        sid_for_cand = pick_shift_best_overlap(
                            cand,
                            wday,
                            prefer_sid=sid_w,
                            require_demand=demand_overlap_guard,
                        )
                        if sid_for_cand is None:
                            fail_reasons['no_weekend_shifts'] += 1
                            continue

                        # Cerca un giorno infrasettimanale del candidato da scambiare
                        found_valid_weekday = False
                        for day_cand, sid_cand in list(assignments_by_emp[cand].items()):
                            if day_cand in weekend_days:
                                continue
                            if day_cand in forced_off.get(emp, set()):
                                continue

                            # FLESSIBILITÀ: Cerca QUALSIASI turno disponibile per emp nel giorno infrasettimanale
                            sid_for_emp = pick_shift_best_overlap(
                                emp,
                                day_cand,
                                prefer_sid=sid_cand,
                                require_demand=demand_overlap_guard,
                            )
                            if sid_for_emp is None:
                                fail_reasons['no_weekday_shifts'] += 1
                                continue

                            found_valid_weekday = True

                            # Esegui lo swap
                            remove_assignment(emp, wday)
                            remove_assignment(cand, day_cand)
                            if not can_use_shift(cand, sid_for_cand, wday) or not can_use_shift(emp, sid_for_emp, day_cand):
                                apply_assignment(emp, wday, sid_w)
                                apply_assignment(cand, day_cand, sid_cand)
                                fail_reasons['no_weekday_shifts'] += 1
                                continue
                            apply_assignment(cand, wday, sid_for_cand)
                            apply_assignment(emp, day_cand, sid_for_emp)
                            total_swaps += 1
                            if total_swaps <= 5:  # Mostra solo i primi 5
                                print(f"   [OK] Swap #{total_swaps}: {emp} {wday}<->{cand} {day_cand}")
                            swapped = True
                            changed = True
                            break

                        # Se non trovato valid weekday, conta come fallimento
                        if not found_valid_weekday:
                            fail_reasons['no_weekday_shifts'] += 1

                        if swapped:
                            break
                    if swapped:
                        break
                if changed:
                    break

            # Stampa statistiche failure se non ci sono stati swap
            if iterations == 1 and total_swaps == 0 and (fail_reasons['forced_off'] > 0 or fail_reasons['no_weekend_shifts'] > 0):
                print(f"   [!]  Nessuno swap possibile. Motivi:")
                if fail_reasons['forced_off'] > 0:
                    print(f"      - {fail_reasons['forced_off']} tentativi bloccati da forced_off")
                if fail_reasons['no_weekend_shifts'] > 0:
                    print(f"      - {fail_reasons['no_weekend_shifts']} candidati senza turni weekend disponibili")
                if fail_reasons['no_weekday_shifts'] > 0:
                    print(f"      - {fail_reasons['no_weekday_shifts']} candidati senza turni infrasettimanali validi")

    rebalance_weekends()

    def ensure_required_days():
        attempts = 0
        max_attempts = max(1, len(ris) * 4)
        while True:
            missing = [emp for emp in ris['id dipendente'] if days_done[emp] < work_need[emp]]
            if not missing:
                break
            missing.sort(key=lambda e: (days_done[e], len(get_shifts(e))))
            progress = False
            for emp in missing:
                if force_balance:
                    day_sel, sid_sel = pick_force_assignment(emp)
                else:
                    best = None
                    day_sel = None
                    sid_sel = None
                    for day in giorni_validi:
                        if (emp, day) in assigned_once or day in forced_off.get(emp, set()):
                            continue
                        if weekend_overcovered(day):
                            continue
                        for sid in get_shifts(emp, day):
                            if not can_use_shift(emp, sid, day):
                                continue
                            val = shift_value(emp, day, sid, allow_overcapacity=True)
                            if val <= -1e4:
                                continue
                            key = (val, -shift_start_min[sid])
                            if best is None or key > best:
                                best = key
                                day_sel = day
                                sid_sel = sid
                if sid_sel is None:
                    continue
                apply_assignment(emp, day_sel, sid_sel, forced=force_balance)
                progress = True
            if not progress:
                default_msg = 'Impossibile assegnare il numero di turni richiesto dalle risorse.'
                for emp in missing:
                    entry = (emp, default_msg)
                    if entry not in infeasible:
                        infeasible.append(entry)
                break
            attempts += 1
            if attempts > max_attempts:
                break

    ensure_required_days()

    def enforce_critical_coverage():
        """
        Garantisce che ogni slot con domanda > 0 abbia almeno 1 persona assegnata.
        Questa funzione viene eseguita dopo il loop principale per fixare gap critici.
        """
        print("\n[TARGET] COVERAGE ENFORCEMENT - Verifica fasce scoperte...")

        uncovered_slots = []
        for day in giorni_validi:
            for slot in slot_list:
                demand = demand_by_slot[day].get(slot, 0.0)
                if demand > 0 and current_coverage[day][slot] == 0:
                    uncovered_slots.append((day, slot, demand))

        if not uncovered_slots:
            print("   [OK] Tutte le fasce con requisiti sono coperte")
            return

        print(f"   [!]  Trovate {len(uncovered_slots)} fasce con domanda > 0 ma copertura = 0")

        fixes_applied = 0
        for day, slot, demand in uncovered_slots:
            slot_time = _from_minutes(slot)
            print(f"   -> Tentativo fix: {day} {slot_time} (domanda {demand:.1f})")

            # Cerca dipendenti che possono coprire questo slot
            # VINCOLO HARD: SOLO dipendenti con giorni ancora disponibili (need > 0)
            candidates = []
            for emp in ris['id dipendente']:
                if (emp, day) in assigned_once:
                    continue  # Già assegnato in questo giorno
                if day in forced_off.get(emp, set()):
                    continue  # Forzato OFF in questo giorno

                need = work_need[emp] - days_done[emp]
                if need <= 0:
                    continue  # CRITICO: NON violare riposi obbligatori!

                for sid in get_shifts(emp, day):
                    if not can_use_shift(emp, sid, day):
                        continue
                    if slot in shift_slots.get(sid, []):
                        # Calcola score anche se può causare overcap
                        val = shift_value(emp, day, sid, allow_overcapacity=True, force_any=True)
                        candidates.append((need, val, -shift_start_min[sid], emp, sid))

            if not candidates:
                print(f"      [X] Nessun dipendente disponibile con giorni rimanenti")
                infeasible.append((f"Slot {day} {slot_time}",
                    f"Domanda {demand:.1f} ma nessun dipendente disponibile SENZA violare riposi obbligatori"))
                continue

            # Priorità a chi ha ancora giorni da lavorare, poi miglior score
            candidates.sort(reverse=True)
            need, _, _, emp, sid = candidates[0]

            apply_assignment(emp, day, sid, forced=True)
            fixes_applied += 1
            print(f"      [OK] Assegnato {emp} con turno {sid} (giorni rimanenti: {need})")

        if fixes_applied > 0:
            print(f"   [OK] Applicate {fixes_applied} assegnazioni forzate per garantire copertura critica\n")

    # NOTE: enforce_critical_coverage() viene chiamato DOPO fill_weekend_gap per dare priorità agli swap

    def allocate_overtime():
        """
        Alloca lo straordinario (OT) richiesto per ogni dipendente.
        In modalitÃ  --force-ot: alloca TUTTO lo straordinario possibile, anche in overcapacity.
        MAI allocare straordinario nei giorni di riposo.
        """
        nonlocal assignments, assignments_by_emp

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

        def day_has_positive_gap(day: str) -> bool:
            for slot in slot_list:
                if demand_by_slot[day].get(slot, 0.0) - current_coverage[day][slot] > 1e-6:
                    return True
            return False

        def slot_is_usable(day: str, slot: int) -> bool:
            if slot_has_demand(day, slot):
                return True
            if not allow_ot_overcap:
                return False
            return not day_has_positive_gap(day)

    
        # Calcola gli slot di straordinario richiesti per ogni dipendente/giorno
        required_slots = {}
        id_list = list(ris['id dipendente'])
        for emp in id_list:
            daily_map = ot_daily_minutes_by_emp.get(emp, {})
            for day, minutes in daily_map.items():
                minutes = int(minutes or 0)
                if minutes <= 0:
                    continue
                
                # CRITICAL FIX: Skip OT allocation if employee doesn't work this day
                if (emp, day) not in assignment_details:
                    if allow_ot_overcap:
                        print(f"AVVISO: {emp} ha {minutes} minuti di straordinario richiesti per {day} ma non ha turno base - IGNORATO")
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
                if new_slot not in slot_set:
                    return False
                # In force mode, ignore demand requirement
                if not slot_is_usable(day, new_slot):
                    return False
            return True
    
        def can_extend_end(emp: str, meta: dict, day: str, steps: int) -> bool:
            if steps <= 0:
                return False
            if not split_flags.get(emp, False) and meta.get('ot_start_slots', 0) > 0:
                return False
            for i in range(steps):
                new_slot = meta['actual_end_min'] + i * slot_size
                if new_slot not in slot_set:
                    return False
                # In force mode, ignore demand requirement
                if not slot_is_usable(day, new_slot):
                    return False
            return True
    
        def recompute_total_ot(meta: dict) -> int:
            total = int(meta.get('ot_minutes_start', 0) or 0) + int(meta.get('ot_minutes_end', 0) or 0)
            det_s = meta.get('ot_detached_start_min')
            det_e = meta.get('ot_detached_end_min')
            if det_s is not None and det_e is not None:
                try:
                    total += int(det_e) - int(det_s)
                except Exception:
                    total += 0
            meta['total_ot_minutes'] = int(total)
            return int(total)
    
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
                    # In force mode, ignore demand check
                    if not slot_is_usable(day, new_slot):
                        return False
                    meta['actual_start_min'] = new_slot
                    meta['ot_start_slots'] += 1
                    meta['ot_minutes_start'] = meta['ot_start_slots'] * slot_size
                    recompute_total_ot(meta)
                    meta['ot_direction'] = 'start' if meta.get('ot_direction') in (None, 'start') else 'both'
                    meta['extra_slots'].add(new_slot)
                    update_coverage(day, new_slot, 1)
                return True
            if direction == 'end':
                if not can_extend_end(emp, meta, day, steps):
                    return False
                for _ in range(steps):
                    new_slot = meta['actual_end_min']
                    # In force mode, ignore demand check
                    if not slot_is_usable(day, new_slot):
                        return False
                    meta['actual_end_min'] = meta['actual_end_min'] + slot_size
                    meta['ot_end_slots'] += 1
                    meta['ot_minutes_end'] = meta['ot_end_slots'] * slot_size
                    recompute_total_ot(meta)
                    meta['ot_direction'] = 'end' if meta.get('ot_direction') in (None, 'end') else 'both'
                    meta['extra_slots'].add(new_slot)
                    update_coverage(day, new_slot, 1)
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
                    if not slot_is_usable(day, check_slot):
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
                    if not slot_is_usable(day, check_slot):
                        return None
                if not can_extend_end(emp, meta, day, steps):
                    return None
                return 'end', steps
            return None
    
        def allocate_detached_block(emp: str, meta: dict, day: str, steps: int) -> bool:
            """Allocate detached OT block ONLY if employee already has a base shift that day"""
            if steps <= 0:
                return True
            if meta is None:
                return False
                
            meta.setdefault('extra_slots', set())
            base_start = int(meta.get('base_start', meta.get('actual_start_min', 0)) or 0)
            base_end = int(meta.get('base_end', meta.get('actual_end_min', base_start)) or base_start)
            forbidden = set(meta.get('extra_slots', set()))
            for s in range(base_start, base_end, slot_size):
                forbidden.add(s)
            
            best = None
            for slot in slot_list:
                cur = slot
                ok = True
                score = 0.0
                for _ in range(steps):
                    if cur not in slot_set or cur in forbidden:
                        ok = False
                        break
                    # In force mode, don't require demand
                    if not slot_is_usable(day, cur):
                        ok = False
                        break
                    score += demand_by_slot[day].get(cur, 0.0)
                    cur += slot_size
                if not ok:
                    continue
                candidate = (score, -slot, slot)
                if best is None or candidate > best:
                    best = candidate
            
            if best is None and allow_ot_overcap and not day_has_positive_gap(day):
                # In force mode, try ANY available slots, even senza domanda residua
                for slot in slot_list:
                    cur = slot
                    ok = True
                    for _ in range(steps):
                        if cur not in slot_set or cur in forbidden:
                            ok = False
                            break
                        cur += slot_size
                    if ok:
                        best = (0, -slot, slot)
                        break
            
            if best is None:
                return False
                
            _, _, best_start = best
            cur = best_start
            for _ in range(steps):
                meta['extra_slots'].add(cur)
                update_coverage(day, cur, 1)
                cur += slot_size
            meta['ot_detached_start_min'] = best_start
            meta['ot_detached_end_min'] = best_start + steps * slot_size
            recompute_total_ot(meta)
            return True
    
        def force_extend_forced(emp: str, day: str, slots_needed: int) -> bool:
            if slots_needed <= 0:
                return True
            meta = assignment_details.get((emp, day))
            if meta is None:
                return False  # Cannot force extend without base shift
                
            split = bool(split_flags.get(emp, False))
    
            def direction_capacity(direction: str):
                steps = 0
                demands = []
                while True:
                    next_step = steps + 1
                    if direction == 'start':
                        if not can_extend_start(emp, meta, day, next_step):
                            break
                        slot = meta['actual_start_min'] - next_step * slot_size
                    else:
                        if not can_extend_end(emp, meta, day, next_step):
                            break
                        slot = meta['actual_end_min'] + (next_step - 1) * slot_size
                    # In force mode, don't check demand
                    if not slot_is_usable(day, slot):
                        break
                    demands.append(demand_by_slot[day].get(slot, 0.0))
                    steps = next_step
                    if steps >= slots_needed and not split:
                        break
                return steps, demands
    
            if not split:
                cap_end, dem_end = direction_capacity('end')
                cap_start, dem_start = direction_capacity('start')
                choices = []
                if cap_end >= slots_needed:
                    # In force mode, prefer end extension
                    choices.append(('end', sum(dem_end[:slots_needed]) if dem_end else 0))
                if cap_start >= slots_needed:
                    choices.append(('start', sum(dem_start[:slots_needed]) if dem_start else 0))
                if not choices:
                    return False
                # Sort to prefer higher demand areas, but in force mode will extend anyway
                choices.sort(key=lambda x: (x[1], x[0] == 'end'), reverse=True)
                direction = choices[0][0]
                return perform_extend(emp, day, direction, slots_needed)
    
            # For split workers, extend in best direction step by step
            remaining = slots_needed
            safety = 0
            while remaining > 0 and safety < slots_needed * 4:
                safety += 1
                candidates = []
                if can_extend_start(emp, meta, day, 1):
                    slot = meta['actual_start_min'] - slot_size
                    # In force mode, add candidate even without demand
                    if slot_is_usable(day, slot):
                        candidates.append(('start', demand_by_slot[day].get(slot, 0.0)))
                if can_extend_end(emp, meta, day, 1):
                    slot = meta['actual_end_min']
                    # In force mode, add candidate even without demand
                    if slot_is_usable(day, slot):
                        candidates.append(('end', demand_by_slot[day].get(slot, 0.0)))
                if not candidates:
                    break
                candidates.sort(key=lambda x: x[1], reverse=True)
                direction = candidates[0][0]
                if not perform_extend(emp, day, direction, 1):
                    break
                remaining -= 1
            if remaining <= 0:
                return True
            # Try detached block for remaining OT
            return allocate_detached_block(emp, meta, day, remaining)
    
        # Processo principale di allocazione
        unmet_requirements = []
    
        for day in ['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom']:
            targets = {emp: required_slots[(emp, day)] for emp in ris['id dipendente'] if (emp, day) in required_slots}
            if not targets:
                continue
    
            day_assigns = [(emp, assignment_details[(emp, day)]) for emp in ris['id dipendente'] if (emp, day) in assignment_details]
            
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
                        if cur in base_slots:
                            ok = False
                            break
                        # In force mode, don't require demand
                        if not slot_is_usable(day, cur):
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
                        update_coverage(day, cur, 1)
                        cur += slot_size
                    meta['ot_detached_start_min'] = best_start
                    meta['ot_detached_end_min'] = best_start + steps * slot_size
                    meta['ot_minutes_start'] = 0
                    meta['ot_minutes_end'] = 0
                    meta['ot_start_slots'] = 0
                    meta['ot_end_slots'] = 0
                    recompute_total_ot(meta)
                    day_required[emp] = 0
    
            if allow_ot_overcap:
                # FORCE MODE: Allocate ALL required OT, ignoring coverage
                for emp, slots_needed in sorted(targets.items(), key=lambda x: x[1], reverse=True):
                    if slots_needed <= 0:
                        continue
                    if (emp, day) not in assignment_details:
                        continue
                    remaining = day_required.get(emp, 0)
                    if remaining <= 0:
                        continue
                        
                    success = force_extend_forced(emp, day, remaining)
                    if success:
                        day_required[emp] = 0
                    else:
                        # If can't extend, try detached for split workers
                        if split_flags.get(emp, False):
                            meta = assignment_details.get((emp, day))
                            if meta and allocate_detached_block(emp, meta, day, remaining):
                                day_required[emp] = 0
                            else:
                                unmet_requirements.append((emp, day, remaining * slot_size))
                        else:
                            unmet_requirements.append((emp, day, remaining * slot_size))
            else:
                # NORMAL MODE: Try to allocate OT respecting demand
                while sum(day_required.values()) > 0:
                    progress = False
                    gap_slots = []
                    for slot in slot_list:
                        gap = demand_by_slot[day].get(slot, 0.0) - current_coverage[day][slot]
                        if gap > 1e-6:
                            gap_slots.append((gap, slot))
                    gap_slots.sort(reverse=True)
    
                    for gap, slot in gap_slots:
                        if not slot_is_usable(day, slot):
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
                        
                    # Fallback allocation
                    best = None
                    for emp, meta in day_assigns:
                        remaining = day_required.get(emp, 0)
                        if remaining <= 0:
                            continue
                        if can_extend_start(emp, meta, day, 1):
                            new_slot = meta['actual_start_min'] - slot_size
                            if slot_is_usable(day, new_slot):
                                score = demand_by_slot[day].get(new_slot, 0.0)
                                candidate = (score, -new_slot, emp, 'start')
                                if best is None or candidate > best:
                                    best = candidate
                        if can_extend_end(emp, meta, day, 1):
                            new_slot = meta['actual_end_min']
                            if slot_is_usable(day, new_slot):
                                score = demand_by_slot[day].get(new_slot, 0.0)
                                candidate = (score, -new_slot, emp, 'end')
                                if best is None or candidate > best:
                                    best = candidate
                    if not best:
                        break
                    _, _, emp, direction = best
                    if perform_extend(emp, day, direction, 1):
                        day_required[emp] -= 1
                    else:
                        break
    
            for emp, remaining in day_required.items():
                if remaining > 0:
                    unmet_requirements.append((emp, day, remaining * slot_size))
    
        # Final reporting
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
            recompute_total_ot(meta)
            if isinstance(meta.get('extra_slots'), set):
                meta['extra_slots'] = sorted(meta['extra_slots'])
            ot_minutes_used[emp] += int(meta.get('total_ot_minutes', 0) or 0)
    
        # Report unallocated OT
        if unmet_requirements:
            print("\nATTENZIONE: Straordinario non allocato:")
            unmet_by_emp_day = {}
            for emp, day, minutes in unmet_requirements:
                key = (emp, day)
                if key not in unmet_by_emp_day:
                    unmet_by_emp_day[key] = 0
                unmet_by_emp_day[key] += minutes
            for (emp, day), total_minutes in sorted(unmet_by_emp_day.items()):
                if (emp, day) not in assignment_details:
                    print(f"  {emp} - {day}: {total_minutes} minuti (giorno di riposo)")
                else:
                    print(f"  {emp} - {day}: {total_minutes} minuti (impossibile allocare)")
    
        return ot_minutes_used
    def fill_saturday_gap(max_iter=500):
        """
        PRIORITÀ ASSOLUTA: Garantire presidio su sabato con requisiti > 0.
        Permette cali di copertura infrasettimanale pur di coprire weekend.
        """
        target_day = 'Sab'
        if allow_ot_overcap:
            return

        print(f"\n[LOOP] FILL SATURDAY GAP - Garantire presidio sabato...")

        for iteration in range(max_iter):
            gap_data = [
                (slot, demand_by_slot[target_day].get(slot, 0.0) - current_coverage[target_day][slot])
                for slot in slot_list
            ]
            positive_slots = [slot for slot, gap in gap_data if gap > 0.1]
            total_gap = sum(gap for _, gap in gap_data if gap > 0.1)

            if total_gap <= 0.1 or not positive_slots:
                print(f"   [OK] Sabato coperto (gap residuo: {total_gap:.2f})")
                break

            slot = positive_slots[0]
            candidate = None

            # Cerca dipendenti per swap, priorità a chi ha meno weekend
            # MODIFICA: Aumenta più velocemente il limite per garantire presidio
            # Prima 50 iter: max=1, poi 50: max=2, poi: max=3, infine: nessun limite
            if iteration < 50:
                max_weekend_allowed = 1
            elif iteration < 100:
                max_weekend_allowed = 2
            elif iteration < 200:
                max_weekend_allowed = 3
            else:
                max_weekend_allowed = 999  # Nessun limite: presidio > bilanciamento

            for emp in sorted(ris['id dipendente'], key=lambda e: (len(weekend_work[e]), days_done[e])):
                if (emp, target_day) in assigned_once:
                    continue
                if target_day in forced_off.get(emp, set()):
                    continue
                if len(weekend_work[emp]) >= max_weekend_allowed:
                    continue

                removable = None
                best_removal_score = None

                for day_existing, sid_existing in list(assignments_by_emp[emp].items()):
                    if day_existing == target_day or day_existing in weekend_days:
                        continue

                    # NUOVA LOGICA: Permetti rimozione se target è più critico del source
                    can_remove = True

                    # Calcola se target_day (Sabato/Domenica) ha copertura zero
                    target_has_zero_coverage = False
                    for s in shift_slots[sid_existing]:
                        target_coverage = current_coverage[target_day].get(s, 0)
                        target_demand = demand_by_slot[target_day].get(s, 0.0)
                        if target_demand > 0 and target_coverage == 0:
                            target_has_zero_coverage = True
                            break

                    for s in shift_slots[sid_existing]:
                        coverage_after = current_coverage[day_existing][s] - 1
                        demand = demand_by_slot[day_existing].get(s, 0.0)

                        # MODIFICA CRITICA: Permetti che source vada a 0 SE target è completamente scoperto
                        # Priorità: presidio weekend > mantenere copertura infrasettimanale
                        if demand > 0 and coverage_after <= 0:
                            if not target_has_zero_coverage:
                                # Target non è critico, mantieni vincolo
                                can_remove = False
                                break
                            # Altrimenti: target è a 0, permetti swap anche se source va a 0

                    if can_remove:
                        # Calcola rapporto copertura/requisiti MEDIO del giorno DOPO la rimozione
                        # Preferisci rimuovere da giorni più sovra-coperti (proporzionalmente)
                        ratios_after = []
                        for s in shift_slots[sid_existing]:
                            coverage_after = current_coverage[day_existing][s] - 1
                            demand = demand_by_slot[day_existing].get(s, 0.0)
                            if demand > 0:
                                ratio = coverage_after / demand
                                ratios_after.append(ratio)

                        # Score = rapporto medio (più alto = più sovra-coperto)
                        avg_ratio_after = sum(ratios_after) / len(ratios_after) if ratios_after else 0

                        if best_removal_score is None or avg_ratio_after > best_removal_score:
                            best_removal_score = avg_ratio_after
                            removable = (day_existing, sid_existing)

                if removable is None:
                    continue

                day_remove, sid_remove = removable
                sid_new = pick_shift_covering_slot(
                    emp,
                    target_day,
                    slot,
                    min_overlap=0.3,
                    require_demand=demand_overlap_guard,
                )
                if sid_new is not None:
                    candidate = (emp, day_remove, sid_remove, sid_new)

                if candidate:
                    break

            if not candidate:
                print(f"   [!]  Impossibile trovare swap per sabato (gap: {total_gap:.2f})")
                break

            emp, day_remove, sid_remove, sid_new = candidate
            print(f"   -> Swap: {emp} {day_remove}->Sab (gap infrasettimanale accettato per presidio)")
            remove_assignment(emp, day_remove)
            if not can_use_shift(emp, sid_new, target_day):
                apply_assignment(emp, day_remove, sid_remove)
                continue
            apply_assignment(emp, target_day, sid_new)

    def fill_sunday_gap(max_iter=500):
        """
        PRIORITÀ ASSOLUTA: Garantire presidio su domenica con requisiti > 0.
        Permette cali di copertura infrasettimanale pur di coprire weekend.
        """
        target_day = 'Dom'
        if allow_ot_overcap:
            return

        print(f"\n[LOOP] FILL SUNDAY GAP - Garantire presidio domenica...")

        for iteration in range(max_iter):
            gap_data = [
                (slot, demand_by_slot[target_day].get(slot, 0.0) - current_coverage[target_day][slot])
                for slot in slot_list
            ]
            positive_slots = [slot for slot, gap in gap_data if gap > 0.1]
            total_gap = sum(gap for _, gap in gap_data if gap > 0.1)

            if total_gap <= 0.1 or not positive_slots:
                print(f"   [OK] Domenica coperta (gap residuo: {total_gap:.2f})")
                break

            slot = positive_slots[0]
            candidate = None

            # Cerca dipendenti per swap, priorità a chi ha meno weekend
            # MODIFICA: Aumenta più velocemente il limite per garantire presidio
            # Prima 50 iter: max=1, poi 50: max=2, poi: max=3, infine: nessun limite
            if iteration < 50:
                max_weekend_allowed = 1
            elif iteration < 100:
                max_weekend_allowed = 2
            elif iteration < 200:
                max_weekend_allowed = 3
            else:
                max_weekend_allowed = 999  # Nessun limite: presidio > bilanciamento

            for emp in sorted(ris['id dipendente'], key=lambda e: (len(weekend_work[e]), days_done[e])):
                if (emp, target_day) in assigned_once:
                    continue
                if target_day in forced_off.get(emp, set()):
                    continue
                if len(weekend_work[emp]) >= max_weekend_allowed:
                    continue

                removable = None
                best_removal_score = None

                for day_existing, sid_existing in list(assignments_by_emp[emp].items()):
                    if day_existing == target_day or day_existing in weekend_days:
                        continue

                    # NUOVA LOGICA: Permetti rimozione se target è più critico del source
                    can_remove = True

                    # Calcola se target_day (Sabato/Domenica) ha copertura zero
                    target_has_zero_coverage = False
                    for s in shift_slots[sid_existing]:
                        target_coverage = current_coverage[target_day].get(s, 0)
                        target_demand = demand_by_slot[target_day].get(s, 0.0)
                        if target_demand > 0 and target_coverage == 0:
                            target_has_zero_coverage = True
                            break

                    for s in shift_slots[sid_existing]:
                        coverage_after = current_coverage[day_existing][s] - 1
                        demand = demand_by_slot[day_existing].get(s, 0.0)

                        # MODIFICA CRITICA: Permetti che source vada a 0 SE target è completamente scoperto
                        # Priorità: presidio weekend > mantenere copertura infrasettimanale
                        if demand > 0 and coverage_after <= 0:
                            if not target_has_zero_coverage:
                                # Target non è critico, mantieni vincolo
                                can_remove = False
                                break
                            # Altrimenti: target è a 0, permetti swap anche se source va a 0

                    if can_remove:
                        # Calcola rapporto copertura/requisiti MEDIO del giorno DOPO la rimozione
                        # Preferisci rimuovere da giorni più sovra-coperti (proporzionalmente)
                        ratios_after = []
                        for s in shift_slots[sid_existing]:
                            coverage_after = current_coverage[day_existing][s] - 1
                            demand = demand_by_slot[day_existing].get(s, 0.0)
                            if demand > 0:
                                ratio = coverage_after / demand
                                ratios_after.append(ratio)

                        # Score = rapporto medio (più alto = più sovra-coperto)
                        avg_ratio_after = sum(ratios_after) / len(ratios_after) if ratios_after else 0

                        if best_removal_score is None or avg_ratio_after > best_removal_score:
                            best_removal_score = avg_ratio_after
                            removable = (day_existing, sid_existing)

                if removable is None:
                    continue

                day_remove, sid_remove = removable
                sid_new = pick_shift_covering_slot(
                    emp,
                    target_day,
                    slot,
                    min_overlap=0.3,
                    require_demand=demand_overlap_guard,
                )
                if sid_new is not None:
                    candidate = (emp, day_remove, sid_remove, sid_new)

                if candidate:
                    break

            if not candidate:
                print(f"   [!]  Impossibile trovare swap per domenica (gap: {total_gap:.2f})")
                break

            emp, day_remove, sid_remove, sid_new = candidate
            print(f"   -> Swap: {emp} {day_remove}->Dom (gap infrasettimanale accettato per presidio)")
            remove_assignment(emp, day_remove)
            if not can_use_shift(emp, sid_new, target_day):
                apply_assignment(emp, day_remove, sid_remove)
                continue
            apply_assignment(emp, target_day, sid_new)

    def fill_all_critical_gaps(max_iter=500):
        """
        PRIORITÀ ASSOLUTA: Garantire presidio su TUTTI i giorni con requisiti > 0.
        Non solo weekend, ma QUALSIASI giorno (Lun-Dom).
        Fa swap aggressivi per coprire fasce scoperte.
        """
        print(f"\n[TARGET] FILL ALL CRITICAL GAPS - Garantire presidio su TUTTI i giorni...")
        failed_swaps = set()

        for iteration in range(max_iter):
            # Trova TUTTI i giorni con gap > 0
            days_with_gaps = []
            for day in giorni_validi:
                gap_data = [
                    (slot, demand_by_slot[day].get(slot, 0.0), current_coverage[day][slot])
                    for slot in slot_list
                ]
                positive_slots = [slot for slot, demand, current in gap_data if demand > 0 and current < demand]

                if not positive_slots:
                    continue

                # Calcola copertura percentuale media del giorno
                total_demand = sum(d for _, d, c in gap_data if d > 0 and c < d)
                total_current = sum(c for _, d, c in gap_data if d > 0 and c < d)
                total_gap = total_demand - total_current

                if total_gap > 0.1:
                    # PRIORITÀ PROPORZIONALE: copertura % del giorno
                    coverage_ratio = total_current / total_demand if total_demand > 0 else 1.0

                    # Bonus weekend per parità di copertura
                    day_bonus = 0.15 if day == 'Dom' else 0.10 if day == 'Sab' else 0.0

                    priority = (1.0 - coverage_ratio) + day_bonus
                    days_with_gaps.append((priority, total_gap, day, positive_slots[0]))

            if not days_with_gaps:
                print(f"   [ok] Tutti i giorni coperti")
                break

            # Ordina per priorità proporzionale, poi gap assoluto
            days_with_gaps.sort(reverse=True)
            _, total_gap_value, target_day, target_slot = days_with_gaps[0]

            if target_slot is None:
                break

            print(f"   -> Gap su {target_day}: {total_gap_value:.2f} persone mancanti")

            candidate = None

            # BILANCIAMENTO WEEKEND: Limite progressivo come fill_saturday/sunday_gap
            # Solo per target_day weekend, altrimenti nessun limite
            if target_day in weekend_days:
                if iteration < 100:
                    max_weekend_allowed = 1
                elif iteration < 250:
                    max_weekend_allowed = 2
                else:
                    max_weekend_allowed = 999  # Presidio > bilanciamento
            else:
                max_weekend_allowed = 999  # Nessun limite per giorni infrasettimanali

            for emp in sorted(ris['id dipendente'], key=lambda e: (len(weekend_work[e]), days_done[e])):
                if (emp, target_day) in assigned_once:
                    continue
                if target_day in forced_off.get(emp, set()):
                    continue
                if target_day in weekend_days and len(weekend_work[emp]) >= max_weekend_allowed:
                    continue  # Applica limite solo per target weekend

                removable = None
                best_removal_score = None

                for day_existing, sid_existing in list(assignments_by_emp[emp].items()):
                    if day_existing == target_day:
                        continue
                    if (emp, day_existing, target_day) in failed_swaps:
                        continue

                    # BILANCIAMENTO WEEKEND: Non spostare DA weekend A weekend
                    # Evita di creare sbilanciamenti (es. togliere Sab per mettere Dom)
                    if day_existing in weekend_days and target_day in weekend_days:
                        continue

                    # NUOVA LOGICA: Permetti rimozione se target è più critico del source
                    can_remove = True

                    # Calcola se target_day ha copertura zero su questi slot
                    target_has_zero_coverage = False
                    for s in shift_slots[sid_existing]:
                        target_coverage = current_coverage[target_day].get(s, 0)
                        target_demand = demand_by_slot[target_day].get(s, 0.0)
                        if target_demand > 0 and target_coverage == 0:
                            target_has_zero_coverage = True
                            break

                    for s in shift_slots[sid_existing]:
                        coverage_after = current_coverage[day_existing][s] - 1
                        demand = demand_by_slot[day_existing].get(s, 0.0)

                        # MODIFICA CRITICA: Permetti che source vada a 0 SE target è completamente scoperto
                        # Priorità: coprire fasce a zero > mantenere copertura esistente
                        if demand > 0 and coverage_after <= 0:
                            if not target_has_zero_coverage:
                                # Target non è critico, mantieni vincolo
                                can_remove = False
                                break
                            # Altrimenti: target è a 0, permetti swap anche se source va a 0

                    if can_remove:
                        # Calcola rapporto copertura/requisiti MEDIO del giorno DOPO la rimozione
                        # Preferisci rimuovere da giorni più sovra-coperti (proporzionalmente)
                        ratios_after = []
                        for s in shift_slots[sid_existing]:
                            coverage_after = current_coverage[day_existing][s] - 1
                            demand = demand_by_slot[day_existing].get(s, 0.0)
                            if demand > 0:
                                ratio = coverage_after / demand
                                ratios_after.append(ratio)

                        # Score = rapporto medio (più alto = più sovra-coperto)
                        avg_ratio_after = sum(ratios_after) / len(ratios_after) if ratios_after else 0

                        if best_removal_score is None or avg_ratio_after > best_removal_score:
                            best_removal_score = avg_ratio_after
                            removable = (day_existing, sid_existing)

                if removable is None:
                    continue

                day_remove, sid_remove = removable
                sid_new = pick_shift_covering_slot(
                    emp,
                    target_day,
                    target_slot,
                    min_overlap=0.3,
                    require_demand=demand_overlap_guard,
                )
                if sid_new is not None:
                    candidate = (emp, day_remove, sid_remove, sid_new)

                if candidate:
                    break

            if not candidate:
                print(f"   [!]  Impossibile swap per {target_day} (gap: {total_gap_value:.2f})")
                break

            emp, day_remove, sid_remove, sid_new = candidate
            print(f"   -> Swap: {emp} {day_remove}->{target_day}")
            remove_assignment(emp, day_remove)
            if not can_use_shift(emp, sid_new, target_day):
                apply_assignment(emp, day_remove, sid_remove)
                failed_swaps.add((emp, day_remove, target_day))
                continue
            apply_assignment(emp, target_day, sid_new)

    # PRIORITÀ ASSOLUTA: Garantire presidio su TUTTE le fasce
    fill_all_critical_gaps()  # Swap per tutti i giorni con gap

    # Riequilibra weekend dopo fill_all_critical_gaps
    print("\n[REBAL]  Riequilibrio weekend post-fill_all_critical_gaps...")
    rebalance_weekends()

    fill_saturday_gap()       # Extra focus su sabato
    fill_sunday_gap()         # Extra focus su domenica

    # Riequilibra weekend dopo fill_saturday/sunday_gap
    print("\n[REBAL]  Riequilibrio weekend post-fill_saturday/sunday_gap...")
    rebalance_weekends()

    ensure_required_days()

    def rebalance_proportional_coverage(max_iter=100):
        """
        RIEQUILIBRIO PROPORZIONALE POST-FILL:
        Dopo i fill_gap, bilancia la copertura infrasettimanale proporzionalmente ai requisiti.
        Sposta persone da giorni sovra-coperti (ratio > 1.3) a sotto-coperti (ratio < 0.8).
        NON tocca weekend (gia ottimizzato dai fill_gap), salvo modalita uniforme.
        """
        loop_label = "infrasettimanale"
        if uniform_overcap_active:
            loop_label = "tutti i giorni"
        print(f"\n[LOOP] RIEQUILIBRIO PROPORZIONALE - Bilanciamento finale {loop_label}...")
        failed_swaps = set()
        if uniform_overcap_active:
            rebalance_days = [g for g in giorni_validi if demand_by_day.get(g, 0.0) > 0]
            diff_target = max(0.1, (uniform_overcap_tol or 0.05) * 2.0)
            report_label = "giorni"
        else:
            rebalance_days = ['Lun', 'Mar', 'Mer', 'Gio', 'Ven']
            diff_target = 0.3
            report_label = "infrasettimanale"

        for iteration in range(max_iter):
            # Calcola ratio coverage/demand per ogni giorno infrasettimanale
            day_ratios = {}
            for day in rebalance_days:
                total_demand = sum(demand_by_slot[day].values())
                total_coverage = sum(current_coverage[day].values())
                if total_demand > 0:
                    day_ratios[day] = total_coverage / total_demand
                else:
                    day_ratios[day] = 1.0

            # Trova giorno più sovra-coperto
            most_over = max(day_ratios.keys(), key=lambda d: day_ratios[d])
            max_ratio = day_ratios[most_over]

            # Trova giorno più sotto-coperto
            most_under = min(day_ratios.keys(), key=lambda d: day_ratios[d])
            min_ratio = day_ratios[most_under]

            # Se differenza < soglia, stop (bilanciamento accettabile)
            diff = max_ratio - min_ratio
            if diff < diff_target:
                print(f"   [OK] Bilanciamento accettabile (max diff: {diff:.1%})")
                break

            print(f"   Iter {iteration+1}: {most_over} {max_ratio:.0%} -> {most_under} {min_ratio:.0%} (diff: {diff:.1%})")

            # Prova swap: persona da most_over → most_under
            swapped = False

            # Cerca dipendente assegnato a most_over
            candidates = []
            for emp in ris['id dipendente']:
                if most_over not in assignments_by_emp[emp]:
                    continue
                if (emp, most_under) in assigned_once:
                    continue  # Già assegnato a most_under
                if most_under in forced_off.get(emp, set()):
                    continue  # Vietato in most_under
                if (emp, most_over, most_under) in failed_swaps:
                    continue  # Già fallito prima

                # BILANCIAMENTO WEEKEND: Evita di spostare persone con entrambi i weekend assegnati
                # per preservare il bilanciamento weekend fatto da rebalance_weekends()
                weekend_days_assigned = [d for d in ['Sab', 'Dom'] if d in assignments_by_emp[emp]]
                if len(weekend_days_assigned) >= 2:
                    continue  # Ha entrambi i weekend, preserva questo operatore

                sid_over = assignments_by_emp[emp][most_over]

                # Verifica che rimuovendo da most_over non si vada a 0 su fasce critiche
                can_remove = True
                min_coverage_after = float('inf')

                for s in shift_slots[sid_over]:
                    coverage_after = current_coverage[most_over][s] - 1
                    demand = demand_by_slot[most_over].get(s, 0.0)

                    # VINCOLO: Non portare a 0 fasce con domanda > 0 (a meno che most_under sia a 0)
                    if demand > 0 and coverage_after <= 0:
                        # Verifica se most_under è critico (a zero)
                        under_is_critical = False
                        for s2 in shift_slots.get(sid_over, []):
                            if demand_by_slot[most_under].get(s2, 0.0) > 0 and current_coverage[most_under][s2] == 0:
                                under_is_critical = True
                                break

                        if not under_is_critical:
                            can_remove = False
                            break

                    min_coverage_after = min(min_coverage_after, coverage_after)

                if not can_remove:
                    continue

                # Verifica che emp abbia un turno disponibile per most_under
                matching_shifts = []
                for sid_new in get_shifts(emp, most_under):
                    # Il turno deve coprire almeno 1 slot con domanda > 0 in most_under
                    covers_needed = False
                    for s in shift_slots.get(sid_new, []):
                        if demand_by_slot[most_under].get(s, 0.0) > 0:
                            covers_needed = True
                            break
                    if covers_needed:
                        matching_shifts.append(sid_new)

                if not matching_shifts:
                    continue

                # Calcola beneficio dello swap
                benefit = (max_ratio - 1.0) + (1.0 - min_ratio)  # Quanto migliora il bilanciamento

                # PRIORITÀ: Preferisci spostare chi ha meno giorni di weekend
                # Questo preserva meglio il bilanciamento weekend
                weekend_penalty = len(weekend_days_assigned) * 0.5  # Penalità per ogni giorno weekend

                candidates.append((benefit - weekend_penalty, min_coverage_after, emp, sid_over, matching_shifts[0]))

            if not candidates:
                print(f"   [!]  Impossibile swap {most_over}->{most_under} (nessun candidato)")
                break

            # Prendi migliore candidato (massimo beneficio, mantiene più copertura)
            candidates.sort(reverse=True)
            _, _, emp, sid_over, sid_new = candidates[0]

            # Verifica weekend per log
            emp_weekends = [d for d in ['Sab', 'Dom'] if d in assignments_by_emp[emp]]
            weekend_info = f" (weekend: {','.join(emp_weekends) if emp_weekends else 'nessuno'})" if emp_weekends else ""

            # Esegui swap
            print(f"   -> Swap: {emp} {most_over}->{most_under}{weekend_info}")
            remove_assignment(emp, most_over)
            if not can_use_shift(emp, sid_new, most_under):
                apply_assignment(emp, most_over, sid_over)
                failed_swaps.add((emp, most_over, most_under))
                continue
            apply_assignment(emp, most_under, sid_new)
            swapped = True

        # Report finale
        print(f"\n[STATS] BILANCIAMENTO FINALE ({report_label}):")
        for day in rebalance_days:
            total_demand = sum(demand_by_slot[day].values())
            total_coverage = sum(current_coverage[day].values())
            if total_demand > 0:
                ratio = total_coverage / total_demand
                if uniform_overcap_active:
                    status = "[OK]" if abs(ratio - uniform_target_ratio) <= diff_target else "[!]"
                else:
                    status = "[OK]" if 0.8 <= ratio <= 1.3 else "[!]"
                print(f"   {status} {day}: {ratio:.0%} coperto (domanda: {total_demand:.0f}, copertura: {total_coverage:.0f})")

    rebalance_proportional_coverage()

    # PULIZIA FINALE: Riequilibra weekend dopo tutti i riempimenti e bilanciamenti
    # Le funzioni fill_gap potrebbero aver creato sbilanciamenti in scenari difficili
    print("\n[CLEAN]  PULIZIA FINALE - Riequilibrio weekend post-fill...")
    rebalance_weekends()

    # ENFORCE CRITICAL COVERAGE viene eseguito DOPO fill_gap per dare priorità agli swap
    enforce_critical_coverage()

    ot_minutes_used = allocate_overtime()

    minute_distribution.clear()
    for emp, day, sid in assignments:
        meta = assignment_details.get((emp, day))
        if meta:
            base_start = meta.get('actual_start_min', shift_start_min.get(sid, 0))
        else:
            base_start = shift_start_min.get(sid, 0)
        minute_distribution[int(base_start) % 60] += 1

    if force_balance and forced_records:
        print("\nDEBUG: Assegnazioni forzate (force_balance attivo):")
        for emp, day in sorted(forced_records):
            print(f"  {emp} {day}")

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

    # ==================== VALIDAZIONE FINALE VINCOLI HARD ====================
    print("\n" + "="*80)
    print("[LOCK] VALIDAZIONE VINCOLI HARD OBBLIGATORI")
    print("="*80)

    violations = []

    # 1. Verifica riposi obbligatori
    for emp in ris['id dipendente']:
        actual_days = days_done[emp]
        required_days = work_need[emp]
        if actual_days > required_days:
            violations.append(f"[ERR] {emp}: lavora {actual_days} giorni ma dovrebbe lavorare MAX {required_days} (riposi violati!)")
            infeasible.append((emp, f"VIOLAZIONE RIPOSI: lavora {actual_days} giorni invece di {required_days}"))
        elif actual_days < required_days:
            violations.append(f"[ERR] {emp}: lavora {actual_days} giorni ma dovrebbe lavorare MIN {required_days} (riposi eccessivi)")
            infeasible.append((emp, f"VIOLAZIONE RIPOSI: lavora {actual_days} giorni invece di {required_days}"))

    # 2. Verifica pattern ore/giorno (durate previste)
    actual_duration_counts: Dict[str, Counter] = {emp: Counter() for emp in ris['id dipendente']}
    for emp, day, sid in assignments:
        actual_duration = shift_duration_min.get(sid, shift_end_min[sid] - shift_start_min[sid])
        actual_duration_counts[emp][int(actual_duration)] += 1

    for emp in ris['id dipendente']:
        expected_counts = expected_duration_counts.get(emp, Counter())
        actual_counts = actual_duration_counts.get(emp, Counter())
        for duration, expected_count in expected_counts.items():
            actual_count = actual_counts.get(duration, 0)
            if actual_count != expected_count:
                violations.append(
                    f"[ERR] {emp}: durata {duration}min assegnata {actual_count}/{expected_count} volte"
                )
                infeasible.append(
                    (emp, f"Durate non rispettate: {duration}m attesi {expected_count}, assegnati {actual_count}")
                )
        extra = {dur: cnt for dur, cnt in actual_counts.items() if dur not in expected_counts and cnt > 0}
        for dur, cnt in extra.items():
            violations.append(f"[ERR] {emp}: durata non prevista {dur}m ({cnt} volte)")
            infeasible.append((emp, f"Durata {dur}m non prevista assegnata {cnt} volte"))

    # 3. Verifica straordinario non superi il disponibile
    for emp in ris['id dipendente']:
        total_ot_assigned = 0
        for (e, d), meta in assignment_details.items():
            if e == emp:
                total_ot_assigned += meta.get('total_ot_minutes', 0)

        available_ot = ot_minutes_by_emp.get(emp, 0)
        if total_ot_assigned > available_ot + 1:  # +1 per tolleranza arrotondamento
            violations.append(f"[ERR] {emp}: straordinario {total_ot_assigned}min supera disponibile {available_ot}min")

    if violations:
        print("\n[!]  VIOLAZIONI RILEVATE:")
        for v in violations:
            print(f"  {v}")
        print("\n[!]  IL FILE OUTPUT CONTIENE VIOLAZIONI DI VINCOLI OBBLIGATORI!")
        print("[INFO] Verifica i dati di input o disabilita coverage enforcement")
    else:
        print("\n[OK] Tutti i vincoli hard rispettati:")
        print("   * Riposi obbligatori: OK")
        print("   * Ore/giorno contrattuale: OK")
        print("   * Straordinario disponibile: OK")

    print("="*80 + "\n")
    # ========================================================================

    return assignments, riposi_info, infeasible, day_colmap, slot_list, slot_size, shift_slots, assignment_details, ot_minutes_used

# ----------------------------- Warning Summary -----------------------------

def build_warning_summary(
    riposi_info,
    infeasible,
    ris,
    assignments,
    turno_map,
    prefer_phases,
    assignment_details,
    forced_records=None,
    out_of_range=None
):
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

    if forced_records:
        forced_by_emp: Dict[str, Set[str]] = {}
        for emp, day in forced_records:
            forced_by_emp.setdefault(emp, set()).add(day)
        if forced_by_emp:
            dipendenti = ', '.join(sorted(name_map.get(emp, emp) for emp in forced_by_emp))
            dettagli = '; '.join(
                f"{name_map.get(emp, emp)}: {', '.join(sorted(days))}"
                for emp, days in sorted(forced_by_emp.items(), key=lambda item: name_map.get(item[0], item[0]))
            )
            rows.append({
                'Categoria': 'Assegnazioni forzate',
                'Messaggio': 'Turni aggiunti con modalità force_balance',
                'Dipendenti': dipendenti,
                'Dettagli': dettagli
            })

    if out_of_range:
        entries = []
        coinvolti = set()
        for emp, day, actual_start, actual_end, allowed_start, allowed_end in out_of_range:
            name = name_map.get(emp, emp)
            coinvolti.add(name)
            entries.append(
                f"{name} {day}: {_from_minutes(actual_start)}-{_from_minutes(actual_end)} "
                f"(fascia {_from_minutes(allowed_start)}-{_from_minutes(allowed_end)})"
            )
        rows.append({
            'Categoria': 'Turni fuori fascia',
            'Messaggio': 'Verificare fasce orarie rispetto alle disponibilità dichiarate',
            'Dipendenti': ', '.join(sorted(coinvolti)),
            'Dettagli': '; '.join(entries)
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
    day_name_map = {'Lun':'LunedÃ¬','Mar':'MartedÃ¬','Mer':'MercoledÃ¬','Gio':'GiovedÃ¬','Ven':'VenerdÃ¬','Sab':'Sabato','Dom':'Domenica'}

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
    all_days_long = ['LunedÃ¬','MartedÃ¬','MercoledÃ¬','GiovedÃ¬','VenerdÃ¬','Sabato','Domenica']

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
        days = ['LunedÃ¬','MartedÃ¬','MercoledÃ¬','GiovedÃ¬','VenerdÃ¬','Sabato','Domenica']
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
    long_map = {'Lun':'LunedÃ¬','Mar':'MartedÃ¬','Mer':'MercoledÃ¬','Gio':'GiovedÃ¬','Ven':'VenerdÃ¬','Sab':'Sabato','Dom':'Domenica'}
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

def process_single_skill_group(req, turni, ris, cfg, args, skill_label):
    req_pre = prepara_req(req)

    (
        durations_by_emp,
        rest_target_by_emp,
        ot_minutes_by_emp,
        ot_split_by_emp,
        ot_daily_minutes_by_emp,
        duration_patterns_by_emp,
        out_of_range_allowance_by_emp,
    ) = infer_personal_params_from_risorse(ris)

    allowed_durations_by_emp: Dict[str, Set[int]] = {}
    for emp in ris['id dipendente']:
        pattern = duration_patterns_by_emp.get(emp)
        if pattern:
            allowed_durations_by_emp[emp] = {int(d) for d in pattern}
        else:
            allowed_durations_by_emp[emp] = {int(durations_by_emp.get(emp, 240))}
    durations_set_min = {int(d) for durs in allowed_durations_by_emp.values() for d in durs} or {240}

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
    weekend_overcap_max = None
    if args.weekend_overcap_max is not None:
        if args.weekend_overcap_max > 0:
            weekend_overcap_max = args.weekend_overcap_max / 100.0
    uniform_overcap_tol = None
    if args.uniform_overcap_tol is not None:
        if args.uniform_overcap_tol > 0:
            uniform_overcap_tol = args.uniform_overcap_tol / 100.0
    
    # Logic for turni_cand
    if args.use_predefined:
        try:
            turni_cand = carica_turni_predefiniti(turni)
        except ValueError as exc:
            raise SystemExit(str(exc))
    else:
        try:
            turni_cand = genera_turni_candidati(
                req_pre,
                durations_set_min,
                grid_step_min=args.grid,
                force_phase_minutes=force_minutes
            )
        except ValueError as exc:
            raise SystemExit(str(exc))
    if turni_cand.empty:
        raise SystemExit(
            "Nessun turno candidato generato: verifica griglia e minuti preferiti (es. 0,30) rispetto alle fasce."
        )
    
    (
        shift_by_emp,
        shift_by_emp_day,
        shift_out_of_range,
        shift_out_of_range_day,
        avail_ini_min,
        avail_end_min,
        avail_ini_min_day,
        avail_end_min_day,
        forced_off_day,
    ) = determina_turni_ammissibili(
        ris,
        turni_cand,
        durations_by_emp,
        allowed_durations_by_emp,
        out_of_range_allowance_by_emp,
    )

    # Verifica ammissibilità
    emps_no_shifts = [emp for emp, shifts in shift_by_emp.items() if not shifts]
    if emps_no_shifts:
        print(f"   [!]  ATTENZIONE: {len(emps_no_shifts)} dipendenti senza turni ammissibili!")

    forced_off, forced_on = leggi_vincoli_weekend(ris)
    if forced_off_day:
        for emp, days in forced_off_day.items():
            forced_off.setdefault(emp, set()).update(days)
            if emp in forced_on:
                forced_on[emp].difference_update(days)
    prefer_tuple = prefer_phases if prefer_phases else tuple(sorted({0, 15, 30, 45}))

    try:
        percent_override = _parse_overcap_spec(args.overcap, 'percentuale overcap')
        penalty_override = _parse_overcap_spec(args.overcap_penalty, 'penalità overcap')
    except ValueError as exc:
        raise SystemExit(str(exc))

    overcap_percent_map, overcap_penalty_map = load_overcap_settings(cfg, percent_override, penalty_override)

    assignments, riposi_info, infeasible, day_colmap, slot_list, slot_size, shift_slots, assignment_details, ot_minutes_used = assegnazione_tight_capacity(
        req_pre.copy(), turni_cand, ris, shift_by_emp, shift_by_emp_day, rest_target_by_emp, durations_by_emp,
        forced_off, forced_on, ot_minutes_by_emp, ot_split_by_emp, ot_daily_minutes_by_emp,
        allow_ot_overcap=args.force_ot,
        prefer_phases=prefer_tuple, strict_phase=args.strict_phase,
        force_balance=args.force_balance,
        overcap_percent_map=overcap_percent_map,
        overcap_penalty_map=overcap_penalty_map,
        duration_patterns_by_emp=duration_patterns_by_emp,
        allowed_durations_by_emp=allowed_durations_by_emp,
        shift_out_of_range=shift_out_of_range,
        shift_out_of_range_day=shift_out_of_range_day,
        out_of_range_allowance_by_emp=out_of_range_allowance_by_emp,
        weekend_guard=args.weekend_guard,
        weekend_overcap_max=weekend_overcap_max,
        uniform_overcap=args.uniform_overcap,
        uniform_overcap_tol=uniform_overcap_tol,
    )

    pivot, df_ass, df_cov = crea_output(assignments, turni_cand, ris, req_pre, day_colmap, slot_list, slot_size, shift_slots, assignment_details)

    turno_map = dict(zip(turni_cand['id turno'], turni_cand['start_min']))
    forced_summary = [(emp, day) for (emp, day), meta in assignment_details.items() if meta.get('forced')]
    
    out_of_range = []
    for (emp, day), meta in assignment_details.items():
        actual_start = int(meta.get('actual_start_min', meta['base_start']))
        actual_end = int(meta.get('actual_end_min', meta['base_end']))
        allowed_start = avail_ini_min_day.get((emp, day), avail_ini_min.get(emp, 0))
        allowed_end = avail_end_min_day.get((emp, day), avail_end_min.get(emp, 24 * 60))
        if actual_start < allowed_start or actual_end > allowed_end:
            out_of_range.append((emp, day, actual_start, actual_end, allowed_start, allowed_end))

    df_warn = build_warning_summary(
        riposi_info,
        infeasible,
        ris,
        assignments,
        turno_map,
        prefer_tuple,
        assignment_details,
        forced_summary if args.force_balance else None,
        out_of_range if out_of_range else None
    )

    return pivot, df_ass, df_cov, df_warn

def main(argv=None):
    parser = argparse.ArgumentParser(description='Pianificazione v5.2 - riposi rigidi e bilanciamento minuti (Multi-skill).')
    parser.add_argument('--input', required=True, help='Percorso al file Excel di input (.xlsx/.xlsm)')
    parser.add_argument('--out', default='tabella_turni.xlsx', help='File Excel di output')
    parser.add_argument('--grid', type=int, default=15, help='Griglia minuti per start turno (default 15)')
    parser.add_argument('--prefer_phase', type=str, default='15,45', help="Minuti preferiti per l'inizio turno")
    parser.add_argument('--force_phase', action='store_true', help='Consente solo i minuti specificati')
    parser.add_argument('--strict-phase', action='store_true', help='Enfatizza minuti preferiti')
    parser.add_argument('--force-ot', action='store_true', help='Assegna OT anche in overcoverage')
    parser.add_argument('--force-balance', action='store_true', help='Garantisce giorni minimi con overcoverage')
    parser.add_argument('--weekend-guard', action='store_true', help='Limita overstaff weekend e turni senza domanda utile')
    parser.add_argument('--weekend-overcap-max', type=float, default=None, help='Max overcoverage weekend in percent (0=auto)')
    parser.add_argument('--uniform-overcap', action='store_true', help='Distribuisce overstaff in modo uniforme per giorno')
    parser.add_argument('--uniform-overcap-tol', type=float, default=None, help='Tolleranza percentuale per overstaff uniforme (0=auto)')
    parser.add_argument('--use-predefined', action='store_true', help='Usa i turni predefiniti dal foglio Turni')
    parser.add_argument('--overcap', type=str, default=None, help='Override percentuale overcapacity')
    parser.add_argument('--overcap-penalty', type=str, default=None, help='Override penalità overcapacity')
    parser.add_argument('--skill', type=str, default=None, help="Seleziona skill specifica (es. 'ps')")
    args = parser.parse_args(argv)

    # 1. Caricamento dati base
    xls, turni, ris_all, cfg = carica_dati_base(args.input)

    # 2. Identificazione skills
    fogli_req_list = trova_fogli_requisiti(xls, args.skill)
    print(f"INFO: Trovati {len(fogli_req_list)} fogli requisiti da elaborare: {[f[0] for f in fogli_req_list]}")

    all_pivots = []
    all_ass = []
    all_cov = []
    all_warn = []

    for sheet_name, skill_val in fogli_req_list:
        skill_display = skill_val if skill_val else "Generica"
        print(f"\n{'='*60}")
        print(f" ELABORAZIONE SKILL: {skill_display} (Foglio: {sheet_name})")
        print(f"{'='*60}")

        # Carica requisiti specifici
        req = pd.read_excel(xls, sheet_name)

        # Filtra risorse per skill
        try:
            ris_skill = _filtra_risorse_per_skill(ris_all, skill_val)
        except ValueError as e:
            print(f"[!]  SKIP SKILL '{skill_display}': {e}")
            continue
        
        print(f"INFO: Risorse disponibili per '{skill_display}': {len(ris_skill)}")

        # Esegue elaborazione
        pivot, df_ass, df_cov, df_warn = process_single_skill_group(req, turni, ris_skill, cfg, args, skill_display)

        # Aggiunge colonna Skill
        if not pivot.empty:
            pivot['Skill'] = skill_display
        if not df_ass.empty:
            df_ass['Skill'] = skill_display
        if not df_cov.empty:
            df_cov['Skill'] = skill_display
        if not df_warn.empty:
            df_warn['Skill'] = skill_display

        all_pivots.append(pivot)
        all_ass.append(df_ass)
        all_cov.append(df_cov)
        all_warn.append(df_warn)

    # 3. Aggregazione risultati
    print(f"\n{'='*60}")
    print(" SALVATAGGIO RISULTATI")
    print(f"{'='*60}")
    
    final_ass = pd.concat(all_ass, ignore_index=True) if all_ass else pd.DataFrame()
    final_cov = pd.concat(all_cov, ignore_index=True) if all_cov else pd.DataFrame()
    final_warn = pd.concat(all_warn, ignore_index=True) if all_warn else pd.DataFrame()
    
    # Per la pivot (Pianificazione), concateniamo e gestiamo l'indice
    # La pivot originale aveva indice [CF, Operatore]. 
    # Qui abbiamo aggiunto Skill. Meglio averla come colonna.
    final_plan = pd.concat([p.reset_index() for p in all_pivots], ignore_index=True) if all_pivots else pd.DataFrame()

    out_path = Path(args.out).expanduser()
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_path) as writer:
        if not final_plan.empty:
            # Ordina colonne: metti Skill all'inizio
            cols = list(final_plan.columns)
            if 'Skill' in cols:
                cols.insert(0, cols.pop(cols.index('Skill')))
                final_plan = final_plan[cols]
            final_plan.to_excel(writer, sheet_name='Pianificazione', index=False)
        
        if not final_ass.empty:
            cols = list(final_ass.columns)
            if 'Skill' in cols:
                cols.insert(0, cols.pop(cols.index('Skill')))
                final_ass = final_ass[cols]
            final_ass.to_excel(writer, sheet_name='Assegnazioni', index=False)
            
        if not final_cov.empty:
            cols = list(final_cov.columns)
            if 'Skill' in cols:
                cols.insert(0, cols.pop(cols.index('Skill')))
                final_cov = final_cov[cols]
            final_cov.to_excel(writer, sheet_name='Copertura', index=False)
            
        if not final_warn.empty:
            cols = list(final_warn.columns)
            if 'Skill' in cols:
                cols.insert(0, cols.pop(cols.index('Skill')))
                final_warn = final_warn[cols]
            final_warn.to_excel(writer, sheet_name='Warnings', index=False)

    print(f"Salvato file completato: {out_path}")
    return 0
if __name__ == '__main__':
    main()










