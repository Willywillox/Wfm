"""
ANALISI COMPLETA CURVE DI TRAFFICO - CALL CENTER
Analisi professionale per WFM con curve previsionali, consuntivi e forecast

✨ NUOVE FUNZIONALITÀ (versione migliorata):
====================================================

🔮 FORECAST AVANZATI CON MULTIPLE STAGIONALITÀ:
- ✅ TBATS: Gestisce automaticamente weekly + monthly + trend
- ✅ Prophet: Include festività italiane + regressori weekend
- ✅ Forecast Intraday Dinamico: Modelli separati per ogni fascia oraria
- ✅ SARIMA: Modelli ARIMA con stagionalità
- ✅ Confronto visivo tra tutti i modelli

📊 STAGIONALITÀ CATTURATE:
- Weekly (lun-dom): ✅ Tutti i modelli
- Monthly (pattern mensile): ✅ Prophet, TBATS
- Intraday (fasce orarie): ✅ Forecast Intraday Dinamico
- Festività italiane: ✅ Prophet
- Interazioni giorno×fascia: ✅ Forecast Intraday Dinamico

🎯 MIGLIORAMENTI RISPETTO ALLA VERSIONE PRECEDENTE:
1. Pattern intraday DINAMICI invece di fissi storici
2. Gestione automatica festività italiane (Natale, Pasqua, ecc.)
3. Modelli che catturano multiple stagionalità simultaneamente
4. Confronto grafico tra 7 diversi modelli di forecast
5. Regressori esterni (weekend, festività)

📦 DIPENDENZE:
- Obbligatorie: pandas, numpy, matplotlib, seaborn
- Consigliate: statsmodels, tbats, prophet, holidays

Per installare tutte le dipendenze:
    pip install pandas numpy matplotlib seaborn statsmodels tbats prophet holidays

AUTORE: Analisi WFM Call Center
VERSIONE: 2.0 Enhanced (con multiple stagionalità)
"""

SCRIPT_VERSION = "2.3.0"
LAST_UPDATE = "2025-11-23"

import logging
import math
import matplotlib
matplotlib.use("Agg")
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import queue
import threading
import io
from datetime import datetime, timedelta
from contextlib import contextmanager, redirect_stdout, redirect_stderr
import os
import glob
import subprocess
from pathlib import Path
import warnings
import tempfile
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
import time
warnings.filterwarnings('ignore')
from concurrent.futures import ProcessPoolExecutor, as_completed

# Fix encoding per Windows subprocess (supporto emoji)
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr.reconfigure(encoding='utf-8')

# Parametri globali per IC basate su quantili dei residui
DEFAULT_ALPHA = 0.10  # 80% central interval by default

HOLIDAY_FLAGS = [
    'Capodanno', 'Epifania', 'Festa_Liberazione', 'Festa_Lavoro',
    'Festa_Repubblica', 'Ferragosto', 'Ognissanti', 'Immacolata',
    'Natale', 'Santo_Stefano', 'Capodanno_Vigilia', 'Pasqua',
    'Venerdi_Santo', 'PostPasqua', 'Periodo_Natalizio', 'Post_Capodanno'
]

VERBOSE = os.environ.get("FORECAST_VERBOSE", "0").lower() not in ("0", "false", "no", "")
FAST_MODE = os.environ.get("FORECAST_FAST", "0").lower() not in ("0", "false", "no", "") or "--fast" in sys.argv


def log_debug(message: str):
    if VERBOSE:
        print(message)


def _log_step_time(label: str, started_at: float):
    elapsed = time.time() - started_at
    print(f"   ⏱️ {label} completato in {elapsed:.1f}s")

@contextmanager
def safe_excel_writer(path, **kwargs):
    path = Path(path)

    # Prova con engine specificato, se fallisce prova openpyxl
    engine = kwargs.get('engine', 'xlsxwriter')
    writer = None

    try:
        writer = pd.ExcelWriter(path, **kwargs)
    except (PermissionError, ImportError, ModuleNotFoundError) as e:
        if isinstance(e, PermissionError):
            # File in uso, prova con nome alternativo
            fallback = path.with_name(f"{path.stem}_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
            print(f"  File {path.name} in uso, salvo come {fallback.name}")
            path = fallback
            try:
                writer = pd.ExcelWriter(path, **kwargs)
            except (ImportError, ModuleNotFoundError):
                # Engine non disponibile, prova openpyxl
                if engine != 'openpyxl':
                    kwargs_fallback = kwargs.copy()
                    kwargs_fallback['engine'] = 'openpyxl'
                    writer = pd.ExcelWriter(path, **kwargs_fallback)
                else:
                    raise
        else:
            # Engine non disponibile (ImportError/ModuleNotFoundError)
            if engine != 'openpyxl':
                kwargs_fallback = kwargs.copy()
                kwargs_fallback['engine'] = 'openpyxl'
                writer = pd.ExcelWriter(path, **kwargs_fallback)
            else:
                raise

    try:
        yield writer, path
    finally:
        if writer is not None:
            writer.close()


def _interval_from_residuals(residuals, forecast_values, alpha=DEFAULT_ALPHA):
    """Calcola intervalli di confidenza basati sui quantili dei residui.

    Usa distribuzione empirica dei residui per stimare bande asimmetriche.
    Se i residui non sono disponibili o sono costanti, fallback a +/-1.96*std.
    """
    forecast_values = np.asarray(forecast_values)
    if residuals is None:
        residuals = np.array([])
    residuals = np.asarray(residuals)
    residuals = residuals[~pd.isna(residuals)]

    if residuals.size >= 10:
        lower_q = np.quantile(residuals, alpha / 2)
        upper_q = np.quantile(residuals, 1 - alpha / 2)
        lower = forecast_values + lower_q
        upper = forecast_values + upper_q
    else:
        resid_std = float(np.nanstd(residuals, ddof=1)) if residuals.size > 0 else 0.0
        if not np.isfinite(resid_std) or resid_std == 0.0:
            resid_std = float(np.nanstd(forecast_values, ddof=1) * 0.1)
        delta = 1.96 * resid_std
        lower = forecast_values - delta
        upper = forecast_values + delta

    return np.clip(lower, a_min=0, a_max=None), np.clip(upper, a_min=0, a_max=None)


def _fmt(val):
    if val is None:
        return "-"
    try:
        float_val = float(val)
    except Exception:
        return str(val)
    if not np.isfinite(float_val):
        return "-"
    try:
        return f"{float_val:.2f}"
    except Exception:
        return str(val)


# Verifica librerie opzionali
try:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from scipy import stats, signal
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False
    print("ATTENZIONE: statsmodels non disponibile")
    print("Per forecast avanzato installa: pip install statsmodels")

try:
    import sys
    log_debug(f"DEBUG: Python executable: {sys.executable}")
    log_debug("DEBUG: Tentativo import TBATS...")
    from tbats import TBATS
    TBATS_AVAILABLE = True
    log_debug("DEBUG: ✅ TBATS importato con successo!")
    log_debug(f"DEBUG: TBATS location: {TBATS.__module__}")
except (ImportError, ValueError) as e:
    TBATS_AVAILABLE = False
    log_debug("DEBUG: ❌ TBATS import fallito")
    log_debug(f"DEBUG: Tipo errore: {type(e).__name__}")
    log_debug(f"DEBUG: Messaggio: {str(e)[:200]}")
    if 'numpy.dtype size changed' in str(e):
        log_debug("⚠️  TBATS: Errore compatibilità numpy/pmdarima")
        log_debug("Soluzione: pip uninstall -y tbats pmdarima && pip install --no-cache-dir tbats")
    else:
        log_debug("NOTA: TBATS non disponibile (opzionale)")
        log_debug("Per multiple stagionalità avanzate: pip install tbats")
except Exception as e:
    TBATS_AVAILABLE = False
    log_debug(f"DEBUG: ❌ TBATS errore inaspettato: {type(e).__name__}: {e}")
    import traceback
    log_debug(traceback.format_exc())

# Configurazione grafica
plt.rcParams['figure.figsize'] = (15, 8)
plt.rcParams['font.size'] = 10
sns.set_style("whitegrid")
sns.set_palette("husl")

# =============================================================================
# TROVA FILE EXCEL
# =============================================================================

def trova_file_excel(custom_dirs=None):
    """Trova automaticamente tutti i file Excel disponibili.

    Se custom_dirs è valorizzato, cerca nei percorsi indicati;
    altrimenti usa la cartella dello script e la sottocartella ``file input``.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    search_roots = []
    if custom_dirs:
        for path in custom_dirs:
            if path and os.path.isdir(path):
                search_roots.append(path)

    if not search_roots:
        search_roots.append(script_dir)
        input_dir = os.path.join(script_dir, "file input")
        if os.path.isdir(input_dir):
            search_roots.append(input_dir)

    patterns = ["*.xlsx", "*.xlsm", "*.xls"]
    file_excel = []
    for root in search_roots:
        for pattern in patterns:
            file_excel.extend(glob.glob(os.path.join(root, pattern)))

    # Escludi file temporanei e file già nella cartella output
    file_excel = [f for f in file_excel if not os.path.basename(f).startswith('~$')]
    file_excel = [f for f in file_excel if 'output' not in f]

    # Rimuovi duplicati e ordina per nome per una stampa stabile
    file_excel = sorted(set(file_excel), key=lambda p: os.path.basename(p).lower())

    if len(file_excel) == 0:
        raise FileNotFoundError(
            "Nessun file Excel trovato: aggiungi i file nella stessa cartella dello script o in 'file input'"
        )

    print(f"\n{'='*80}")
    print(f"TROVATI {len(file_excel)} FILE EXCEL DA PROCESSARE")
    print(f"{'='*80}")
    for i, f in enumerate(file_excel, 1):
        root = os.path.basename(os.path.dirname(f))
        print(f"  {i}. {os.path.basename(f)} (cartella: {root or '.'})")
    print(f"{'='*80}\n")

    return file_excel

# =============================================================================
# CARICAMENTO DATI
# =============================================================================

def carica_dati(file_path):
    """Carica e prepara i dati dal file Excel"""
    print("Caricamento dati...")
    
    df = pd.read_excel(file_path)
    df['DATA'] = pd.to_datetime(df['DATA'])
    df['ANNO'] = df['DATA'].dt.year
    df['MESE'] = df['DATA'].dt.month
    df['MESE_NOME'] = df['DATA'].dt.strftime('%B')
    df['GIORNO_MESE'] = df['DATA'].dt.day
    fascia_inizio = df['FASCIA'].astype(str).str.split(' - ').str[0].str.strip()
    fascia_inizio = fascia_inizio.replace({'nan': np.nan, '': np.nan})
    fascia_normalizzata = fascia_inizio.str.replace('.', ':', regex=False)
    ora_dt = pd.to_datetime(fascia_normalizzata, format='%H:%M', errors='coerce')
    df['ORA_INIZIO'] = np.where(ora_dt.notna(), ora_dt.dt.strftime('%H:%M'), fascia_inizio)
    df['MINUTI'] = ora_dt.dt.hour * 60 + ora_dt.dt.minute
    if df['MINUTI'].isna().any():
        invalid_count = int(df['MINUTI'].isna().sum())
        print(f"Attenzione: {invalid_count} fasce orarie con formato non riconosciuto (minuti impostati a -1)")
        df['MINUTI'] = df['MINUTI'].fillna(-1)
    df['MINUTI'] = df['MINUTI'].astype(int)
    df['IS_WEEKEND'] = df['GG SETT'].isin(['sab', 'dom'])
    if 'week' not in df.columns:
        df['week'] = df['DATA'].dt.isocalendar().week.astype(int)
    
    print(f"Caricati {len(df):,} record")
    print(f"Periodo: {df['DATA'].min().date()} -> {df['DATA'].max().date()}")
    print(f"Totale chiamate: {df['OFFERTO'].sum():,}")
    
    return df

# =============================================================================
# ANALISI FASCIA ORARIA
# =============================================================================

def analisi_fascia_oraria(df, output_dir):
    print("\nANALISI PER FASCIA ORARIA")
    print("=" * 60)
    
    fascia_stats = df.groupby(['FASCIA', 'MINUTI']).agg({
        'OFFERTO': ['mean', 'median', 'std', 'min', 'max', 'sum', 'count']
    }).reset_index()
    fascia_stats.columns = ['FASCIA', 'MINUTI', 'MEDIA', 'MEDIANA', 'STD', 'MIN', 'MAX', 'TOTALE', 'CONTEGGIO']
    fascia_stats = fascia_stats.sort_values('MINUTI')
    
    fascia_wd = df[~df['IS_WEEKEND']].groupby(['FASCIA', 'MINUTI'])['OFFERTO'].mean().reset_index()
    fascia_we = df[df['IS_WEEKEND']].groupby(['FASCIA', 'MINUTI'])['OFFERTO'].mean().reset_index()
    
    fig, axes = plt.subplots(2, 1, figsize=(16, 10))
    
    axes[0].plot(fascia_stats['MINUTI'], fascia_stats['MEDIA'], 
                 linewidth=2.5, marker='o', markersize=4, label='Media', color='#2E86AB')
    axes[0].fill_between(fascia_stats['MINUTI'], 
                          fascia_stats['MEDIA'] - fascia_stats['STD'],
                          fascia_stats['MEDIA'] + fascia_stats['STD'],
                          alpha=0.2, color='#2E86AB')
    axes[0].set_title('Curva di Traffico Intraday', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Ora del giorno')
    axes[0].set_ylabel('Chiamate Offerte (media)')
    axes[0].grid(True, alpha=0.3)
    axes[0].legend()
    
    axes[1].plot(fascia_wd['MINUTI'], fascia_wd['OFFERTO'], 
                 linewidth=2.5, marker='o', label='Lun-Ven', color='#A23B72')
    axes[1].plot(fascia_we['MINUTI'], fascia_we['OFFERTO'], 
                 linewidth=2.5, marker='s', label='Sab-Dom', color='#F18F01')
    axes[1].set_title('Confronto Weekday vs Weekend', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Ora del giorno')
    axes[1].set_ylabel('Chiamate Offerte (media)')
    axes[1].grid(True, alpha=0.3)
    axes[1].legend()
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/curva_fascia_oraria.png', dpi=150 if FAST_MODE else 300, bbox_inches='tight')
    print(f"Grafico salvato: curva_fascia_oraria.png")
    
    top_fasce = fascia_stats.nlargest(5, 'MEDIA')[['FASCIA', 'MEDIA', 'TOTALE']]
    print("\nTop 5 Fasce di Picco:")
    for idx, row in top_fasce.iterrows():
        print(f"   {row['FASCIA']}: {row['MEDIA']:.1f} chiamate/slot")
    
    return fascia_stats

# =============================================================================
# ANALISI GIORNO SETTIMANA
# =============================================================================

def analisi_giorno_settimana(df, output_dir):
    print("\nANALISI PER GIORNO DELLA SETTIMANA")
    print("=" * 60)
    
    ordine_giorni = ['lun', 'mar', 'mer', 'gio', 'ven', 'sab', 'dom']
    
    giorno_stats = df.groupby('GG SETT').agg({
        'OFFERTO': ['mean', 'median', 'std', 'sum', 'count']
    }).reset_index()
    giorno_stats.columns = ['GIORNO', 'MEDIA', 'MEDIANA', 'STD', 'TOTALE', 'CONTEGGIO']
    giorno_stats['GIORNO'] = pd.Categorical(giorno_stats['GIORNO'], categories=ordine_giorni, ordered=True)
    giorno_stats = giorno_stats.sort_values('GIORNO')
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    colors = ['#A23B72' if g in ['sab', 'dom'] else '#2E86AB' for g in giorno_stats['GIORNO']]
    axes[0].bar(giorno_stats['GIORNO'], giorno_stats['MEDIA'], color=colors, alpha=0.8)
    axes[0].set_title('Chiamate Medie per Giorno Settimana', fontsize=13, fontweight='bold')
    axes[0].set_ylabel('Chiamate Offerte (media per slot)')
    axes[0].grid(axis='y', alpha=0.3)
    
    axes[1].bar(giorno_stats['GIORNO'], giorno_stats['TOTALE'], color=colors, alpha=0.8)
    axes[1].set_title('Volume Totale per Giorno', fontsize=13, fontweight='bold')
    axes[1].set_ylabel('Totale Chiamate')
    axes[1].grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/curva_giorno_settimana.png', dpi=150 if FAST_MODE else 300, bbox_inches='tight')
    print(f"Grafico salvato: curva_giorno_settimana.png")
    
    return giorno_stats

# =============================================================================
# ANALISI SETTIMANA
# =============================================================================

def analisi_settimana(df, output_dir):
    print("\nANALISI PER SETTIMANA")
    print("=" * 60)
    
    week_stats = df.groupby('week').agg({
        'OFFERTO': ['mean', 'sum', 'count'],
        'DATA': 'min'
    }).reset_index()
    week_stats.columns = ['SETTIMANA', 'MEDIA', 'TOTALE', 'SLOT', 'DATA_INIZIO']
    week_stats = week_stats.sort_values('SETTIMANA')
    
    fig, axes = plt.subplots(2, 1, figsize=(16, 10))
    
    axes[0].plot(week_stats['SETTIMANA'], week_stats['TOTALE'], 
                 marker='o', linewidth=2, markersize=6, color='#2E86AB')
    axes[0].fill_between(week_stats['SETTIMANA'], week_stats['TOTALE'], alpha=0.3, color='#2E86AB')
    axes[0].set_title('Trend Settimanale - Volume Totale', fontsize=13, fontweight='bold')
    axes[0].set_ylabel('Totale Chiamate')
    axes[0].grid(True, alpha=0.3)
    
    axes[1].plot(week_stats['SETTIMANA'], week_stats['MEDIA'], 
                 marker='s', linewidth=2, markersize=6, color='#A23B72')
    axes[1].axhline(week_stats['MEDIA'].mean(), color='red', linestyle='--', linewidth=2)
    axes[1].set_title('Trend Settimanale - Media per Slot', fontsize=13, fontweight='bold')
    axes[1].set_ylabel('Chiamate Medie per Slot')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/curva_settimana.png', dpi=150 if FAST_MODE else 300, bbox_inches='tight')
    print(f"Grafico salvato: curva_settimana.png")
    
    return week_stats

# =============================================================================
# ANALISI MESE
# =============================================================================

def analisi_mese(df, output_dir):
    print("\nANALISI PER MESE")
    print("=" * 60)
    
    mese_stats = df.groupby(['ANNO', 'MESE', 'MESE_NOME']).agg({
        'OFFERTO': ['mean', 'sum', 'count']
    }).reset_index()
    mese_stats.columns = ['ANNO', 'MESE', 'MESE_NOME', 'MEDIA', 'TOTALE', 'SLOT']
    mese_stats = mese_stats.sort_values(['ANNO', 'MESE'])
    mese_stats['ETICHETTA'] = mese_stats['MESE_NOME'] + ' ' + mese_stats['ANNO'].astype(str)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(mese_stats['ETICHETTA'], mese_stats['TOTALE'], color='#F18F01', alpha=0.8)
    ax.set_title('Volume Chiamate per Mese', fontsize=13, fontweight='bold')
    ax.set_ylabel('Totale Chiamate')
    ax.grid(axis='y', alpha=0.3)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/curva_mese.png', dpi=150 if FAST_MODE else 300, bbox_inches='tight')
    print(f"Grafico salvato: curva_mese.png")
    
    return mese_stats

# =============================================================================
# HEATMAP
# =============================================================================

def crea_heatmap(df, output_dir):
    print("\nCREAZIONE HEATMAP")
    print("=" * 60)
    
    ordine_giorni = ['lun', 'mar', 'mer', 'gio', 'ven', 'sab', 'dom']
    pivot = df.pivot_table(values='OFFERTO', index='GG SETT', columns='FASCIA', aggfunc='mean')
    pivot = pivot.reindex(ordine_giorni)
    fasce_ordinate = df.sort_values('MINUTI')['FASCIA'].unique()
    pivot = pivot[fasce_ordinate]
    
    plt.figure(figsize=(20, 8))
    sns.heatmap(pivot, annot=False, cmap='YlOrRd', cbar_kws={'label': 'Chiamate Medie'}, linewidths=0.5)
    plt.title('Heatmap: Giorno x Fascia Oraria', fontsize=14, fontweight='bold', pad=20)
    plt.xlabel('Fascia Oraria')
    plt.ylabel('Giorno Settimana')
    plt.xticks(rotation=90, fontsize=8)
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/heatmap_giorno_fascia.png', dpi=150 if FAST_MODE else 300, bbox_inches='tight')
    print(f"Heatmap salvata")

# =============================================================================
# CURVE PREVISIONALI
# =============================================================================

def genera_curve_previsionali(df, output_dir):
    print("\nGENERAZIONE CURVE PREVISIONALI")
    print("=" * 60)
    
    curve = {}
    curve['intraday_generale'] = df.groupby(['FASCIA', 'MINUTI'])['OFFERTO'].mean().reset_index()
    curve['intraday_generale'] = curve['intraday_generale'].sort_values('MINUTI')
    
    ordine_giorni = ['lun', 'mar', 'mer', 'gio', 'ven', 'sab', 'dom']
    for giorno in ordine_giorni:
        curve[f'intraday_{giorno}'] = (df[df['GG SETT'] == giorno]
                                       .groupby(['FASCIA', 'MINUTI'])['OFFERTO']
                                       .mean().reset_index().sort_values('MINUTI'))
    
    media_generale = df['OFFERTO'].mean()
    curve['coeff_giornalieri'] = (df.groupby('GG SETT')['OFFERTO'].mean() / media_generale).to_dict()
    
    output_path = Path(output_dir) / 'curve_previsionali.xlsx'
    with safe_excel_writer(output_path, engine='xlsxwriter') as (writer, actual_path):
        curve['intraday_generale'].to_excel(writer, sheet_name='Curva_Intraday', index=False)
        for giorno in ordine_giorni:
            curve[f'intraday_{giorno}'].to_excel(writer, sheet_name=f'Curva_{giorno.title()}', index=False)
        pd.DataFrame(list(curve['coeff_giornalieri'].items()),
                     columns=['Giorno', 'Coefficiente']).to_excel(writer, sheet_name='Coefficienti', index=False)

    print(f"Curve salvate in: {actual_path.name}")
    print(f"Curve salvate in: curve_previsionali.xlsx")
    return curve

# =============================================================================
# ANALISI TREND CONSUNTIVI
# =============================================================================

def analisi_consuntiva_trend(df, output_dir):
    print("\nANALISI TREND STORICO")
    print("=" * 60)
    
    daily = df.groupby('DATA').agg({'OFFERTO': ['sum', 'mean']}).reset_index()
    daily.columns = ['DATA', 'TOTALE', 'MEDIA']
    daily = daily.sort_values('DATA')
    daily['MA7'] = daily['TOTALE'].rolling(window=7, min_periods=1).mean()
    daily['MA28'] = daily['TOTALE'].rolling(window=28, min_periods=1).mean()
    
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(daily['DATA'], daily['TOTALE'], linewidth=1, alpha=0.4, color='lightgray', label='Totale Giornaliero')
    ax.plot(daily['DATA'], daily['MA7'], linewidth=2.5, label='Media Mobile 7gg', color='#2E86AB')
    ax.plot(daily['DATA'], daily['MA28'], linewidth=2, label='Media Mobile 28gg', color='#A23B72')
    ax.set_title('Trend Storico Volume', fontsize=14, fontweight='bold')
    ax.set_ylabel('Chiamate Totali')
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/analisi_trend_storico.png', dpi=150 if FAST_MODE else 300, bbox_inches='tight')
    print("Grafico salvato: analisi_trend_storico.png")
    
    return daily

# =============================================================================
# CONFRONTO PERIODI
# =============================================================================

def analisi_confronto_periodi(df, output_dir):
    print("\nCONFRONTO PERIODI")
    print("=" * 60)

    week_comp = df.groupby('week')['OFFERTO'].sum().reset_index()
    week_comp.columns = ['SETTIMANA', 'TOTALE']
    week_comp = week_comp.sort_values('SETTIMANA')
    week_comp['VAR_WEEK'] = week_comp['TOTALE'].pct_change() * 100

    month_comp = (df.groupby(['ANNO', 'MESE', 'MESE_NOME'])['OFFERTO']
                    .sum()
                    .reset_index())
    month_comp.columns = ['ANNO', 'MESE', 'MESE_NOME', 'TOTALE']
    month_comp = month_comp.sort_values(['ANNO', 'MESE'])
    month_comp['ETICHETTA'] = month_comp['MESE_NOME'] + ' ' + month_comp['ANNO'].astype(str)
    month_comp['VAR_MONTH'] = month_comp['TOTALE'].pct_change() * 100

    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=False)

    colors_week = ['green' if x > 0 else 'red' if x < 0 else 'gray'
                   for x in week_comp['VAR_WEEK'].fillna(0)]
    axes[0].bar(week_comp['SETTIMANA'], week_comp['VAR_WEEK'], color=colors_week, alpha=0.7)
    axes[0].axhline(0, color='black', linewidth=1)
    axes[0].set_title('Variazione % Settimana su Settimana', fontsize=13, fontweight='bold')
    axes[0].set_ylabel('Variazione %')
    axes[0].grid(axis='y', alpha=0.3)

    if not month_comp.empty:
        colors_month = ['green' if x > 0 else 'red' if x < 0 else 'gray'
                        for x in month_comp['VAR_MONTH'].fillna(0)]
        axes[1].bar(month_comp['ETICHETTA'], month_comp['VAR_MONTH'], color=colors_month, alpha=0.7)
        axes[1].axhline(0, color='black', linewidth=1)
        axes[1].set_title('Variazione % Mese su Mese', fontsize=13, fontweight='bold')
        axes[1].set_ylabel('Variazione %')
        axes[1].set_xticks(range(len(month_comp)))
        axes[1].set_xticklabels(month_comp['ETICHETTA'], rotation=45, ha='right')
        axes[1].grid(axis='y', alpha=0.3)
    else:
        axes[1].set_visible(False)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/confronto_periodi.png', dpi=150 if FAST_MODE else 300, bbox_inches='tight')
    print('Grafico salvato: confronto_periodi.png')

    if len(week_comp) >= 2 and not np.isnan(week_comp['VAR_WEEK'].iloc[-1]):
        ultima_settimana = int(week_comp['SETTIMANA'].iloc[-1])
        print(f"  Ultima settimana ({ultima_settimana}): {week_comp['VAR_WEEK'].iloc[-1]:+.1f}% vs settimana precedente")
    if len(month_comp) >= 2 and not np.isnan(month_comp['VAR_MONTH'].iloc[-1]):
        ultimo_mese = month_comp.iloc[-1]
        print(f"  Ultimo mese ({ultimo_mese['ETICHETTA']}): {ultimo_mese['VAR_MONTH']:+.1f}% vs mese precedente")

    return week_comp, month_comp


# =============================================================================
# =============================================================================
# IDENTIFICAZIONE ANOMALIE
# =============================================================================

def identifica_anomalie(df, output_dir):
    print("\nIDENTIFICAZIONE ANOMALIE")
    print("=" * 60)

    daily = df.groupby('DATA')['OFFERTO'].sum().reset_index()
    daily.columns = ['DATA', 'TOTALE']

    media = daily['TOTALE'].mean()
    std = daily['TOTALE'].std()
    soglia_alta = media + 2 * std
    soglia_bassa = media - 2 * std

    daily['ANOMALIA'] = 'Normale'

    if SKLEARN_AVAILABLE and not FAST_MODE:
        # Metodo Avanzato: Isolation Forest
        try:
            # Reshape per sklearn
            X = daily['TOTALE'].values.reshape(-1, 1)
            # Contamination stima la % di outlier (es. 5%)
            iso = IsolationForest(contamination=0.05, random_state=42)
            preds = iso.fit_predict(X)
            # -1 sono anomalie, 1 sono normali
            daily.loc[preds == -1, 'ANOMALIA'] = 'Anomalia'
            
            # Distinguiamo Alto/Basso in base alla media
            mask_anom = daily['ANOMALIA'] == 'Anomalia'
            daily.loc[mask_anom & (daily['TOTALE'] > media), 'ANOMALIA'] = 'Picco Alto'
            daily.loc[mask_anom & (daily['TOTALE'] < media), 'ANOMALIA'] = 'Picco Basso'
            
            print("   ✅ Isolation Forest applicato per rilevamento anomalie")
        except Exception as e:
            print(f"   ⚠️ Errore Isolation Forest: {e}, uso metodo statistico standard")
            # Fallback al metodo statistico
            daily.loc[daily['TOTALE'] > soglia_alta, 'ANOMALIA'] = 'Picco Alto'
            daily.loc[daily['TOTALE'] < soglia_bassa, 'ANOMALIA'] = 'Picco Basso'
    else:
        # Metodo statistico standard (Mean +/- 2*Std)
        daily.loc[daily['TOTALE'] > soglia_alta, 'ANOMALIA'] = 'Picco Alto'
        daily.loc[daily['TOTALE'] < soglia_bassa, 'ANOMALIA'] = 'Picco Basso'

    anomalie_alte = daily[daily['ANOMALIA'] == 'Picco Alto'].sort_values('TOTALE', ascending=False)
    anomalie_basse = daily[daily['ANOMALIA'] == 'Picco Basso'].sort_values('TOTALE')

    fig, ax = plt.subplots(figsize=(16, 6))
    colors = daily['ANOMALIA'].map({'Normale': '#2E86AB', 'Picco Alto': '#FF6B6B', 'Picco Basso': '#FFA500'})
    ax.scatter(daily['DATA'], daily['TOTALE'], c=colors, alpha=0.6, s=50)
    ax.axhline(media, color='green', linestyle='--', linewidth=2)
    ax.axhline(soglia_alta, color='red', linestyle='--', linewidth=1.5)
    ax.axhline(soglia_bassa, color='orange', linestyle='--', linewidth=1.5)
    ax.set_title('Identificazione Anomalie', fontsize=14, fontweight='bold')
    ax.set_ylabel('Chiamate Totali')
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/identificazione_anomalie.png", dpi=150 if FAST_MODE else 300, bbox_inches='tight')
    print("Grafico salvato: identificazione_anomalie.png")

    print(f"\nPicchi alti: {len(anomalie_alte)}")
    print(f"Picchi bassi: {len(anomalie_basse)}")

    top_n = 3
    if not anomalie_alte.empty:
        print(f"\nTop {min(top_n, len(anomalie_alte))} picchi alti:")
        for _, row in anomalie_alte.head(top_n).iterrows():
            print(f"  {row['DATA'].strftime('%Y-%m-%d')}: {row['TOTALE']:,.0f} chiamate")
    else:
        print("\nNessun picco alto rilevato")

    if not anomalie_basse.empty:
        print(f"\nTop {min(top_n, len(anomalie_basse))} picchi bassi:")
        for _, row in anomalie_basse.head(top_n).iterrows():
            print(f"  {row['DATA'].strftime('%Y-%m-%d')}: {row['TOTALE']:,.0f} chiamate")
    else:
        print("\nNessun picco basso rilevato")

    anomalie_path = os.path.join(output_dir, 'anomalie_riepilogo.txt')
    with open(anomalie_path, 'w', encoding='utf-8') as f:
        f.write('ANOMALIE RILEVATE\n')
        f.write('=' * 80 + '\n')
        f.write(f"Picchi alti: {len(anomalie_alte)}\n")
        f.write(f"Picchi bassi: {len(anomalie_basse)}\n\n")
        if not anomalie_alte.empty:
            f.write('Top picchi alti:\n')
            for _, row in anomalie_alte.head(top_n).iterrows():
                f.write(f"  {row['DATA'].strftime('%Y-%m-%d')}: {row['TOTALE']:,.0f} chiamate\n")
            f.write('\n')
        if not anomalie_basse.empty:
            f.write('Top picchi bassi:\n')
            for _, row in anomalie_basse.head(top_n).iterrows():
                f.write(f"  {row['DATA'].strftime('%Y-%m-%d')}: {row['TOTALE']:,.0f} chiamate\n")
    print('Riepilogo anomalie salvato: anomalie_riepilogo.txt')

    return anomalie_alte, anomalie_basse


def _rileva_alert(df, forecast_df, backtest_metrics, output_dir):
    """
    Rileva condizioni di alert da mostrare nella GUI.

    Args:
        df: DataFrame dati storici
        forecast_df: DataFrame forecast (con colonna FORECAST o modelli)
        backtest_metrics: dict metriche affidabilità
        output_dir: cartella output

    Returns:
        list di dict con:
            - tipo: 'warning' | 'error' | 'info'
            - icona: emoji
            - titolo: stringa breve
            - descrizione: dettagli
            - severita: 'alta' | 'media' | 'bassa'
    """
    print("\nRILEVAMENTO ALERT AUTOMATICI")
    print("=" * 80)

    alerts = []

    # 1. Alert: Picchi previsti elevati
    daily_historical = df.groupby('DATA')['OFFERTO'].sum()
    media_storica = daily_historical.mean()
    std_storica = daily_historical.std()
    soglia_picco = media_storica + 2 * std_storica

    # Trova colonna forecast (priorità: FORECAST, poi prima disponibile)
    if forecast_df is None or forecast_df.empty:
        print("⚠️  Nessun forecast disponibile per alert")
        return alerts

    forecast_col = 'FORECAST' if 'FORECAST' in forecast_df.columns else \
                   [c for c in forecast_df.columns if c != 'DATA'][0]

    giorni_picco = forecast_df[forecast_df[forecast_col] > soglia_picco]
    if len(giorni_picco) > 0:
        max_picco = giorni_picco[forecast_col].max()
        data_picco = giorni_picco.loc[giorni_picco[forecast_col].idxmax(), 'DATA']
        pct_over = ((max_picco - media_storica) / media_storica * 100)

        alerts.append({
            'tipo': 'warning',
            'icona': '⚠️',
            'titolo': 'Picco Previsto Elevato',
            'descrizione': f'{len(giorni_picco)} giorni con picco >20% sopra media storica. '
                          f'Max: {max_picco:,.0f} chiamate il {data_picco.strftime("%d/%m/%Y")} '
                          f'(+{pct_over:.1f}% vs media)',
            'severita': 'alta' if pct_over > 50 else 'media'
        })

    # 2. Alert: Bassa affidabilità modello
    if backtest_metrics:
        # Trova MAPE del miglior modello
        valid_models = {m: v.get('MAPE') for m, v in backtest_metrics.items()
                       if v.get('MAPE') is not None and np.isfinite(v.get('MAPE'))}
        if valid_models:
            best_model = min(valid_models, key=valid_models.get)
            best_mape = valid_models[best_model]

            if best_mape > 15:
                alerts.append({
                    'tipo': 'error',
                    'icona': '🔴',
                    'titolo': 'Bassa Affidabilità Forecast',
                    'descrizione': f'Miglior modello ({best_model}) ha MAPE={best_mape:.1f}% (>15%). '
                                  f'Forecast poco affidabile, usare con cautela e margine sicurezza.',
                    'severita': 'alta'
                })
            elif best_mape > 10:
                alerts.append({
                    'tipo': 'warning',
                    'icona': '⚠️',
                    'titolo': 'Affidabilità Moderata',
                    'descrizione': f'MAPE={best_mape:.1f}%. Accuratezza accettabile ma monitorare i picchi.',
                    'severita': 'media'
                })

    # 3. Alert: Dati storici incompleti
    date_range = pd.date_range(df['DATA'].min(), df['DATA'].max(), freq='D')
    giorni_attesi = len(date_range)
    giorni_effettivi = df['DATA'].nunique()
    missing_pct = ((giorni_attesi - giorni_effettivi) / giorni_attesi * 100)

    if missing_pct > 5:
        alerts.append({
            'tipo': 'warning',
            'icona': '📉',
            'titolo': 'Dati Storici Incompleti',
            'descrizione': f'Mancano {giorni_attesi - giorni_effettivi} giorni ({missing_pct:.1f}%) '
                          f'nel periodo storico. Potrebbe impattare accuracy forecast.',
            'severita': 'media' if missing_pct < 15 else 'alta'
        })

    # 4. Alert: Pattern anomali rilevati
    daily = df.groupby('DATA')['OFFERTO'].sum()
    if SKLEARN_AVAILABLE and not FAST_MODE and len(daily) > 30:
        try:
            from sklearn.ensemble import IsolationForest
            X = daily.values.reshape(-1, 1)
            iso = IsolationForest(contamination=0.05, random_state=42)
            preds = iso.fit_predict(X)
            n_anomalie = (preds == -1).sum()

            if n_anomalie > len(daily) * 0.1:  # >10% anomalie
                alerts.append({
                    'tipo': 'info',
                    'icona': '🔍',
                    'titolo': 'Pattern Anomali Rilevati',
                    'descrizione': f'{n_anomalie} giorni anomali rilevati ({n_anomalie/len(daily)*100:.1f}%). '
                                  f'Verifica eventi speciali o cambi operativi.',
                    'severita': 'bassa'
                })
        except Exception:
            pass

    # 5. Alert: Trend in cambiamento
    if len(daily) >= 60:
        # Trend su ultimi 30 vs precedenti 30
        recent_trend = np.polyfit(range(30), daily.values[-30:], 1)[0]
        older_trend = np.polyfit(range(30), daily.values[-60:-30], 1)[0]

        if abs(recent_trend - older_trend) > media_storica * 0.02:  # Cambio >2%
            direction = "crescita" if recent_trend > older_trend else "decrescita"
            alerts.append({
                'tipo': 'info',
                'icona': '📊',
                'titolo': f'Trend in {direction.capitalize()}',
                'descrizione': f'Ultimi 30 giorni mostrano {direction} rispetto al periodo precedente. '
                              f'Considera aggiornare forecast più frequentemente.',
                'severita': 'bassa'
            })

    # 6. Alert: Stagionalità weekend molto diversa
    if 'IS_WEEKEND' in df.columns:
        media_wd = df[~df['IS_WEEKEND']]['OFFERTO'].mean()
        media_we = df[df['IS_WEEKEND']]['OFFERTO'].mean()
        diff_pct = abs((media_we - media_wd) / media_wd * 100)

        if diff_pct > 30:
            alerts.append({
                'tipo': 'info',
                'icona': '📅',
                'titolo': 'Forte Stagionalità Weekend',
                'descrizione': f'Weekend ha volume {diff_pct:.0f}% diverso da weekday. '
                              f'Usa modelli con stagionalità settimanale (Prophet, TBATS).',
                'severita': 'bassa'
            })

    # Ordina per severità
    severita_order = {'alta': 0, 'media': 1, 'bassa': 2}
    alerts.sort(key=lambda x: severita_order[x['severita']])

    print(f"✅ Rilevati {len(alerts)} alert:")
    for alert in alerts:
        print(f"   {alert['icona']} [{alert['severita'].upper()}] {alert['titolo']}")

    # Salva report alert
    alert_path = Path(output_dir) / 'alert_automatici.txt'
    with open(alert_path, 'w', encoding='utf-8') as f:
        f.write("ALERT AUTOMATICI\n")
        f.write("=" * 80 + "\n\n")
        for alert in alerts:
            f.write(f"{alert['icona']} [{alert['severita'].upper()}] {alert['titolo']}\n")
            f.write(f"{alert['descrizione']}\n")
            f.write("-" * 80 + "\n\n")

    return alerts


# =============================================================================
# KPI CONSUNTIVI
# =============================================================================

def dashboard_kpi_consuntivi(df, output_dir):
    print("\nDASHBOARD KPI CONSUNTIVI")
    print("=" * 60)

    kpi = {}
    kpi['totale_chiamate'] = df['OFFERTO'].sum()
    kpi['media_giornaliera'] = df.groupby('DATA')['OFFERTO'].sum().mean()
    kpi['media_per_slot'] = df['OFFERTO'].mean()

    daily_totals = df.groupby('DATA')['OFFERTO'].sum().values
    trend_coeff = np.polyfit(range(len(daily_totals)), daily_totals, 1)[0]
    kpi['trend'] = "Crescita" if trend_coeff > 0 else "Decrescita"
    kpi['trend_valore'] = trend_coeff

    kpi['std'] = df['OFFERTO'].std()
    mean_slot = df['OFFERTO'].mean()
    kpi['cv'] = (df['OFFERTO'].std() / mean_slot) * 100 if mean_slot != 0 else np.nan
    kpi['max_slot'] = df['OFFERTO'].max()
    kpi['min_slot'] = df['OFFERTO'].min()

    daily = df.groupby(['DATA', 'GG SETT'])['OFFERTO'].sum().reset_index()
    kpi['giorno_max'] = daily.loc[daily['OFFERTO'].idxmax()]
    kpi['giorno_min'] = daily.loc[daily['OFFERTO'].idxmin()]

    fascia_media = df.groupby('FASCIA')['OFFERTO'].mean()
    kpi['fascia_picco'] = fascia_media.idxmax()
    kpi['fascia_picco_valore'] = fascia_media.max()

    giorno_media = df.groupby('GG SETT')['OFFERTO'].mean()
    kpi['giorno_sett_picco'] = giorno_media.idxmax()
    kpi['giorno_sett_picco_valore'] = giorno_media.max()

    kpi['media_weekday'] = df[~df['IS_WEEKEND']]['OFFERTO'].mean()
    kpi['media_weekend'] = df[df['IS_WEEKEND']]['OFFERTO'].mean()
    if kpi['media_weekday'] != 0:
        kpi['diff_weekend'] = ((kpi['media_weekend'] / kpi['media_weekday']) - 1) * 100
    else:
        kpi['diff_weekend'] = np.nan

    with open(f"{output_dir}/kpi_consuntivi.txt", 'w', encoding='utf-8') as f:
        f.write("KPI CONSUNTIVI\n")
        f.write("=" * 80 + "\n")
        f.write(f"Totale chiamate: {kpi['totale_chiamate']:,.0f}\n")
        f.write(f"Media giornaliera: {kpi['media_giornaliera']:,.0f}\n")
        f.write(f"Trend: {kpi['trend']}\n")
        f.write(f"Fascia picco: {kpi['fascia_picco']}\n")

    print("KPI salvati in: kpi_consuntivi.txt")
    return kpi




# =============================================================================
# FORECAST AVANZATO - HOLT-WINTERS
# =============================================================================

def _forecast_holtwinters(df, output_dir, giorni_forecast=28, produce_outputs=True):
    if produce_outputs:
        print(f"\nFORECAST AVANZATO HOLT-WINTERS - Prossimi {giorni_forecast} giorni")
        print("=" * 80)
        print(f">>> PARAMETRO RICEVUTO: {giorni_forecast} giorni <<<")
        print("=" * 80)

    weekly = df.groupby('week').agg({'OFFERTO': 'sum', 'DATA': 'min'}).reset_index()
    weekly.columns = ['SETTIMANA', 'TOTALE', 'DATA_INIZIO']
    weekly = weekly.sort_values('SETTIMANA')

    weeks_ahead = max(0, int(np.ceil(giorni_forecast / 7)))
    if weeks_ahead == 0:
        if produce_outputs:
            print("   Giorni forecast pari a 0: nessun forecast generato")
        forecast_week_df = pd.DataFrame(columns=['SETTIMANA', 'FORECAST', 'CI_LOWER', 'CI_UPPER'])
    elif STATSMODELS_AVAILABLE and len(weekly) >= 8:
        try:
            from statsmodels.tsa.holtwinters import ExponentialSmoothing

            model_week = ExponentialSmoothing(
                weekly['TOTALE'].values,
                seasonal_periods=4,
                trend='add',
                seasonal='add',
                initialization_method='estimated'
            )
            fit_week = model_week.fit()
            forecast_week = fit_week.forecast(steps=weeks_ahead)

            resid_week = np.asarray(fit_week.resid)
            lower, upper = _interval_from_residuals(resid_week, forecast_week)

            start_settimana = weekly['SETTIMANA'].max() + 1
            forecast_week_df = pd.DataFrame({
                'SETTIMANA': range(start_settimana, start_settimana + weeks_ahead),
                'FORECAST': forecast_week,
                'CI_LOWER': lower,
                'CI_UPPER': upper
            })

            observed = weekly['TOTALE'].values
            sst = np.sum((observed - observed.mean()) ** 2)
            r2_week = float('nan')
            if sst > 0:
                r2_week = 1 - (fit_week.sse / sst)
            if produce_outputs:
                if np.isfinite(r2_week):
                    print(f"   Modello Holt-Winters applicato (R^2 stimato: {r2_week:.3f})")
                else:
                    print("   Modello Holt-Winters applicato (R^2 non calcolabile)")
        except Exception as e:
            if produce_outputs:
                print(f"   Holt-Winters settimanale fallito, uso fallback: {e}")
            forecast_week_df = forecast_settimanale_fallback(weekly, weeks_ahead)
    else:
        if produce_outputs:
            print("   Uso metodo fallback (dati insufficienti o statsmodels mancante)")
        forecast_week_df = forecast_settimanale_fallback(weekly, weeks_ahead)

    daily = df.groupby('DATA').agg({'OFFERTO': 'sum', 'GG SETT': 'first'}).reset_index()
    daily = daily.sort_values('DATA')
    daily.set_index('DATA', inplace=True)

    if giorni_forecast <= 0:
        forecast_daily_df = pd.DataFrame(columns=['DATA', 'FORECAST', 'GG_SETT', 'CI_LOWER', 'CI_UPPER'])
    elif STATSMODELS_AVAILABLE and len(daily) >= 14:
        try:
            from statsmodels.tsa.holtwinters import ExponentialSmoothing

            model_daily = ExponentialSmoothing(
                daily['OFFERTO'].values,
                seasonal_periods=7,
                trend='add',
                seasonal='add',
                initialization_method='estimated'
            )
            fit_daily = model_daily.fit()

            forecast_daily_values = fit_daily.forecast(steps=giorni_forecast)

            last_date = daily.index.max()
            future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=giorni_forecast, freq='D')

            forecast_daily_df = pd.DataFrame({
                'DATA': future_dates,
                'FORECAST': forecast_daily_values,
                'GG_SETT': [['lun','mar','mer','gio','ven','sab','dom'][d.weekday()] for d in future_dates]
            })

            resid_daily = np.asarray(fit_daily.resid)
            lower, upper = _interval_from_residuals(resid_daily, forecast_daily_df['FORECAST'])
            forecast_daily_df['CI_LOWER'] = lower
            forecast_daily_df['CI_UPPER'] = upper

            observed_daily = daily['OFFERTO'].values
            sst_daily = np.sum((observed_daily - observed_daily.mean()) ** 2)
            r2_daily = float('nan')
            if sst_daily > 0:
                r2_daily = 1 - (fit_daily.sse / sst_daily)
            if produce_outputs:
                if np.isfinite(r2_daily):
                    print(f"   Modello Holt-Winters giornaliero (R^2 stimato: {r2_daily:.3f})")
                else:
                    print("   Modello Holt-Winters giornaliero (R^2 non calcolabile)")
        except Exception as e:
            if produce_outputs:
                print(f"   Holt-Winters giornaliero fallito, uso fallback: {e}")
            forecast_daily_df = forecast_giornaliero_fallback(daily, giorni_forecast)
    else:
        if produce_outputs:
            print("   Uso metodo fallback giornaliero")
        forecast_daily_df = forecast_giornaliero_fallback(daily, giorni_forecast)

    pattern_intraday = _costruisci_pattern_intraday(df)
    forecast_fascia_df = _distribuisci_forecast_per_fascia(pattern_intraday, forecast_daily_df)

    if produce_outputs:
        print(f"   Forecast per fascia generato: {len(forecast_fascia_df)} slot previsti")
        print("\n4. Generazione Grafici...")

        if not forecast_week_df.empty:
            fig, ax = plt.subplots(figsize=(14, 6))
            ax.plot(weekly['SETTIMANA'], weekly['TOTALE'], marker='o', linewidth=2,
                    label='Storico', color='#2E86AB')
            ax.plot(forecast_week_df['SETTIMANA'], forecast_week_df['FORECAST'],
                    marker='s', linewidth=2.5, label='Forecast', color='#FF6B6B', linestyle='--')
            ax.fill_between(forecast_week_df['SETTIMANA'],
                             forecast_week_df['CI_LOWER'],
                             forecast_week_df['CI_UPPER'],
                             alpha=0.3, color='#FF6B6B', label='IC 95%')
            ax.set_title('Forecast Settimanale - Holt-Winters', fontsize=14, fontweight='bold')
            ax.set_xlabel('Settimana')
            ax.set_ylabel('Chiamate Totali')
            ax.legend()
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(f"{output_dir}/forecast_settimanale.png", dpi=150 if FAST_MODE else 300, bbox_inches='tight')

        if not forecast_daily_df.empty:
            fig, ax = plt.subplots(figsize=(16, 6))
            history_span = min(30, len(daily))
            if history_span > 0:
                ax.plot(daily.index[-history_span:], daily['OFFERTO'].values[-history_span:],
                        marker='o', linewidth=2, label=f'Storico (ultimi {history_span} gg)', color='#2E86AB')
            ax.plot(forecast_daily_df['DATA'], forecast_daily_df['FORECAST'],
                    marker='s', linewidth=2.5, label='Forecast', color='#FF6B6B', linestyle='--')
            ax.fill_between(forecast_daily_df['DATA'],
                             forecast_daily_df['CI_LOWER'],
                             forecast_daily_df['CI_UPPER'],
                             alpha=0.3, color='#FF6B6B', label='IC 95%')
            ax.set_title(f'Forecast Giornaliero - Prossimi {giorni_forecast} Giorni', fontsize=14, fontweight='bold')
            ax.set_xlabel('Data')
            ax.set_ylabel('Chiamate Totali')
            ax.legend()
            ax.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(f"{output_dir}/forecast_giornaliero.png", dpi=150 if FAST_MODE else 300, bbox_inches='tight')

        if not forecast_fascia_df.empty:
            primo_giorno = forecast_fascia_df[forecast_fascia_df['DATA'] == forecast_fascia_df['DATA'].min()]
            fig, ax = plt.subplots(figsize=(14, 6))
            ax.plot(primo_giorno['MINUTI'], primo_giorno['FORECAST_FASCIA'],
                    marker='o', linewidth=2.5, color='#A23B72')
            ax.fill_between(primo_giorno['MINUTI'], primo_giorno['FORECAST_FASCIA'],
                             alpha=0.3, color='#A23B72')
            ax.set_title(f"Forecast Intraday - {primo_giorno['DATA'].iloc[0].strftime('%Y-%m-%d')} ({primo_giorno['GG_SETT'].iloc[0]})",
                         fontsize=14, fontweight='bold')
            ax.set_xlabel('Ora del Giorno')
            ax.set_ylabel('Chiamate Previste per Slot')
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(f"{output_dir}/forecast_intraday_esempio.png", dpi=150 if FAST_MODE else 300, bbox_inches='tight')
        else:
            print("   Nota: nessun forecast per fascia disponibile, grafico intraday saltato.")

        print("   Grafici salvati")

        excel_path = Path(output_dir) / 'forecast_completo.xlsx'
        with safe_excel_writer(excel_path, engine='xlsxwriter') as (writer, actual_path):
            forecast_week_df.to_excel(writer, sheet_name='Forecast_Settimanale', index=False)
            forecast_daily_df.to_excel(writer, sheet_name='Forecast_Giornaliero', index=False)
            forecast_fascia_df.to_excel(writer, sheet_name='Forecast_per_Fascia', index=False)
            if not forecast_daily_df.empty:
                riepilogo = pd.DataFrame([
                    ['Periodo Forecast', f'{giorni_forecast} giorni'],
                    ['Metodo', 'Holt-Winters' if STATSMODELS_AVAILABLE else 'Fallback'],
                    ['Totale Chiamate Previste', f"{forecast_daily_df['FORECAST'].sum():,.0f}"],
                    ['Media Giornaliera Prevista', f"{forecast_daily_df['FORECAST'].mean():,.1f}"],
                    ['', ''],
                    ['Date Forecast', ''],
                    ['Prima data', forecast_daily_df['DATA'].min().strftime('%Y-%m-%d')],
                    ['Ultima data', forecast_daily_df['DATA'].max().strftime('%Y-%m-%d')]
                ], columns=['Parametro', 'Valore'])
                riepilogo.to_excel(writer, sheet_name='Riepilogo', index=False)
        print(f"   Excel salvato: {actual_path.name}")

        if not forecast_daily_df.empty:
            print("\n" + "=" * 80)
            print("RIEPILOGO FORECAST")
            print("=" * 80)
            print(f"Periodo: {forecast_daily_df['DATA'].min().strftime('%Y-%m-%d')} -> {forecast_daily_df['DATA'].max().strftime('%Y-%m-%d')}")
            print(f"Giorni previsti: {giorni_forecast}")
            print(f"Totale chiamate previste: {forecast_daily_df['FORECAST'].sum():,.0f}")
            print(f"Media giornaliera: {forecast_daily_df['FORECAST'].mean():,.1f}")
            if len(forecast_daily_df) >= 3:
                print("\nTop 3 giorni previsti:")
                top3 = forecast_daily_df.nlargest(3, 'FORECAST')[['DATA', 'GG_SETT', 'FORECAST']]
                for _, row in top3.iterrows():
                    print(f"  {row['DATA'].strftime('%Y-%m-%d')} ({row['GG_SETT']}): {row['FORECAST']:,.0f} chiamate")

    return {
        'settimanale': forecast_week_df,
        'giornaliero': forecast_daily_df,
        'per_fascia': forecast_fascia_df
    }


def forecast_settimanale_fallback(weekly, weeks_ahead):
    if weeks_ahead <= 0 or weekly.empty:
        return pd.DataFrame(columns=['SETTIMANA', 'FORECAST', 'CI_LOWER', 'CI_UPPER'])
    ma_value = weekly['TOTALE'].tail(4).mean()
    trend = weekly['TOTALE'].diff().tail(4).mean()
    trend = 0 if not np.isfinite(trend) else trend
    ma_value = 0 if not np.isfinite(ma_value) else ma_value
    forecast_values = [ma_value + trend * i for i in range(1, weeks_ahead + 1)]
    forecast_values = [max(0, v) for v in forecast_values]
    ultima_settimana = weekly['SETTIMANA'].max() if not weekly.empty else 0
    residuals = weekly['TOTALE'].rolling(4, min_periods=2).mean() - weekly['TOTALE']
    lower, upper = _stima_intervallo_confidenza(residuals.values, forecast_values, fallback_ratio=0.2)
    return pd.DataFrame({
        'SETTIMANA': range(int(ultima_settimana) + 1, int(ultima_settimana) + weeks_ahead + 1),
        'FORECAST': forecast_values,
        'CI_LOWER': lower,
        'CI_UPPER': upper
    })


def forecast_giornaliero_fallback(daily, giorni_forecast):
    if giorni_forecast <= 0:
        return pd.DataFrame(columns=['DATA', 'FORECAST', 'GG_SETT', 'CI_LOWER', 'CI_UPPER'])
    daily_with_dow = daily.copy()
    daily_with_dow['DOW'] = daily_with_dow.index.dayofweek
    pattern_dow = daily_with_dow.groupby('DOW')['OFFERTO'].mean().reindex(range(7))
    overall_mean = daily_with_dow['OFFERTO'].mean()
    fill_value = float(overall_mean) if np.isfinite(overall_mean) else 0.0
    pattern_dow = pattern_dow.fillna(fill_value)
    pattern_mean = pattern_dow.mean()
    if not np.isfinite(pattern_mean) or pattern_mean == 0.0:
        pattern_mean = fill_value if fill_value != 0.0 else 1.0

    trend = daily['OFFERTO'].diff().tail(7).mean()
    if not np.isfinite(trend):
        trend = 0.0
    base_value = daily['OFFERTO'].tail(7).mean()
    if not np.isfinite(base_value):
        base_value = daily['OFFERTO'].mean()
    if not np.isfinite(base_value):
        base_value = 0.0

    last_date = daily.index.max()
    future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=giorni_forecast, freq='D')

    forecasts = []
    for i, date in enumerate(future_dates):
        dow = date.dayofweek
        dow_value = pattern_dow.loc[dow]
        dow_factor = dow_value / pattern_mean if pattern_mean != 0 else 1.0
        forecast_val = (base_value + trend * (i + 1)) * dow_factor
        forecasts.append(max(0.0, forecast_val))

    residuals = daily['OFFERTO'].diff().dropna()
    lower, upper = _stima_intervallo_confidenza(residuals.values, forecasts, fallback_ratio=0.2)
    return pd.DataFrame({
        'DATA': future_dates,
        'FORECAST': forecasts,
        'GG_SETT': [['lun','mar','mer','gio','ven','sab','dom'][d.weekday()] for d in future_dates],
        'CI_LOWER': lower,
        'CI_UPPER': upper
    })


def _costruisci_pattern_intraday(df):
    """Costruisce pattern intraday percentuali per ciascun giorno della settimana."""
    pattern_intraday = {}
    ordine_giorni = ['lun', 'mar', 'mer', 'gio', 'ven', 'sab', 'dom']
    for giorno in ordine_giorni:
        df_giorno = df[df['GG SETT'] == giorno]
        if len(df_giorno) == 0:
            continue
        pattern_fascia = df_giorno.groupby(['FASCIA', 'MINUTI'])['OFFERTO'].mean().reset_index()
        pattern_fascia = pattern_fascia.sort_values('MINUTI')
        totale_giorno = pattern_fascia['OFFERTO'].sum()
        if totale_giorno > 0:
            pattern_fascia['PERCENTUALE'] = pattern_fascia['OFFERTO'] / totale_giorno
        else:
            pattern_fascia['PERCENTUALE'] = 0
        pattern_intraday[giorno] = pattern_fascia
    return pattern_intraday


def _distribuisci_forecast_per_fascia(pattern_intraday, daily_forecast_df):
    """Applica il pattern intraday ad un forecast giornaliero."""
    forecast_fascia_list = []
    if daily_forecast_df.empty:
        return pd.DataFrame(columns=['DATA', 'GG_SETT', 'FASCIA', 'MINUTI',
                                     'FORECAST_GIORNO', 'PERCENTUALE', 'FORECAST_FASCIA'])
    for _, row_day in daily_forecast_df.iterrows():
        giorno_sett = row_day['GG_SETT']
        if giorno_sett not in pattern_intraday:
            continue
        pattern = pattern_intraday[giorno_sett].copy()
        pattern['DATA'] = row_day['DATA']
        pattern['GG_SETT'] = giorno_sett
        pattern['FORECAST_GIORNO'] = row_day['FORECAST']
        pattern['FORECAST_FASCIA'] = row_day['FORECAST'] * pattern['PERCENTUALE']
        forecast_fascia_list.append(pattern[['DATA', 'GG_SETT', 'FASCIA', 'MINUTI',
                                             'FORECAST_GIORNO', 'PERCENTUALE', 'FORECAST_FASCIA']])

    if not forecast_fascia_list:
        return pd.DataFrame(columns=['DATA', 'GG_SETT', 'FASCIA', 'MINUTI',
                                     'FORECAST_GIORNO', 'PERCENTUALE', 'FORECAST_FASCIA'])
    return pd.concat(forecast_fascia_list, ignore_index=True)


def _stima_intervallo_confidenza(residuals, forecast_values, fallback_ratio=0.15):
    lower, upper = _interval_from_residuals(residuals, forecast_values)
    if np.all(lower == 0) and np.all(upper == 0):
        lower = np.clip(np.array(forecast_values) * (1 - fallback_ratio), a_min=0, a_max=None)
        upper = np.array(forecast_values) * (1 + fallback_ratio)
    return lower, upper


def _compute_error_metrics(actual, predicted):
    mask = actual.notna() & predicted.notna()
    if not mask.any():
        return None
    actual_valid = actual[mask]
    predicted_valid = predicted[mask]
    mae = float(np.mean(np.abs(actual_valid - predicted_valid)))
    mape = float(np.mean(np.abs((actual_valid - predicted_valid) / np.clip(actual_valid, 1e-8, None))) * 100)
    smape = float(np.mean(
        2 * np.abs(predicted_valid - actual_valid) / (np.abs(actual_valid) + np.abs(predicted_valid) + 1e-8)
    ) * 100)
    return {'MAE': mae, 'MAPE': mape, 'SMAPE': smape}


def _run_forecast_for_backtest(metodo, df_train, horizon):
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            if metodo == 'holtwinters':
                return _forecast_holtwinters(df_train, output_dir=tmpdir, giorni_forecast=horizon, produce_outputs=False)
            if metodo == 'pattern':
                return _forecast_pattern_based(df_train, horizon)
            if metodo == 'naive':
                return _forecast_naive_baseline(df_train, horizon)
            if metodo == 'sarima':
                return _forecast_sarima(df_train, giorni_forecast=horizon, produce_outputs=False)
            if metodo == 'prophet':
                return _forecast_prophet(df_train, giorni_forecast=horizon, produce_outputs=False)
            if metodo == 'tbats':
                return _forecast_tbats(df_train, giorni_forecast=horizon, produce_outputs=False)
            if metodo == 'intraday_dinamico':
                return _forecast_intraday_dinamico(df_train, giorni_forecast=horizon, produce_outputs=False)
    except Exception as exc:
        log_debug(f"Backtest {metodo} fallito: {exc}")
    return None


def _esegui_backtest(df, metodi, giorni_forecast, fast_mode=False):
    print("\nESECUZIONE BACKTEST (rolling origin)")
    print("=" * 80)
    daily_series = df.groupby('DATA')['OFFERTO'].sum().sort_index()

    t_backtest = time.time()
    base_horizons = [14, 30, 60, 90, giorni_forecast]
    max_windows = None
    if fast_mode:
        base_horizons = [min(30, giorni_forecast), giorni_forecast]
        max_windows = 2
    horizons = sorted({h for h in base_horizons if h > 0})

    summary = {}
    for horizon in horizons:
        horizon = int(horizon)
        min_train = max(28, horizon * 2)
        step = max(7, horizon // 2)
        if fast_mode:
            step = max(step, horizon)

        if len(daily_series) <= min_train + horizon:
            print(f"⚠️  Backtest {horizon} giorni saltato: servono almeno {min_train + horizon} giorni, trovati {len(daily_series)}")
            continue

        dates = daily_series.index.to_list()
        metrics = {m: [] for m in metodi}

        windows_done = 0
        t_horizon = time.time()
        for start_idx in range(min_train, len(dates) - horizon + 1, step):
            if max_windows and windows_done >= max_windows:
                break
            train_end = dates[start_idx - 1]
            test_dates = dates[start_idx:start_idx + horizon]
            df_train = df[df['DATA'] <= train_end]
            actual = daily_series.loc[test_dates]

            for metodo in metodi:
                result = _run_forecast_for_backtest(metodo, df_train, horizon)
                if result is None or 'giornaliero' not in result:
                    continue
                forecast_df = result['giornaliero'].copy()
                predicted = forecast_df.set_index('DATA')['FORECAST'].reindex(test_dates)
                metric = _compute_error_metrics(actual, predicted)
                if metric:
                    metrics[metodo].append(metric)
            windows_done += 1

        print(f"Metriche medie (rolling) - orizzonte {horizon} giorni:")
        for metodo, valori in metrics.items():
            if not valori:
                continue
            summary.setdefault(metodo, {'by_horizon': {}})
            summary[metodo]['by_horizon'][horizon] = {
                'MAE': float(np.mean([v['MAE'] for v in valori])),
                'MAPE': float(np.mean([v['MAPE'] for v in valori])),
                'SMAPE': float(np.mean([v['SMAPE'] for v in valori]))
            }
            valori_h = summary[metodo]['by_horizon'][horizon]
            print(f" - {metodo}: MAE={valori_h['MAE']:.2f}, MAPE={valori_h['MAPE']:.2f}%, SMAPE={valori_h['SMAPE']:.2f}%")
        _log_step_time(f"Backtest orizzonte {horizon} giorni", t_horizon)

    # Calcola media complessiva per compatibilità
    for metodo, valori in summary.items():
        all_metrics = list(valori.get('by_horizon', {}).values())
        if not all_metrics:
            continue
        summary[metodo]['MAE'] = float(np.mean([m['MAE'] for m in all_metrics]))
        summary[metodo]['MAPE'] = float(np.mean([m['MAPE'] for m in all_metrics]))
        summary[metodo]['SMAPE'] = float(np.mean([m['SMAPE'] for m in all_metrics]))

    if not summary:
        print("⚠️  Nessuna metrica calcolata (modelli non disponibili sui dati di train)")

    _log_step_time("Backtest complessivo", t_backtest)

    return summary


def carica_dati_consuntivo(file_path):
    """
    Carica file Excel consuntivo con stesso formato del forecast.
    Assume colonne: DATA, OFFERTO
    """
    df = pd.read_excel(file_path)
    df['DATA'] = pd.to_datetime(df['DATA'])
    # Aggrega per giorno se ci sono fasce orarie
    if 'FASCIA' in df.columns:
        df = df.groupby('DATA')['OFFERTO'].sum().reset_index()
    return df


def confronta_forecast_consuntivo(forecast_df, consuntivo_path, output_dir):
    """
    Confronta forecast con dati consuntivi reali.

    Args:
        forecast_df: DataFrame con colonne DATA e FORECAST (o colonne modelli)
        consuntivo_path: path al file Excel consuntivo
        output_dir: cartella output

    Returns:
        dict con:
            - confronto_df: DataFrame con forecast, consuntivo e errori
            - metriche: dict per modello con MAE, MAPE, SMAPE
            - periodo_overlap: date sovrapposte
    """
    print("\nCONFRONTO FORECAST vs CONSUNTIVO")
    print("=" * 80)

    # Carica consuntivo
    consuntivo_df = carica_dati_consuntivo(consuntivo_path)
    print(f"Consuntivo caricato: {len(consuntivo_df)} giorni")
    print(f"Periodo: {consuntivo_df['DATA'].min().date()} - {consuntivo_df['DATA'].max().date()}")

    # Identifica colonne modelli (escludi DATA)
    model_cols = [col for col in forecast_df.columns if col != 'DATA']

    # Merge su DATA
    merged = pd.merge(
        forecast_df,
        consuntivo_df,
        on='DATA',
        how='inner',
        suffixes=('_forecast', '_consuntivo')
    )

    if len(merged) == 0:
        print("⚠️ Nessuna sovrapposizione tra date forecast e consuntivo!")
        return None

    print(f"Date sovrapposte: {len(merged)} giorni")

    # Calcola metriche per ogni modello
    metriche = {}
    for model in model_cols:
        if model not in merged.columns:
            continue

        actual = merged['OFFERTO'].values
        predicted = merged[model].values

        # Rimuovi NaN
        mask = ~(np.isnan(actual) | np.isnan(predicted))
        actual = actual[mask]
        predicted = predicted[mask]

        if len(actual) == 0:
            continue

        mae = np.mean(np.abs(actual - predicted))
        mape = np.mean(np.abs((actual - predicted) / actual)) * 100
        smape = np.mean(2 * np.abs(predicted - actual) / (np.abs(predicted) + np.abs(actual))) * 100

        metriche[model] = {
            'MAE': float(mae),
            'MAPE': float(mape),
            'SMAPE': float(smape),
            'n_giorni': len(actual)
        }

        # Aggiungi colonne errore al DataFrame
        merged[f'{model}_errore'] = merged[model] - merged['OFFERTO']
        merged[f'{model}_errore_pct'] = ((merged[model] - merged['OFFERTO']) / merged['OFFERTO'] * 100)

    # Stampa risultati
    print("\nMetriche di accuratezza per modello:")
    for model, metrics in sorted(metriche.items(), key=lambda x: x[1]['MAPE']):
        print(f"  {model:20s}: MAE={metrics['MAE']:8.1f}  MAPE={metrics['MAPE']:6.2f}%  SMAPE={metrics['SMAPE']:6.2f}%")

    # Salva Excel dettagliato
    excel_path = Path(output_dir) / 'confronto_forecast_consuntivo.xlsx'
    with safe_excel_writer(excel_path, engine='xlsxwriter') as (writer, actual_path):
        # Sheet 1: Confronto giornaliero completo
        merged.to_excel(writer, sheet_name='Confronto_Giornaliero', index=False)

        # Sheet 2: Metriche riepilogo
        metrics_df = pd.DataFrame(metriche).T
        metrics_df.index.name = 'Modello'
        metrics_df = metrics_df.sort_values('MAPE')
        metrics_df.to_excel(writer, sheet_name='Metriche_Accuratezza')

        # Sheet 3: Aggregazione settimanale
        merged_copy = merged.copy()
        merged_copy['DATA'] = pd.to_datetime(merged_copy['DATA'])
        merged_copy = merged_copy.set_index('DATA')
        weekly = merged_copy.resample('W-MON').sum(numeric_only=True).reset_index()
        weekly.to_excel(writer, sheet_name='Confronto_Settimanale', index=False)

        # Sheet 4: Aggregazione mensile
        monthly = merged_copy.resample('MS').sum(numeric_only=True).reset_index()
        monthly.to_excel(writer, sheet_name='Confronto_Mensile', index=False)

    print(f"✅ File confronto salvato: {actual_path.name}")

    # Genera grafico comparativo
    _genera_grafico_confronto_consuntivo(merged, model_cols, output_dir)

    return {
        'confronto_df': merged,
        'metriche': metriche,
        'periodo_overlap': (merged['DATA'].min(), merged['DATA'].max()),
        'output_path': actual_path
    }


def _genera_grafico_confronto_consuntivo(df, model_cols, output_dir):
    """Genera grafici di confronto forecast vs consuntivo."""
    fig, axes = plt.subplots(3, 1, figsize=(16, 12))

    # Grafico 1: Linee forecast vs consuntivo
    axes[0].plot(df['DATA'], df['OFFERTO'],
                 label='CONSUNTIVO', linewidth=3, color='black', marker='o', markersize=4)

    colors = ['#2E86AB', '#A23B72', '#F18F01', '#6A994E', '#BC4B51', '#8B5CF6', '#C73E1D']
    for i, model in enumerate(model_cols[:7]):  # Max 7 modelli per leggibilità
        if model in df.columns:
            axes[0].plot(df['DATA'], df[model],
                        label=model.upper(), linewidth=2, alpha=0.7,
                        color=colors[i % len(colors)], linestyle='--')

    axes[0].set_title('Confronto Forecast vs Consuntivo', fontsize=14, fontweight='bold')
    axes[0].set_ylabel('Chiamate')
    axes[0].legend(loc='best')
    axes[0].grid(True, alpha=0.3)

    # Grafico 2: Errori percentuali
    for i, model in enumerate(model_cols[:7]):
        col_err = f'{model}_errore_pct'
        if col_err in df.columns:
            axes[1].plot(df['DATA'], df[col_err],
                        label=model.upper(), linewidth=2, alpha=0.7,
                        color=colors[i % len(colors)])

    axes[1].axhline(0, color='black', linestyle='-', linewidth=1)
    axes[1].set_title('Errore Percentuale per Modello', fontsize=14, fontweight='bold')
    axes[1].set_ylabel('Errore %')
    axes[1].legend(loc='best')
    axes[1].grid(True, alpha=0.3)

    # Grafico 3: Distribuzione errori (boxplot)
    error_data = []
    labels = []
    for model in model_cols[:7]:
        col_err = f'{model}_errore_pct'
        if col_err in df.columns:
            error_data.append(df[col_err].dropna())
            labels.append(model.upper())

    if error_data:
        axes[2].boxplot(error_data, labels=labels, vert=True, patch_artist=True)
        axes[2].axhline(0, color='red', linestyle='--', linewidth=1)
        axes[2].set_title('Distribuzione Errori per Modello', fontsize=14, fontweight='bold')
        axes[2].set_ylabel('Errore %')
        axes[2].grid(True, alpha=0.3, axis='y')
        plt.setp(axes[2].xaxis.get_majorticklabels(), rotation=45, ha='right')

    plt.tight_layout()
    plt.savefig(f'{output_dir}/confronto_forecast_consuntivo.png', dpi=150 if FAST_MODE else 300, bbox_inches='tight')
    print(f"   Grafico confronto salvato: confronto_forecast_consuntivo.png")
    plt.close()


def _process_single_fascia_intraday(args):
    """Helper per processare una singola fascia oraria in parallelo."""
    fascia, df_fascia_subset, future_dates, giorni_forecast = args
    
    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing
        
        forecast_results = []
        
        if len(df_fascia_subset) < 14:
            # Dati insufficienti, usa media storica
            media_per_dow = df_fascia_subset.groupby('DOW')['OFFERTO'].mean().to_dict()
            for future_date in future_dates:
                dow = future_date.dayofweek
                forecast_val = media_per_dow.get(dow, df_fascia_subset['OFFERTO'].mean())
                forecast_results.append({
                    'DATA': future_date,
                    'FASCIA': fascia,
                    'MINUTI': df_fascia_subset['MINUTI'].iloc[0] if len(df_fascia_subset) > 0 else 0,
                    'GG_SETT': ['lun','mar','mer','gio','ven','sab','dom'][dow],
                    'FORECAST': max(0, forecast_val)
                })
            return forecast_results
        
        # Crea serie temporale per questa fascia
        ts = df_fascia_subset.groupby('DATA')['OFFERTO'].mean().sort_index()
        ts = ts.asfreq('D', fill_value=0)
        
        try:
            # Modello Holt-Winters con stagionalita settimanale
            model = ExponentialSmoothing(
                ts.values,
                seasonal_periods=7,
                trend='add',
                seasonal='add',
                initialization_method='estimated'
            )
            fit = model.fit()
            forecast_vals = fit.forecast(steps=giorni_forecast)
        
        except Exception:
            # Fallback: usa media mobile con pattern settimanale
            media_per_dow = df_fascia_subset.groupby('DOW')['OFFERTO'].mean().to_dict()
            base = ts.tail(7).mean()
            forecast_vals = []
            for i, future_date in enumerate(future_dates):
                dow = future_date.dayofweek
                dow_factor = media_per_dow.get(dow, base) / base if base > 0 else 1.0
                forecast_vals.append(base * dow_factor)
        
        # Salva risultati
        for i, future_date in enumerate(future_dates):
            dow = future_date.dayofweek
            forecast_results.append({
                'DATA': future_date,
                'FASCIA': fascia,
                'MINUTI': df_fascia_subset['MINUTI'].iloc[0] if len(df_fascia_subset) > 0 else 0,
                'GG_SETT': ['lun','mar','mer','gio','ven','sab','dom'][dow],
                'FORECAST': max(0, forecast_vals[i] if i < len(forecast_vals) else 0)
            })
        
        return forecast_results
    
    except Exception as exc:
        # In caso di errore, ritorna lista vuota
        return []


def _forecast_intraday_dinamico(df, giorni_forecast=28, produce_outputs=False):
    """
    Forecast intraday dinamico con modelli separati per fascia oraria.
    Cattura le interazioni giorno×fascia in modo più accurato rispetto ai pattern fissi.
    """
    if not STATSMODELS_AVAILABLE:
        if produce_outputs:
            print("   ⚠️  Forecast intraday dinamico richiede statsmodels (pip install statsmodels)")
        return None

    if giorni_forecast <= 0:
        if produce_outputs:
            print(f"   ⚠️  Giorni forecast non valido: {giorni_forecast}")
        return None

    if len(df) < 100:  # Minimo dati per avere senso
        if produce_outputs:
            print(f"   ⚠️  Intraday dinamico richiede almeno 100 record, presenti solo {len(df)} record")
        return None

    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing

        # Prepara dati per fascia oraria con giorno settimana
        df_fascia = df.copy()
        df_fascia['DOW'] = df_fascia['DATA'].dt.dayofweek

        # Lista delle fasce uniche
        fasce_uniche = df_fascia.sort_values('MINUTI')['FASCIA'].unique()

        forecast_results = []
        last_date = df['DATA'].max()
        future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=giorni_forecast, freq='D')

        if produce_outputs:
            print(f"   Modellando {len(fasce_uniche)} fasce orarie...")

        # Prepara argomenti per processing parallelo
        from concurrent.futures import ThreadPoolExecutor
        
        fascia_args = []
        for fascia in fasce_uniche:
            df_questa_fascia = df_fascia[df_fascia['FASCIA'] == fascia].copy()
            fascia_args.append((fascia, df_questa_fascia, future_dates, giorni_forecast))
        
        # Processing parallelo delle fasce (solo se ci sono abbastanza fasce)
        forecast_results = []
        if len(fasce_uniche) >= 4 and not FAST_MODE:
            # Usa multiprocessing per >= 4 fasce in modalita normale
            if produce_outputs:
                print(f"   Processamento parallelo di {len(fasce_uniche)} fasce orarie...")
            with ThreadPoolExecutor(max_workers=min(4, len(fasce_uniche))) as executor:
                results = executor.map(_process_single_fascia_intraday, fascia_args)
                for result in results:
                    forecast_results.extend(result)
        else:
            # Processing sequenziale per poche fasce o fast mode
            for args in fascia_args:
                result = _process_single_fascia_intraday(args)
                forecast_results.extend(result)

        forecast_df = pd.DataFrame(forecast_results)

        # Calcola anche totale giornaliero per compatibilità
        daily_forecast = forecast_df.groupby(['DATA', 'GG_SETT'])['FORECAST'].sum().reset_index()
        daily_forecast.columns = ['DATA', 'GG_SETT', 'FORECAST']

        if produce_outputs:
            print(f"   Forecast intraday dinamico completato: {len(forecast_df)} slot previsti")

        return {
            'giornaliero': daily_forecast,
            'per_fascia': forecast_df
        }

    except Exception as exc:
        if produce_outputs:
            print(f"   Forecast intraday dinamico fallito: {exc}")
        return None


def _forecast_pattern_based(df, giorni_forecast):
    """Forecast basato su pattern settimanale e trend semplice (fallback attuale)."""
    daily = df.groupby('DATA').agg({'OFFERTO': 'sum', 'GG SETT': 'first'}).reset_index()
    daily = daily.sort_values('DATA').set_index('DATA')
    forecast_daily_df = forecast_giornaliero_fallback(daily, giorni_forecast)
    pattern_intraday = _costruisci_pattern_intraday(df)
    forecast_fascia_df = _distribuisci_forecast_per_fascia(pattern_intraday, forecast_daily_df)
    return {
        'giornaliero': forecast_daily_df,
        'per_fascia': forecast_fascia_df
    }


def _forecast_naive_baseline(df, giorni_forecast):
    """Forecast naïve: ultimo valore (con CI ±15%) distribuito per pattern intraday."""
    daily = df.groupby('DATA').agg({'OFFERTO': 'sum', 'GG SETT': 'first'}).reset_index()
    daily = daily.sort_values('DATA').set_index('DATA')
    if daily.empty or giorni_forecast <= 0:
        empty = pd.DataFrame(columns=['DATA', 'FORECAST', 'GG_SETT', 'CI_LOWER', 'CI_UPPER'])
        return {'giornaliero': empty, 'per_fascia': empty}

    last_value = float(daily['OFFERTO'].iloc[-1])
    future_dates = pd.date_range(start=daily.index.max() + timedelta(days=1),
                                 periods=giorni_forecast, freq='D')
    forecasts = np.full(giorni_forecast, last_value)
    residuals = daily['OFFERTO'].diff().dropna()
    lower, upper = _stima_intervallo_confidenza(residuals.values, forecasts, fallback_ratio=0.15)

    forecast_daily_df = pd.DataFrame({
        'DATA': future_dates,
        'FORECAST': forecasts,
        'GG_SETT': [['lun','mar','mer','gio','ven','sab','dom'][d.weekday()] for d in future_dates],
        'CI_LOWER': lower,
        'CI_UPPER': upper
    })

    pattern_intraday = _costruisci_pattern_intraday(df)
    forecast_fascia_df = _distribuisci_forecast_per_fascia(pattern_intraday, forecast_daily_df)
    return {
        'giornaliero': forecast_daily_df,
        'per_fascia': forecast_fascia_df
    }


def _forecast_sarima(df, giorni_forecast=28, order=(1, 1, 1), seasonal_order=(1, 0, 1, 7), produce_outputs=False):
    """Forecast con SARIMA; richiede statsmodels (già importato)."""
    if not STATSMODELS_AVAILABLE:
        if produce_outputs:
            print("   statsmodels non disponibile: SARIMA saltato")
        return None
    try:
        from statsmodels.tsa.statespace.sarimax import SARIMAX
    except ImportError:
        if produce_outputs:
            print("   Modulo SARIMAX non disponibile, modello SARIMA saltato")
        return None

    daily = df.groupby('DATA').agg({'OFFERTO': 'sum', 'GG SETT': 'first'}).reset_index()
    daily = daily.sort_values('DATA').set_index('DATA')
    if daily.empty or len(daily) < seasonal_order[-1] * 2:
        if produce_outputs:
            print("   Dati insufficienti per SARIMA")
        return None

    try:
        model = SARIMAX(
            daily['OFFERTO'],
            order=order,
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
            freq='D'
        )
        fit = model.fit(disp=False)
    except Exception as exc:
        if produce_outputs:
            print(f"   SARIMA fallito: {exc}")
        return None

    forecast_res = fit.get_forecast(steps=giorni_forecast)
    forecast_mean = forecast_res.predicted_mean
    conf_int = forecast_res.conf_int(alpha=0.10)
    lower = conf_int.iloc[:, 0]
    upper = conf_int.iloc[:, 1]

    future_dates = forecast_mean.index
    forecast_daily_df = pd.DataFrame({
        'DATA': future_dates,
        'FORECAST': forecast_mean.values,
        'GG_SETT': [['lun','mar','mer','gio','ven','sab','dom'][d.weekday()] for d in future_dates],
        'CI_LOWER': np.clip(lower.values, a_min=0, a_max=None),
        'CI_UPPER': np.clip(upper.values, a_min=0, a_max=None)
    })

    pattern_intraday = _costruisci_pattern_intraday(df)
    forecast_fascia_df = _distribuisci_forecast_per_fascia(pattern_intraday, forecast_daily_df)

    return {
        'giornaliero': forecast_daily_df,
        'per_fascia': forecast_fascia_df,
        'model': fit
    }


def _genera_festivita_italiane(anno_inizio, anno_fine):
    """
    Genera un DataFrame con le festività italiane principali.
    Include festività fisse, mobili (Pasqua), pre-festivi e post-festivi.
    """
    festivita_fisse = {
        'Capodanno': (1, 1),
        'Epifania': (1, 6),
        'Festa Liberazione': (4, 25),
        'Festa Lavoro': (5, 1),
        'Festa Repubblica': (6, 2),
        'Ferragosto': (8, 15),
        'Ognissanti': (11, 1),
        'Immacolata': (12, 8),
        'Natale': (12, 25),
        'Santo Stefano': (12, 26),
        'Capodanno_Vigilia': (12, 31),  # Vigilia Capodanno
    }

    festivita_list = []

    # Festività fisse
    for anno in range(anno_inizio, anno_fine + 1):
        for nome, (mese, giorno) in festivita_fisse.items():
            data_festivo = pd.Timestamp(anno, mese, giorno)

            # Festivo principale
            festivita_list.append({
                'holiday': nome,
                'ds': data_festivo,
                'lower_window': 0,
                'upper_window': 0
            })

            # ✨ NUOVO: Pre-festivo (solo se cade lun-ven)
            if data_festivo.dayofweek < 5:  # 0=lun, 4=ven
                pre_festivo = data_festivo - timedelta(days=1)
                if pre_festivo.dayofweek < 5:  # Solo se anche il giorno prima è feriale
                    festivita_list.append({
                        'holiday': f'{nome}_PreFestivo',
                        'ds': pre_festivo,
                        'lower_window': 0,
                        'upper_window': 0
                    })

            # ✨ NUOVO: Post-festivo (solo se cade lun-ven)
            if data_festivo.dayofweek < 5:
                post_festivo = data_festivo + timedelta(days=1)
                if post_festivo.dayofweek < 5:  # Solo se il giorno dopo è feriale
                    festivita_list.append({
                        'holiday': f'{nome}_PostFestivo',
                        'ds': post_festivo,
                        'lower_window': 0,
                        'upper_window': 0
                    })

    # Pasqua (calcolo approssimativo - per produzione usare libreria holidays)
    try:
        import holidays
        it_holidays = holidays.Italy(years=range(anno_inizio, anno_fine + 1))
        for data, nome in it_holidays.items():
            if 'Pasqua' in nome or 'Easter' in nome:
                data_pasqua = pd.Timestamp(data)

                # Pasqua + Lunedì dell'Angelo
                festivita_list.append({
                    'holiday': 'Pasqua',
                    'ds': data_pasqua,
                    'lower_window': 0,
                    'upper_window': 1  # Lunedì dell'Angelo
                })

                # ✨ Pre-Pasqua (Venerdì Santo e Giovedì)
                if data_pasqua.dayofweek == 6:  # Domenica
                    venerdi_santo = data_pasqua - timedelta(days=2)
                    giovedi_santo = data_pasqua - timedelta(days=3)

                    festivita_list.append({
                        'holiday': 'Venerdi_Santo',
                        'ds': venerdi_santo,
                        'lower_window': -1,  # Include anche giovedì
                        'upper_window': 0
                    })

                # ✨ Post-Pasqua (Martedì dopo Pasquetta)
                martedi_post = data_pasqua + timedelta(days=2)
                if martedi_post.dayofweek < 5:  # Se è feriale
                    festivita_list.append({
                        'holiday': 'PostPasqua',
                        'ds': martedi_post,
                        'lower_window': 0,
                        'upper_window': 0
                    })

    except ImportError:
        # Se holidays non disponibile, usa solo festività fisse
        pass

    # ✨ NUOVO: Periodi festivi estesi (Natale/Capodanno)
    for anno in range(anno_inizio, anno_fine + 1):
        # Periodo natalizio esteso (27-30 Dic)
        for giorno in range(27, 31):
            festivita_list.append({
                'holiday': 'Periodo_Natalizio',
                'ds': pd.Timestamp(anno, 12, giorno),
                'lower_window': 0,
                'upper_window': 0
            })

        # Periodo post-Capodanno (2-3 Gen)
        for giorno in [2, 3]:
            festivita_list.append({
                'holiday': 'Post_Capodanno',
                'ds': pd.Timestamp(anno, 1, giorno),
                'lower_window': 0,
                'upper_window': 0
            })

    return pd.DataFrame(festivita_list)


def _forecast_prophet(df, giorni_forecast=28, produce_outputs=False, escludi_festivita=None):
    """
    Forecast con Prophet (se disponibile) - con gestione festività.

    Args:
        df: DataFrame con dati storici
        giorni_forecast: numero giorni da prevedere
        produce_outputs: stampa messaggi di debug
        escludi_festivita: lista di festività da escludere (es. ['Natale', 'Santo_Stefano'])
                          Utile se cambi policy (es. apri il servizio quando prima era chiuso)
    """
    try:
        from prophet import Prophet
    except ImportError:
        if produce_outputs:
            print("   Prophet non installato, modello Prophet saltato")
        return None

    daily = df.groupby('DATA').agg({'OFFERTO': 'sum'}).reset_index().sort_values('DATA')
    if daily.empty or giorni_forecast <= 0:
        return None

    prophet_df = daily.rename(columns={'DATA': 'ds', 'OFFERTO': 'y'})

    # Genera festività italiane
    anno_min = daily['DATA'].min().year
    anno_max = daily['DATA'].max().year + int(np.ceil(giorni_forecast / 365)) + 1
    festivita = _genera_festivita_italiane(anno_min, anno_max)

    # ✨ NUOVO: Filtra festività escluse se richiesto
    if escludi_festivita:
        festivita = festivita[~festivita['holiday'].isin(escludi_festivita)]
        if produce_outputs and len(escludi_festivita) > 0:
            print(f"   Festività escluse da Prophet: {', '.join(escludi_festivita)}")

    model = Prophet(
        holidays=festivita if not festivita.empty else None,  # Gestione festività
        weekly_seasonality=True,
        yearly_seasonality=True if (anno_max - anno_min) >= 1 else False,  # Attiva se multi-anno
        daily_seasonality=False,
        changepoint_prior_scale=0.05,  # Più conservativo per dati call center
        seasonality_mode='multiplicative'  # Migliore per dati con trend crescente
    )
    model.add_seasonality(name='monthly', period=30.5, fourier_order=5)

    # ✅ NOVITÀ: Aggiungi regressori per weekend
    prophet_df['is_weekend'] = prophet_df['ds'].dt.dayofweek.isin([5, 6]).astype(int)
    model.add_regressor('is_weekend')

    try:
        model.fit(prophet_df)
    except Exception as exc:
        if produce_outputs:
            print(f"   Prophet fallito: {exc}")
        return None

    future = model.make_future_dataframe(periods=giorni_forecast, freq='D')
    # Aggiungi regressori anche al future dataframe
    future['is_weekend'] = future['ds'].dt.dayofweek.isin([5, 6]).astype(int)

    forecast = model.predict(future)
    future_forecast = forecast.tail(giorni_forecast)

    forecast_daily_df = pd.DataFrame({
        'DATA': future_forecast['ds'],
        'FORECAST': future_forecast['yhat'],
        'GG_SETT': [['lun','mar','mer','gio','ven','sab','dom'][d.weekday()] for d in future_forecast['ds']],
        'CI_LOWER': np.clip(future_forecast['yhat_lower'], a_min=0, a_max=None),
        'CI_UPPER': np.clip(future_forecast['yhat_upper'], a_min=0, a_max=None)
    })

    pattern_intraday = _costruisci_pattern_intraday(df)
    forecast_fascia_df = _distribuisci_forecast_per_fascia(pattern_intraday, forecast_daily_df)

    return {
        'giornaliero': forecast_daily_df,
        'per_fascia': forecast_fascia_df,
        'model': model
    }


def _forecast_tbats(df, giorni_forecast=28, produce_outputs=False):
    """
    Forecast con TBATS - gestisce multiple stagionalità automaticamente.
    Ottimo per catturare weekly + monthly + intraday patterns.
    """
    if not TBATS_AVAILABLE:
        if produce_outputs:
            print("   ⚠️  TBATS non installato (pip install tbats), modello TBATS saltato")
        return None

    daily = df.groupby('DATA').agg({'OFFERTO': 'sum'}).reset_index().sort_values('DATA')

    if daily.empty or giorni_forecast <= 0:
        if produce_outputs:
            print(f"   ⚠️  Dati insufficienti per TBATS (giorni: {len(daily)}, forecast: {giorni_forecast})")
        return None

    if len(daily) < 21:  # Minimo 3 settimane
        if produce_outputs:
            print(f"   ⚠️  TBATS richiede almeno 21 giorni di dati, presenti solo {len(daily)} giorni")
        return None

    try:
        # TBATS rileva automaticamente le stagionalità
        # seasonal_periods: [7 (weekly), 30.5 (monthly)]
        # PERFORMANCE: In fast_mode usa solo stagionalità settimanale per velocità
        seasonal_periods = [7] if FAST_MODE else [7, 30.5]

        estimator = TBATS(
            seasonal_periods=seasonal_periods,
            use_trend=True,
            use_box_cox=False,  # Box-Cox può essere instabile con dati call center
            n_jobs=-1  # PERFORMANCE: Usa tutti i core CPU disponibili (2-4x speedup)
        )

        if produce_outputs:
            print("   Fitting TBATS (può richiedere tempo)...")

        fitted_model = estimator.fit(daily['OFFERTO'].values)

        # Genera forecast
        forecast_values, conf_int = fitted_model.forecast(steps=giorni_forecast, confidence_level=0.95)

        last_date = daily['DATA'].max()
        future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=giorni_forecast, freq='D')

        forecast_daily_df = pd.DataFrame({
            'DATA': future_dates,
            'FORECAST': forecast_values,
            'GG_SETT': [['lun','mar','mer','gio','ven','sab','dom'][d.weekday()] for d in future_dates],
            'CI_LOWER': np.clip(conf_int['lower_bound'], a_min=0, a_max=None),
            'CI_UPPER': np.clip(conf_int['upper_bound'], a_min=0, a_max=None)
        })

        pattern_intraday = _costruisci_pattern_intraday(df)
        forecast_fascia_df = _distribuisci_forecast_per_fascia(pattern_intraday, forecast_daily_df)

        if produce_outputs:
            print(f"   TBATS completato - Componenti: {fitted_model.params.components}")

        return {
            'giornaliero': forecast_daily_df,
            'per_fascia': forecast_fascia_df,
            'model': fitted_model
        }

    except Exception as exc:
        if produce_outputs:
            print(f"   TBATS fallito: {exc}")
        return None


def _salva_forecast_excel(output_dir, nome_file, forecast_data):
    """Salva forecast giornaliero e per fascia in un unico Excel."""
    output_path = Path(output_dir) / nome_file
    with safe_excel_writer(output_path, engine='xlsxwriter') as (writer, actual_path):
        forecast_data['giornaliero'].to_excel(writer, sheet_name='Forecast_Giornaliero', index=False)
        if 'per_fascia' in forecast_data:
            forecast_data['per_fascia'].to_excel(writer, sheet_name='Forecast_per_Fascia', index=False)
    return actual_path


def _scegli_miglior_modello(backtest_metrics, available_models):
    """Seleziona il modello con la MAPE più bassa tra quelli disponibili."""
    if not backtest_metrics:
        return None
    target_horizon = None
    for m in backtest_metrics.values():
        horizons = list(m.get('by_horizon', {}).keys())
        if horizons:
            target_horizon = max(horizons)
            break
    if target_horizon is None:
        # fallback: usa la chiave orizzonte massima disponibile
        target_horizon = max([max(m.get('by_horizon', {0: 0}).keys()) for m in backtest_metrics.values()])

    candidates = []
    for model, valori in backtest_metrics.items():
        by_h = valori.get('by_horizon', {})
        if not by_h:
            continue
        nearest_h = min(by_h.keys(), key=lambda h: abs(h - target_horizon))
        metrics = by_h[nearest_h]
        candidates.append((model, nearest_h, metrics.get('MAPE', np.inf)))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[2])
    for model, _, _ in candidates:
        if model in available_models:
            return model
    return None


def _seleziona_top_modelli(backtest_metrics, available_models, top_k=2):
    """Ritorna i migliori modelli per MAPE, in base all'orizzonte più vicino."""
    if not backtest_metrics:
        return []

    # Identifica l'orizzonte target come quello più lungo disponibile
    target_horizon = None
    for valori in backtest_metrics.values():
        horizons = list(valori.get('by_horizon', {}).keys())
        if horizons:
            target_horizon = max(horizons)
            break
    if target_horizon is None:
        all_h = [max(m.get('by_horizon', {0: 0}).keys()) for m in backtest_metrics.values() if m.get('by_horizon')]
        target_horizon = max(all_h) if all_h else None

    if target_horizon is None:
        return []

    candidates = []
    for model, valori in backtest_metrics.items():
        if model not in available_models:
            continue
        by_h = valori.get('by_horizon', {})
        if not by_h:
            continue
        nearest_h = min(by_h.keys(), key=lambda h: abs(h - target_horizon))
        mape = by_h[nearest_h].get('MAPE', np.inf)
        candidates.append((model, nearest_h, mape))

    candidates.sort(key=lambda x: x[2])
    return [c[0] for c in candidates[:top_k]]


def _combina_metriche_ensemble(backtest_metrics, modelli):
    """Calcola metriche medie per un ensemble basato su modelli esistenti."""
    if not modelli:
        return None

    horizons = set()
    for modello in modelli:
        horizons.update(backtest_metrics.get(modello, {}).get('by_horizon', {}).keys())

    if not horizons:
        return None

    agg = {'by_horizon': {}}
    for h in sorted(horizons):
        metrics = []
        for modello in modelli:
            m = backtest_metrics.get(modello, {}).get('by_horizon', {}).get(h)
            if m:
                metrics.append(m)
        if not metrics:
            continue
        agg['by_horizon'][h] = {
            'MAE': float(np.mean([m['MAE'] for m in metrics])),
            'MAPE': float(np.mean([m['MAPE'] for m in metrics])),
            'SMAPE': float(np.mean([m['SMAPE'] for m in metrics]))
        }

    all_metrics = list(agg['by_horizon'].values())
    if not all_metrics:
        return None

    agg['MAE'] = float(np.mean([m['MAE'] for m in all_metrics]))
    agg['MAPE'] = float(np.mean([m['MAPE'] for m in all_metrics]))
    agg['SMAPE'] = float(np.mean([m['SMAPE'] for m in all_metrics]))
    return agg


def _salva_forecast_completo(output_dir, confronto_df, backtest_metrics, best_model=None):
    """Crea un unico file Excel con tutti i forecast e il migliore evidenziato."""
    output_path = Path(output_dir) / 'forecast_tutti_modelli.xlsx'

    confronto_export = confronto_df.copy()
    best_sheet = None
    best_metrics = None

    if best_model and best_model in confronto_export.columns:
        confronto_export['BEST_FORECAST'] = confronto_export[best_model]
        best_sheet = confronto_export[['DATA', 'BEST_FORECAST']].rename(columns={'BEST_FORECAST': best_model})
        if backtest_metrics and best_model in backtest_metrics:
            best_metrics = backtest_metrics[best_model]

    with safe_excel_writer(output_path, engine='xlsxwriter') as (writer, actual_path):
        confronto_export.to_excel(writer, sheet_name='Forecast_Tutti_Modelli', index=False)

        # Aggregazioni per confronto rapido
        confronto_export['DATA'] = pd.to_datetime(confronto_export['DATA'])
        confronto_export = confronto_export.sort_values('DATA')
        confronto_export.set_index('DATA', inplace=True)
        weekly = confronto_export.resample('W-MON').sum(numeric_only=True).reset_index().rename(columns={'DATA': 'SETTIMANA'})
        monthly = confronto_export.resample('MS').sum(numeric_only=True).reset_index().rename(columns={'DATA': 'MESE'})
        confronto_export.reset_index(inplace=True)

        weekly.to_excel(writer, sheet_name='Confronto_Settimanale', index=False)
        monthly.to_excel(writer, sheet_name='Confronto_Mensile', index=False)

        if best_sheet is not None:
            best_sheet.to_excel(writer, sheet_name=f'Best_{best_model.upper()}', index=False)

        if backtest_metrics:
            metrics_df = pd.DataFrame(backtest_metrics).T
            ordered_cols = [c for c in ['MAE', 'MAPE', 'SMAPE'] if c in metrics_df.columns]
            metrics_df = metrics_df[ordered_cols].sort_values('MAPE') if not metrics_df.empty else metrics_df
            metrics_df.to_excel(writer, sheet_name='Metriche_Backtest')

            # Esporta il dettaglio per orizzonte
            rows = []
            for modello, valori in backtest_metrics.items():
                for horizon, metriche in valori.get('by_horizon', {}).items():
                    rows.append({
                        'modello': modello,
                        'orizzonte_giorni': horizon,
                        'MAE': metriche.get('MAE'),
                        'MAPE': metriche.get('MAPE'),
                        'SMAPE': metriche.get('SMAPE')
                    })
            if rows:
                pd.DataFrame(rows).sort_values(['orizzonte_giorni', 'MAPE']).to_excel(
                    writer, sheet_name='Metriche_per_Orizzonte', index=False
                )

        summary_rows = []
        if best_model:
            summary_rows.append({'chiave': 'Miglior modello', 'valore': best_model})
            if best_metrics:
                summary_rows.append({'chiave': 'Metriche modello migliore',
                                      'valore': f"MAE={best_metrics.get('MAE'):.2f}, "
                                                f"MAPE={best_metrics.get('MAPE'):.2f}%, "
                                                f"SMAPE={best_metrics.get('SMAPE'):.2f}%"})
        if backtest_metrics:
            summary_rows.append({'chiave': 'Modelli valutati', 'valore': ', '.join(sorted(backtest_metrics.keys()))})
        if summary_rows:
            pd.DataFrame(summary_rows).to_excel(writer, sheet_name='Sintesi', index=False)

        guida_rows = [
            {'Modello': 'holtwinters', 'Uso consigliato': 'Pattern giornaliero regolare, stagionalità settimanale', 'Note': 'Veloce e robusto, richiede statsmodels'},
            {'Modello': 'pattern', 'Uso consigliato': 'Ripetizione media dei pattern storici', 'Note': 'Semplice baseline basata su stagionalità media'},
            {'Modello': 'naive', 'Uso consigliato': 'Baseline di controllo', 'Note': 'Replica l’ultimo valore o media breve periodo'},
            {'Modello': 'sarima', 'Uso consigliato': 'Trend + stagionalità con correlazione autoregressiva', 'Note': 'Più lento, utile con dati stabili e lunghi'},
            {'Modello': 'prophet', 'Uso consigliato': 'Più stagionalità + festività personalizzate', 'Note': 'Gestisce cambi policy con festivi esclusi'},
            {'Modello': 'tbats', 'Uso consigliato': 'Multiple stagionalità complesse', 'Note': 'Richiede tbats installato, può essere lento'},
            {'Modello': 'intraday_dinamico', 'Uso consigliato': 'Distribuzione per fascia oraria', 'Note': 'Allena 24 modelli, utile per staffing'},
            {'Modello': 'ensemble_top2', 'Uso consigliato': 'Media dei migliori due modelli', 'Note': 'Riduce varianza, richiede almeno due modelli riusciti'},
        ]
        pd.DataFrame(guida_rows).to_excel(writer, sheet_name='Guida_Modelli', index=False)

    print(f"   File completo modelli salvato: {actual_path.name}")

    # Monitoraggio rapido: salva una sintesi plain-text per confronti futuri
    monitor_path = Path(output_dir) / 'monitoraggio_metriche.txt'
    with open(monitor_path, 'a', encoding='utf-8') as f:
        f.write(f"Run del {datetime.now():%Y-%m-%d %H:%M:%S}\n")
        if best_model and best_metrics:
            f.write(f"Best model: {best_model} | MAE={best_metrics.get('MAE'):.2f} "
                    f"MAPE={best_metrics.get('MAPE'):.2f}% SMAPE={best_metrics.get('SMAPE'):.2f}%\n")
        if backtest_metrics:
            for modello, valori in backtest_metrics.items():
                by_h = valori.get('by_horizon', {})
                if by_h:
                    best_h = min(by_h, key=lambda h: by_h[h].get('MAPE', np.inf))
                    m = by_h[best_h]
                    f.write(f" - {modello}: MAPE migliore {m.get('MAPE'):.2f}% a {best_h} giorni\n")
        f.write("\n")

    return actual_path


def _calcola_ensemble_pesato(confronto_df, modelli, backtest_metrics):
    """Calcola una media pesata dei modelli basata sull'inverso del MAPE."""
    weights = {}
    valid_models = []
    
    if not backtest_metrics:
        return confronto_df[modelli].mean(axis=1)

    for m in modelli:
        metriche = backtest_metrics.get(m)
        if not metriche:
            continue
        
        # Cerca il miglior MAPE disponibile (globale o per orizzonte)
        mape = metriche.get('MAPE')
        if mape is None or not np.isfinite(mape):
             # Prova a cercare nei dettagli per orizzonte
             by_hor = metriche.get('by_horizon', {})
             if by_hor:
                 # Prende il MAPE medio tra gli orizzonti o il minimo
                 mapes = [v['MAPE'] for v in by_hor.values() if np.isfinite(v.get('MAPE', np.inf))]
                 if mapes:
                     mape = np.mean(mapes)
        
        if mape is not None and np.isfinite(mape) and mape > 0:
            weights[m] = 1 / mape  # Più basso è l'errore, più alto il peso
            valid_models.append(m)
        else:
             # Se MAPE è 0 (improbabile) o nan, diamo peso medio
             weights[m] = 0
    
    if not valid_models:
        return confronto_df[modelli].mean(axis=1)
    
    total_weight = sum(weights[m] for m in valid_models)
    if total_weight == 0:
        return confronto_df[modelli].mean(axis=1)
        
    weighted_sum = 0
    for m in valid_models:
        norm_weight = weights[m] / total_weight
        weighted_sum += confronto_df[m] * norm_weight

    return weighted_sum


# ============================================================================
# ENSEMBLE HYBRID: Combina le migliori componenti di ogni modello
# ============================================================================

def _estrai_componenti_modello(df, forecast_df, nome_modello):
    """
    Estrae componenti decomposte da un forecast esistente.

    Returns:
        dict con chiavi: 'trend', 'weekly', 'monthly', 'residual'
    """
    if forecast_df is None or 'FORECAST' not in forecast_df.columns:
        return None

    try:
        forecast_vals = forecast_df['FORECAST'].values
        dates = pd.to_datetime(forecast_df['DATA'])

        # Trend: regressione lineare semplice
        x = np.arange(len(forecast_vals))
        if len(x) > 1:
            coeffs = np.polyfit(x, forecast_vals, deg=1)
            trend = np.polyval(coeffs, x)
        else:
            trend = forecast_vals.copy()

        detrended = forecast_vals - trend

        # Pattern settimanale: media per giorno settimana
        weekly_pattern = np.zeros(7)
        for dow in range(7):
            mask = dates.dt.dayofweek == dow
            if mask.sum() > 0:
                weekly_pattern[dow] = detrended[mask].mean()

        # Normalizza pattern settimanale (media 1.0)
        if weekly_pattern.sum() > 0:
            weekly_pattern = weekly_pattern / weekly_pattern.mean()
        else:
            weekly_pattern = np.ones(7)

        # Pattern mensile: media per giorno del mese (1-31)
        monthly_pattern = np.zeros(31)
        for day in range(1, 32):
            mask = dates.dt.day == day
            if mask.sum() > 0:
                monthly_pattern[day-1] = detrended[mask].mean()

        # Normalizza pattern mensile
        if monthly_pattern.sum() > 0:
            monthly_pattern = monthly_pattern / monthly_pattern.mean()
        else:
            monthly_pattern = np.ones(31)

        # Residui
        weekly_component = np.array([weekly_pattern[d.dayofweek] for d in dates])
        monthly_component = np.array([monthly_pattern[d.day - 1] for d in dates])
        residual = forecast_vals - trend - detrended * (weekly_component + monthly_component - 2)

        return {
            'trend': trend,
            'weekly_pattern': weekly_pattern,
            'monthly_pattern': monthly_pattern,
            'residual': residual,
            'forecast_vals': forecast_vals
        }
    except Exception as exc:
        return None


def _score_componente_settimanale(df, componente):
    """Valuta la qualità del pattern settimanale."""
    if componente is None or 'weekly_pattern' not in componente:
        return float('inf')

    # Estrai pattern settimanale storico
    daily = df.groupby('DATA')['OFFERTO'].sum().reset_index()
    daily['DOW'] = pd.to_datetime(daily['DATA']).dt.dayofweek

    historical_weekly = np.zeros(7)
    for dow in range(7):
        mask = daily['DOW'] == dow
        if mask.sum() > 0:
            historical_weekly[dow] = daily.loc[mask, 'OFFERTO'].mean()

    if historical_weekly.sum() > 0:
        historical_weekly = historical_weekly / historical_weekly.mean()

    # Calcola differenza con pattern del modello
    diff = np.abs(componente['weekly_pattern'] - historical_weekly)
    return diff.mean()


def _score_componente_mensile(df, componente):
    """Valuta la qualità del pattern mensile."""
    if componente is None or 'monthly_pattern' not in componente:
        return float('inf')

    # Estrai pattern mensile storico
    daily = df.groupby('DATA')['OFFERTO'].sum().reset_index()
    daily['DOM'] = pd.to_datetime(daily['DATA']).dt.day

    historical_monthly = np.zeros(31)
    for day in range(1, 32):
        mask = daily['DOM'] == day
        if mask.sum() > 0:
            historical_monthly[day-1] = daily.loc[mask, 'OFFERTO'].mean()

    if historical_monthly.sum() > 0:
        historical_monthly = historical_monthly / historical_monthly.mean()

    # Calcola differenza
    diff = np.abs(componente['monthly_pattern'] - historical_monthly)
    return diff.mean()


def _score_componente_trend(df, componente):
    """Valuta la qualità del trend."""
    if componente is None or 'trend' not in componente:
        return float('inf')

    # Trend storico semplice
    daily = df.groupby('DATA')['OFFERTO'].sum().reset_index()
    daily = daily.sort_values('DATA')

    if len(daily) < 2:
        return float('inf')

    x = np.arange(len(daily))
    y = daily['OFFERTO'].values
    coeffs = np.polyfit(x, y, deg=1)
    historical_trend = coeffs[0]  # Slope

    # Trend del modello
    model_trend = (componente['trend'][-1] - componente['trend'][0]) / max(len(componente['trend']) - 1, 1)

    # Differenza relativa
    if abs(historical_trend) > 0.1:
        return abs((model_trend - historical_trend) / historical_trend)
    else:
        return abs(model_trend - historical_trend)


def _score_volume_totale(df, forecast_df):
    """Valuta l'accuratezza del volume totale previsto."""
    if forecast_df is None or 'FORECAST' not in forecast_df.columns:
        return float('inf')

    # Media giornaliera storica
    daily = df.groupby('DATA')['OFFERTO'].sum()
    historical_mean = daily.mean()

    # Media forecast
    forecast_mean = forecast_df['FORECAST'].mean()

    if historical_mean > 0:
        return abs((forecast_mean - historical_mean) / historical_mean)
    else:
        return abs(forecast_mean - historical_mean)


def _forecast_ensemble_hybrid(df, tutti_forecast, backtest_metrics, giorni_forecast=28, produce_outputs=False):
    """
    Ensemble ibrido che combina le migliori componenti di ogni modello.

    Seleziona:
    - Pattern settimanale dal modello migliore
    - Pattern mensile dal modello migliore
    - Trend dal modello migliore
    - Volume totale dal modello migliore
    - Distribuzione intraday dal modello intraday_dinamico

    Args:
        df: DataFrame storico
        tutti_forecast: dict {nome_modello: forecast_result}
        backtest_metrics: dict con metriche di backtest
        giorni_forecast: numero giorni da prevedere
        produce_outputs: se True stampa dettagli

    Returns:
        dict con 'giornaliero' e 'per_fascia'
    """
    if not tutti_forecast or len(tutti_forecast) < 2:
        if produce_outputs:
            print("   ⚠️ Ensemble hybrid richiede almeno 2 modelli base")
        return None

    try:
        if produce_outputs:
            print("   🎯 Ensemble Hybrid: analisi componenti...")
            print(f"   Modelli ricevuti: {list(tutti_forecast.keys())}")

        # Estrai componenti da ogni modello
        componenti = {}
        for nome_modello, forecast_result in tutti_forecast.items():
            if produce_outputs:
                print(f"   Elaborazione modello '{nome_modello}'...")

            if forecast_result is None:
                if produce_outputs:
                    print(f"      ⚠️ '{nome_modello}': risultato None, saltato")
                continue

            # Estrai DataFrame giornaliero
            if isinstance(forecast_result, dict) and 'giornaliero' in forecast_result:
                forecast_df = forecast_result['giornaliero']
                if produce_outputs:
                    print(f"      ✓ '{nome_modello}': DataFrame giornaliero trovato ({len(forecast_df)} righe)")
            else:
                forecast_df = forecast_result
                if produce_outputs:
                    print(f"      ✓ '{nome_modello}': usando result diretto")

            comp = _estrai_componenti_modello(df, forecast_df, nome_modello)
            if comp is not None:
                componenti[nome_modello] = comp
                if produce_outputs:
                    print(f"      ✅ '{nome_modello}': componenti estratte")
            else:
                if produce_outputs:
                    print(f"      ⚠️ '{nome_modello}': estrazione componenti fallita")

        if produce_outputs:
            print(f"\n   Componenti estratte da {len(componenti)} modelli: {list(componenti.keys())}")

        if len(componenti) < 2:
            if produce_outputs:
                print(f"   ⚠️ Ensemble hybrid: componenti insufficienti (servono almeno 2, trovate {len(componenti)})")
            return None

        # Valuta ogni componente
        scores_weekly = {}
        scores_monthly = {}
        scores_trend = {}
        scores_volume = {}

        for nome_modello, comp in componenti.items():
            scores_weekly[nome_modello] = _score_componente_settimanale(df, comp)
            scores_monthly[nome_modello] = _score_componente_mensile(df, comp)
            scores_trend[nome_modello] = _score_componente_trend(df, comp)

            # Volume: usa MAPE dal backtest se disponibile
            if backtest_metrics and nome_modello in backtest_metrics:
                mape = backtest_metrics[nome_modello].get('MAPE', float('inf'))
                scores_volume[nome_modello] = mape if np.isfinite(mape) else float('inf')
            else:
                forecast_df = tutti_forecast[nome_modello]
                if isinstance(forecast_df, dict):
                    forecast_df = forecast_df.get('giornaliero')
                scores_volume[nome_modello] = _score_volume_totale(df, forecast_df)

        # Seleziona migliori per ogni componente
        best_weekly = min(scores_weekly, key=scores_weekly.get) if scores_weekly else None
        best_monthly = min(scores_monthly, key=scores_monthly.get) if scores_monthly else None
        best_trend = min(scores_trend, key=scores_trend.get) if scores_trend else None
        best_volume = min(scores_volume, key=scores_volume.get) if scores_volume else None

        if produce_outputs:
            print(f"   📊 Migliori componenti:")
            print(f"      Pattern settimanale: {best_weekly} (score: {scores_weekly.get(best_weekly, 'N/A'):.3f})")
            print(f"      Pattern mensile: {best_monthly} (score: {scores_monthly.get(best_monthly, 'N/A'):.3f})")
            print(f"      Trend: {best_trend} (score: {scores_trend.get(best_trend, 'N/A'):.3f})")
            print(f"      Volume totale: {best_volume} (score: {scores_volume.get(best_volume, 'N/A'):.3f})")

        # Costruisci forecast combinato
        last_date = df['DATA'].max()
        future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=giorni_forecast, freq='D')

        # Base: volume medio storico
        daily_hist = df.groupby('DATA')['OFFERTO'].sum()
        base_volume = daily_hist.mean()

        # Applica correzione volume dal modello migliore
        if best_volume and best_volume in componenti:
            volume_factor = componenti[best_volume]['forecast_vals'].mean() / max(base_volume, 1)
        else:
            volume_factor = 1.0

        # Costruisci forecast finale
        forecast_vals = []
        for i, date in enumerate(future_dates):
            dow = date.dayofweek
            dom = date.day - 1  # 0-indexed

            # Base
            value = base_volume * volume_factor

            # Applica pattern settimanale
            if best_weekly and best_weekly in componenti:
                weekly_factor = componenti[best_weekly]['weekly_pattern'][dow]
                value *= weekly_factor

            # Applica pattern mensile (additivo, non moltiplicativo)
            if best_monthly and best_monthly in componenti:
                monthly_adjustment = componenti[best_monthly]['monthly_pattern'][dom]
                value *= monthly_adjustment

            # Applica trend
            if best_trend and best_trend in componenti:
                trend_component = componenti[best_trend]['trend']
                if len(trend_component) > 0:
                    # Estendi trend linearmente
                    trend_slope = (trend_component[-1] - trend_component[0]) / max(len(trend_component) - 1, 1)
                    trend_adjustment = trend_slope * i
                    value += trend_adjustment

            forecast_vals.append(max(0, value))

        # Crea DataFrame forecast giornaliero
        forecast_daily_df = pd.DataFrame({
            'DATA': future_dates,
            'FORECAST': forecast_vals,
            'GG_SETT': [['lun','mar','mer','gio','ven','sab','dom'][d.weekday()] for d in future_dates]
        })

        # Calcola intervalli di confidenza (combinazione dei modelli)
        residuals_combined = []
        for nome_modello, comp in componenti.items():
            if 'residual' in comp:
                residuals_combined.extend(comp['residual'])

        if residuals_combined:
            lower, upper = _stima_intervallo_confidenza(
                np.array(residuals_combined),
                forecast_vals,
                fallback_ratio=0.20
            )
            forecast_daily_df['CI_LOWER'] = lower
            forecast_daily_df['CI_UPPER'] = upper

        # Distribuzione intraday: usa intraday_dinamico se disponibile, altrimenti pattern storico
        if 'intraday_dinamico' in tutti_forecast and tutti_forecast['intraday_dinamico'] is not None:
            intraday_result = tutti_forecast['intraday_dinamico']
            if isinstance(intraday_result, dict) and 'per_fascia' in intraday_result:
                # Usa distribuzione intraday dal modello specializzato
                pattern_intraday = {}
                fascia_df = intraday_result['per_fascia']

                for _, row in fascia_df.iterrows():
                    key = (row['GG_SETT'], row['FASCIA'])
                    if key not in pattern_intraday:
                        pattern_intraday[key] = []
                    pattern_intraday[key].append(row['FORECAST'])

                # Media per chiave
                for key in pattern_intraday:
                    pattern_intraday[key] = np.mean(pattern_intraday[key])

                forecast_fascia_df = _distribuisci_forecast_per_fascia(pattern_intraday, forecast_daily_df)
            else:
                pattern_intraday = _costruisci_pattern_intraday(df)
                forecast_fascia_df = _distribuisci_forecast_per_fascia(pattern_intraday, forecast_daily_df)
        else:
            pattern_intraday = _costruisci_pattern_intraday(df)
            forecast_fascia_df = _distribuisci_forecast_per_fascia(pattern_intraday, forecast_daily_df)

        if produce_outputs:
            print(f"   ✅ Ensemble Hybrid completato: {len(forecast_daily_df)} giorni previsti")

        return {
            'giornaliero': forecast_daily_df,
            'per_fascia': forecast_fascia_df,
            'metadata': {
                'best_weekly': best_weekly,
                'best_monthly': best_monthly,
                'best_trend': best_trend,
                'best_volume': best_volume,
                'scores': {
                    'weekly': scores_weekly,
                    'monthly': scores_monthly,
                    'trend': scores_trend,
                    'volume': scores_volume
                }
            }
        }

    except Exception as exc:
        if produce_outputs:
            print(f"   ❌ Ensemble hybrid fallito: {exc}")
        import traceback
        if produce_outputs:
            traceback.print_exc()
        return None


def _correggi_forecast_con_storico(forecast_df, df_storico, soglia_minima=5):
    """
    Corregge il forecast basandosi sui pattern storici degli stessi giorni dell'anno.

    Se nello storico un certo giorno/mese (es. 1 novembre) aveva SEMPRE volume molto basso
    (< soglia_minima), allora anche il forecast per quel giorno viene azzerato.

    Args:
        forecast_df: DataFrame con colonne ['DATA', 'FORECAST', ...]
        df_storico: DataFrame storico originale con colonne ['DATA', 'OFFERTO']
        soglia_minima: soglia sotto cui considerare il giorno "chiuso" (default: 5 chiamate)

    Returns:
        DataFrame forecast corretto
    """
    # Aggrega storico per giorno
    daily_storico = df_storico.groupby('DATA')['OFFERTO'].sum().reset_index()

    # Per ogni giorno nello storico, estrai mese/giorno
    daily_storico['MESE_GIORNO'] = daily_storico['DATA'].dt.strftime('%m-%d')

    # Identifica giorni che sono SEMPRE stati chiusi/quasi chiusi (< soglia)
    giorni_chiusi = daily_storico.groupby('MESE_GIORNO').agg({
        'OFFERTO': ['count', 'mean', 'max']
    }).reset_index()
    giorni_chiusi.columns = ['MESE_GIORNO', 'occorrenze', 'media', 'massimo']

    # Un giorno è "chiuso" se:
    # - La media è < soglia_minima E
    # - Il massimo storico è < soglia_minima * 2 (per evitare falsi positivi)
    # - Ha almeno 1 occorrenza nello storico
    giorni_da_azzerare = giorni_chiusi[
        (giorni_chiusi['media'] < soglia_minima) &
        (giorni_chiusi['massimo'] < soglia_minima * 2) &
        (giorni_chiusi['occorrenze'] >= 1)
    ]['MESE_GIORNO'].tolist()

    if not giorni_da_azzerare:
        return forecast_df  # Nessuna correzione necessaria

    # Applica correzione al forecast
    forecast_corretto = forecast_df.copy()
    forecast_corretto['MESE_GIORNO'] = forecast_corretto['DATA'].dt.strftime('%m-%d')

    mask_azzerare = forecast_corretto['MESE_GIORNO'].isin(giorni_da_azzerare)
    n_giorni_azzerati = mask_azzerare.sum()

    if n_giorni_azzerati > 0:
        # Azzera la colonna FORECAST
        forecast_corretto.loc[mask_azzerare, 'FORECAST'] = 0

        # Azzera anche CI_LOWER e CI_UPPER se presenti
        if 'CI_LOWER' in forecast_corretto.columns:
            forecast_corretto.loc[mask_azzerare, 'CI_LOWER'] = 0
        if 'CI_UPPER' in forecast_corretto.columns:
            forecast_corretto.loc[mask_azzerare, 'CI_UPPER'] = 0

        print(f"   🔧 Corretti {n_giorni_azzerati} giorni di forecast basandosi su pattern storico")
        print(f"      Giorni azzerati: {', '.join(sorted(set(forecast_corretto[mask_azzerare]['DATA'].dt.strftime('%d/%m'))))}")

    # Rimuovi colonna temporanea
    forecast_corretto = forecast_corretto.drop(columns=['MESE_GIORNO'])

    return forecast_corretto


def genera_forecast_modelli(df, output_dir, giorni_forecast=28, metodi=None, escludi_festivita=None, fast_mode=False):
    """
    Esegue più modelli di forecast in parallelo e produce un confronto.

    Args:
        df: dataframe sorgente
        output_dir: cartella output
        giorni_forecast: orizzonte di forecast
        metodi: iterabile con i metodi da eseguire
                 (valori supportati: 'holtwinters', 'pattern', 'naive', 'sarima', 'prophet', 'tbats', 'intraday_dinamico')
        escludi_festivita: lista festività da escludere da Prophet (es. ['Natale'] se apri quando prima eri chiuso)
    """
    if fast_mode:
        print("   ⚡ Modalita veloce (fast mode): eseguo solo modelli rapidi e TBATS senza grafici")
    if metodi is None:
        if fast_mode:
            metodi = ('holtwinters', 'naive', 'pattern', 'intraday_dinamico')
        else:
            metodi = ('holtwinters', 'pattern', 'naive', 'sarima', 'prophet', 'tbats', 'intraday_dinamico')

    risultati = {}
    confronto_frames = []
    stati_modelli = []

    for metodo in metodi:
        metodo = metodo.lower()
        detail = ""
        success = False
        try:
            if metodo == 'holtwinters':
                risultati[metodo] = _forecast_holtwinters(df, output_dir, giorni_forecast, produce_outputs=True)
            elif metodo == 'pattern':
                risultati[metodo] = _forecast_pattern_based(df, giorni_forecast)
                if risultati[metodo] is not None:
                    actual_path = _salva_forecast_excel(output_dir, 'forecast_pattern.xlsx', risultati[metodo])
                    print(f"   Forecast pattern salvato: {actual_path.name}")
            elif metodo == 'naive':
                risultati[metodo] = _forecast_naive_baseline(df, giorni_forecast)
                if risultati[metodo] is not None:
                    actual_path = _salva_forecast_excel(output_dir, 'forecast_naive.xlsx', risultati[metodo])
                    print(f"   Forecast naive salvato: {actual_path.name}")
            elif metodo == 'sarima':
                risultati[metodo] = _forecast_sarima(df, giorni_forecast, produce_outputs=False)
                if risultati[metodo] is not None:
                    actual_path = _salva_forecast_excel(output_dir, 'forecast_sarima.xlsx', risultati[metodo])
                    print(f"   Forecast SARIMA salvato: {actual_path.name}")
            elif metodo == 'prophet':
                risultati[metodo] = _forecast_prophet(df, giorni_forecast, produce_outputs=False, escludi_festivita=escludi_festivita)
                if risultati[metodo] is not None:
                    actual_path = _salva_forecast_excel(output_dir, 'forecast_prophet.xlsx', risultati[metodo])
                    print(f"   Forecast Prophet salvato: {actual_path.name}")
            elif metodo == 'tbats':
                print(f"   Avvio TBATS...")
                risultati[metodo] = _forecast_tbats(df, giorni_forecast, produce_outputs=not fast_mode)
                if risultati[metodo] is not None:
                    actual_path = _salva_forecast_excel(output_dir, 'forecast_tbats.xlsx', risultati[metodo])
                    print(f"   ✅ Forecast TBATS salvato: {actual_path.name}")
                else:
                    detail = "TBATS non generato (dipendenze o dati insufficienti)"
                    print(f"   ⚠️  Forecast TBATS non generato (verifica messaggi sopra)")
            elif metodo == 'intraday_dinamico':
                print(f"   Avvio Forecast Intraday Dinamico...")
                risultati[metodo] = _forecast_intraday_dinamico(df, giorni_forecast, produce_outputs=True)
                if risultati[metodo] is not None:
                    actual_path = _salva_forecast_excel(output_dir, 'forecast_intraday_dinamico.xlsx', risultati[metodo])
                    print(f"   ✅ Forecast Intraday Dinamico salvato: {actual_path.name}")
                else:
                    detail = "Intraday dinamico non disponibile (dipendenze o dati insufficienti)"
                    print(f"   ⚠️  Forecast Intraday Dinamico non generato (verifica messaggi sopra)")
            elif metodo == 'ensemble_hybrid' or metodo == 'hybrid':
                # Ensemble hybrid viene eseguito DOPO il backtest, non qui nel loop
                print(f"   ⏭️  Ensemble Hybrid verrà calcolato dopo il backtest...")
                continue
            else:
                print(f"   Metodo forecast '{metodo}' non riconosciuto, ignorato.")
                risultati[metodo] = None
                detail = "Metodo non riconosciuto"

            result = risultati.get(metodo)
            if result is not None and 'giornaliero' in result and not result['giornaliero'].empty:
                success = True
                detail = detail or "Completato"

                # ✨ NUOVO: Correggi forecast basandosi su pattern storico (giorni chiusi)
                result['giornaliero'] = _correggi_forecast_con_storico(
                    result['giornaliero'],
                    df,
                    soglia_minima=5
                )

                daily_df = result['giornaliero'][['DATA', 'FORECAST']].copy()
                daily_df.rename(columns={'FORECAST': metodo}, inplace=True)
                confronto_frames.append(daily_df)
            else:
                detail = detail or "Nessun output generato"
        except Exception as exc:
            risultati[metodo] = None
            detail = f"Errore: {exc}"

        stati_modelli.append({'metodo': metodo, 'successo': success, 'dettaglio': detail})

    confronto = None
    if confronto_frames:
        confronto = confronto_frames[0]
        for frame in confronto_frames[1:]:
            confronto = confronto.merge(frame, on='DATA', how='outer')
        confronto = confronto.sort_values('DATA')
        confronto_path = _salva_forecast_excel(output_dir, 'forecast_confronto_modelli.xlsx',
                                               {'giornaliero': confronto})
        print(f"   Confronto modelli salvato: {confronto_path.name}")

        # Genera grafico di confronto
        _genera_grafico_confronto_modelli(confronto, output_dir)

    if stati_modelli:
        print("\nRIEPILOGO STATO MODELLI")
        print("-" * 80)
        for stato in stati_modelli:
            simbolo = "✅" if stato['successo'] else "⚠️"
            print(f" {simbolo} {stato['metodo']}: {stato['dettaglio']}")

    backtest_metrics = _esegui_backtest(df, metodi, giorni_forecast, fast_mode=fast_mode)
    best_model = None
    if backtest_metrics:
        risultati['backtest'] = backtest_metrics

    # ✨ NUOVO: Ensemble Hybrid - combina le migliori componenti di ogni modello
    if 'ensemble_hybrid' in metodi or 'hybrid' in metodi:
        print("\n" + "="*80)
        print("🎯 ENSEMBLE HYBRID - COMBINAZIONE COMPONENTI MIGLIORI")
        print("="*80)
        print(f"   Modelli disponibili per analisi: {list(risultati.keys())}")
        print(f"   Backtest metrics disponibili: {list(backtest_metrics.keys()) if backtest_metrics else 'Nessuno'}")

        try:
            hybrid_result = _forecast_ensemble_hybrid(
                df,
                risultati,
                backtest_metrics,
                giorni_forecast,
                produce_outputs=True
            )

            print(f"\n   Risultato ensemble_hybrid: {type(hybrid_result)}")

            if hybrid_result is not None:
                print(f"   ✅ Ensemble Hybrid generato con successo!")
                risultati['ensemble_hybrid'] = hybrid_result

                # Aggiungi al confronto
                if confronto is not None and 'giornaliero' in hybrid_result:
                    hybrid_daily = hybrid_result['giornaliero'][['DATA', 'FORECAST']].copy()
                    hybrid_daily.rename(columns={'FORECAST': 'ensemble_hybrid'}, inplace=True)
                    confronto = confronto.merge(hybrid_daily, on='DATA', how='outer')
                    confronto = confronto.sort_values('DATA')
                    print(f"   ✅ Aggiunto al confronto modelli")

                # Salva file
                actual_path = _salva_forecast_excel(output_dir, 'forecast_ensemble_hybrid.xlsx', hybrid_result)
                print(f"   ✅ Forecast Ensemble Hybrid salvato: {actual_path}")

                # Aggiungi metadata al riepilogo
                if 'metadata' in hybrid_result:
                    meta = hybrid_result['metadata']
                    detail = f"Best: weekly={meta.get('best_weekly', 'N/A')}, monthly={meta.get('best_monthly', 'N/A')}, trend={meta.get('best_trend', 'N/A')}, volume={meta.get('best_volume', 'N/A')}"
                    print(f"   📊 {detail}")
                else:
                    detail = "Completato"

                stati_modelli.append({
                    'metodo': 'ensemble_hybrid',
                    'successo': True,
                    'dettaglio': detail
                })
                print("="*80 + "\n")
            else:
                print(f"   ⚠️ Ensemble Hybrid non generato (richiede almeno 2 modelli base)")
                print(f"   Debug: risultati keys = {list(risultati.keys())}")
                stati_modelli.append({
                    'metodo': 'ensemble_hybrid',
                    'successo': False,
                    'dettaglio': "Richiede almeno 2 modelli base"
                })
                print("="*80 + "\n")
        except Exception as exc:
            import traceback
            print(f"   ❌ ERRORE Ensemble Hybrid: {exc}")
            print(f"   Traceback completo:")
            traceback.print_exc()
            stati_modelli.append({
                'metodo': 'ensemble_hybrid',
                'successo': False,
                'dettaglio': f"Errore: {exc}"
            })
            print("="*80 + "\n")

    ensemble_models = []
    if confronto is not None:
        available_models = [col for col in confronto.columns if col != 'DATA']
        if backtest_metrics:
            best_model = _scegli_miglior_modello(backtest_metrics, available_models)
            ensemble_models = _seleziona_top_modelli(backtest_metrics, available_models, top_k=2)
            if len(ensemble_models) >= 2:
                # Confronto['ensemble_top2'] = confronto[ensemble_models].mean(axis=1)
                # Sostituito con media pesata
                confronto['ensemble_top2'] = _calcola_ensemble_pesato(confronto, ensemble_models, backtest_metrics)
                available_models.append('ensemble_top2')
                stati_modelli.append({
                    'metodo': 'ensemble_top2',
                    'successo': True,
                    'dettaglio': f"Media dei migliori: {', '.join(ensemble_models)}"
                })
                if backtest_metrics:
                    ensemble_metrics = _combina_metriche_ensemble(backtest_metrics, ensemble_models)
                    if ensemble_metrics:
                        backtest_metrics = dict(backtest_metrics)  # evita side-effects
                        backtest_metrics['ensemble_top2'] = ensemble_metrics
        _salva_forecast_completo(output_dir, confronto, backtest_metrics, best_model)

    if confronto is not None:
        risultati['confronto_df'] = confronto.copy()

    if best_model:
        risultati['miglior_modello'] = best_model
        print(f"\n   Miglior modello selezionato dal backtest: {best_model}")
    if ensemble_models:
        risultati['ensemble'] = ensemble_models
        print(f"   Ensemble calcolato sui modelli: {', '.join(ensemble_models)}")

    return risultati


def _genera_grafico_confronto_modelli(confronto_df, output_dir):
    """Genera un grafico comparativo tra tutti i modelli di forecast."""
    fig, axes = plt.subplots(2, 1, figsize=(16, 10))

    # Grafico 1: Linee per ogni modello
    metodi_cols = [col for col in confronto_df.columns if col != 'DATA']
    colors_map = {
        'holtwinters': '#2E86AB',
        'prophet': '#A23B72',
        'tbats': '#F18F01',
        'sarima': '#C73E1D',
        'pattern': '#6A994E',
        'naive': '#BC4B51',
        'intraday_dinamico': '#8B5CF6',
        'ensemble_hybrid': '#FF6B6B',
        'ensemble_top2': '#4ECDC4'
    }

    for metodo in metodi_cols:
        if metodo in confronto_df.columns:
            color = colors_map.get(metodo, np.random.rand(3,))
            axes[0].plot(confronto_df['DATA'], confronto_df[metodo],
                        label=metodo.upper(), linewidth=2, marker='o', markersize=3,
                        color=color, alpha=0.8)

    axes[0].set_title('Confronto Forecast tra Modelli', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Data')
    axes[0].set_ylabel('Chiamate Previste')
    axes[0].legend(loc='best')
    axes[0].grid(True, alpha=0.3)
    axes[0].tick_params(axis='x', rotation=45)

    # Grafico 2: Deviazione dalla media dei modelli
    confronto_numeric = confronto_df[metodi_cols]
    media_modelli = confronto_numeric.mean(axis=1)

    for metodo in metodi_cols:
        if metodo in confronto_df.columns:
            deviazione = ((confronto_df[metodo] - media_modelli) / media_modelli * 100)
            color = colors_map.get(metodo, np.random.rand(3,))
            axes[1].plot(confronto_df['DATA'], deviazione,
                        label=metodo.upper(), linewidth=2, alpha=0.7, color=color)

    axes[1].axhline(0, color='black', linestyle='--', linewidth=1)
    axes[1].set_title('Deviazione % dalla Media dei Modelli', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Data')
    axes[1].set_ylabel('Deviazione %')
    axes[1].legend(loc='best')
    axes[1].grid(True, alpha=0.3)
    axes[1].tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/confronto_modelli_forecast.png', dpi=150 if FAST_MODE else 300, bbox_inches='tight')
    print(f"   Grafico confronto modelli salvato: confronto_modelli_forecast.png")
    plt.close()


def genera_forecast_avanzato(df, output_dir, giorni_forecast=28):
    """Compatibilità retro: esegue solo Holt-Winters con output completo."""
    return _forecast_holtwinters(df, output_dir, giorni_forecast, produce_outputs=True)



# =============================================================================
# VALUTAZIONE FORECAST
# =============================================================================

def _calcola_metriche_forecast(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    mae = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
    mask = y_true != 0
    if mask.any():
        mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)
    else:
        mape = np.nan
    return {'MAE': mae, 'RMSE': rmse, 'MAPE': mape}


def valuta_modelli_forecast(df, output_dir, giorni_forecast=28, min_train_giorni=56, step_giorni=7, fast_mode=False):
    print("\nVALUTAZIONE FORECAST HOLT-WINTERS")

    if not STATSMODELS_AVAILABLE:
        print("  statsmodels non disponibile, salto valutazione")
        return None

    daily = df.groupby('DATA')['OFFERTO'].sum().sort_index()
    if not isinstance(daily.index, pd.DatetimeIndex):
        raise ValueError("La colonna DATA deve essere di tipo datetime per la valutazione forecast")

    if len(daily) < min_train_giorni + giorni_forecast:
        print("  Dati insufficienti per backtest (richiesti almeno "
              f"{min_train_giorni + giorni_forecast} giorni, disponibili {len(daily)})")
        return None

    daily = daily.asfreq('D', fill_value=0)
    min_train = max(min_train_giorni, giorni_forecast * 2)
    step = max(1, step_giorni)
    max_splits = None
    if fast_mode:
        step = max(step, giorni_forecast)
        max_splits = 3

    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing
    except ImportError:
        print("  Impossibile importare ExponentialSmoothing, salto valutazione")
        return None

    risultati = []
    split_indices = range(min_train, len(daily) - giorni_forecast + 1, step)
    total_splits = len(list(split_indices))
    if total_splits == 0:
        print("  Dati insufficienti per creare split di validazione")
        return None

    print(f"  Eseguo {total_splits} split di validazione rolling (orizzonte {giorni_forecast} giorni)")

    for idx, cutoff in enumerate(range(min_train, len(daily) - giorni_forecast + 1, step), 1):
        if max_splits and idx > max_splits:
            break
        train = daily.iloc[:cutoff]
        test = daily.iloc[cutoff:cutoff + giorni_forecast]

        try:
            model = ExponentialSmoothing(
                train.values,
                seasonal_periods=7,
                trend='add',
                seasonal='add',
                initialization_method='estimated'
            )
            fit = model.fit()
            hw_pred = fit.forecast(giorni_forecast)
        except Exception as exc:
            print(f"    Holt-Winters fallito allo split con cutoff {train.index[-1].date()}: {exc}")
            hw_pred = np.repeat(train.iloc[-1], giorni_forecast)

        naive_pred = np.repeat(train.iloc[-1], giorni_forecast)
        ma_window = min(7, len(train))
        ma_pred = np.repeat(train.tail(ma_window).mean(), giorni_forecast)

        metrics_hw = _calcola_metriche_forecast(test.values, hw_pred)
        metrics_naive = _calcola_metriche_forecast(test.values, naive_pred)
        metrics_ma = _calcola_metriche_forecast(test.values, ma_pred)

        risultati.append({
            'cutoff': train.index[-1],
            'periodo_test_inizio': test.index[0],
            'periodo_test_fine': test.index[-1],
            'HW_MAE': metrics_hw['MAE'],
            'HW_RMSE': metrics_hw['RMSE'],
            'HW_MAPE': metrics_hw['MAPE'],
            'Naive_MAE': metrics_naive['MAE'],
            'Naive_RMSE': metrics_naive['RMSE'],
            'Naive_MAPE': metrics_naive['MAPE'],
            'MA7_MAE': metrics_ma['MAE'],
            'MA7_RMSE': metrics_ma['RMSE'],
            'MA7_MAPE': metrics_ma['MAPE'],
        })

    risultati_df = pd.DataFrame(risultati)
    if risultati_df.empty:
        print("  Nessun risultato calcolato")
        return None

    summary = risultati_df.mean(numeric_only=True)
    summary_df = summary.to_frame(name='Media').reset_index().rename(columns={'index': 'Metrica'})

    output_path = Path(output_dir) / 'valutazione_forecast.xlsx'
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        risultati_df.to_excel(writer, sheet_name='Dettaglio', index=False)
        summary_df.to_excel(writer, sheet_name='Sintesi', index=False)

    print("  Valutazione salvata in: valutazione_forecast.xlsx")
    print("  Metriche medie (HW):")
    print(f"    MAE : {summary.get('HW_MAE', np.nan):,.1f}")
    print(f"    RMSE: {summary.get('HW_RMSE', np.nan):,.1f}")
    if not np.isnan(summary.get('HW_MAPE', np.nan)):
        print(f"    MAPE: {summary.get('HW_MAPE', np.nan):.2f}%")
    else:
        print("    MAPE: non disponibile (valori reali nulli)")

    return risultati_df

def genera_forecast(df, output_dir, weeks_ahead=4):
    print(f"\nFORECAST SEMPLICE - {weeks_ahead} settimane")
    weekly = df.groupby('week').agg({'OFFERTO': 'sum'}).reset_index()
    weekly.columns = ['SETTIMANA', 'TOTALE']
    weekly = weekly.sort_values('SETTIMANA')
    if weeks_ahead <= 0 or weekly.empty:
        return pd.DataFrame(columns=['SETTIMANA', 'FORECAST', 'CI_LOWER', 'CI_UPPER'])
    ma_value = weekly['TOTALE'].tail(4).mean()
    ma_value = 0 if not np.isfinite(ma_value) else ma_value
    forecast_final = np.array([ma_value] * weeks_ahead)
    ci_lower = forecast_final * 0.9
    ci_upper = forecast_final * 1.1
    ultima_settimana = weekly['SETTIMANA'].max() if not weekly.empty else 0
    return pd.DataFrame({
        'SETTIMANA': range(int(ultima_settimana) + 1, int(ultima_settimana) + weeks_ahead + 1),
        'FORECAST': forecast_final,
        'CI_LOWER': ci_lower,
        'CI_UPPER': ci_upper
    })


# =============================================================================
# REPORT STATISTICO
# =============================================================================

def genera_report_statistico(df, fascia_stats, giorno_stats, week_stats, mese_stats, week_comp, month_comp, anomalie_alte, anomalie_basse, kpi, output_dir):
    print("\nREPORT STATISTICO")
    print("=" * 60)

    def fmt_percent(value, decimals=1):
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return 'n.d.'
        return f"{value:+.{decimals}f}%"

    periodo_inizio = df['DATA'].min().date()
    periodo_fine = df['DATA'].max().date()
    totale_chiamate = df['OFFERTO'].sum()
    media_slot = df['OFFERTO'].mean()

    ultima_settimana = week_comp['SETTIMANA'].iloc[-1] if len(week_comp) else None
    var_settimana = week_comp['VAR_WEEK'].iloc[-1] if len(week_comp) >= 2 else None

    ultimo_mese_etichetta = month_comp['ETICHETTA'].iloc[-1] if len(month_comp) else None
    var_mese = month_comp['VAR_MONTH'].iloc[-1] if len(month_comp) >= 2 else None

    diff_weekend = kpi.get('diff_weekend')
    giorno_max = kpi.get('giorno_max')
    giorno_min = kpi.get('giorno_min')
    fascia_picco = kpi.get('fascia_picco')
    fascia_picco_val = kpi.get('fascia_picco_valore')

    top_fasce = fascia_stats.nlargest(3, 'MEDIA') if len(fascia_stats) > 0 else pd.DataFrame()
    top_giorni_sett = giorno_stats.nlargest(3, 'TOTALE') if len(giorno_stats) > 0 else pd.DataFrame()

    top_anom_high = anomalie_alte.head(3) if not anomalie_alte.empty else anomalie_alte
    top_anom_low = anomalie_basse.head(3) if not anomalie_basse.empty else anomalie_basse

    report_path = os.path.join(output_dir, 'report_statistico.txt')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('REPORT STATISTICO\n')
        f.write('=' * 80 + '\n')
        f.write(f"Periodo analizzato: {periodo_inizio} - {periodo_fine}\n")
        f.write(f"Totale chiamate: {totale_chiamate:,.0f}\n")
        f.write(f"Media per slot: {media_slot:,.2f}\n\n")

        f.write('Andamento recente\n')
        f.write('-' * 80 + '\n')
        if ultima_settimana is not None:
            f.write(f"Ultima settimana (W{int(ultima_settimana)}): {fmt_percent(var_settimana)}\n")
        else:
            f.write('Ultima settimana: n.d.\n')
        if ultimo_mese_etichetta is not None:
            f.write(f"Ultimo mese ({ultimo_mese_etichetta}): {fmt_percent(var_mese)}\n")
        else:
            f.write('Ultimo mese: n.d.\n')
        if diff_weekend is not None and not (isinstance(diff_weekend, float) and np.isnan(diff_weekend)):
            f.write(f"Weekend vs Weekday: {diff_weekend:+.1f}%\n")
        else:
            f.write('Weekend vs Weekday: n.d.\n')
        if kpi.get('trend') is not None:
            f.write(f"Trend lineare: {kpi['trend']} (coefficiente {kpi['trend_valore']:.2f})\n")
        f.write('\n')

        f.write('Fasce orarie pi\xf9 rilevanti (media per slot)\n')
        f.write('-' * 80 + '\n')
        if not top_fasce.empty:
            for _, row in top_fasce.iterrows():
                f.write(f"  {row['FASCIA']}: {row['MEDIA']:,.1f} chiamate (totale {row['TOTALE']:,.0f})\n")
        else:
            f.write('  n.d.\n')
        f.write('\n')

        f.write('Giorni della settimana pi\xf9 intensi\n')
        f.write('-' * 80 + '\n')
        if not top_giorni_sett.empty:
            for _, row in top_giorni_sett.iterrows():
                f.write(f"  {row['GIORNO']}: {row['TOTALE']:,.0f} chiamate totali (media {row['MEDIA']:,.1f})\n")
        else:
            f.write('  n.d.\n')
        f.write('\n')

        if isinstance(giorno_max, pd.Series):
            f.write('Giorno con maggiore volume: ' + f"{giorno_max['DATA'].date()} ({giorno_max['GG SETT']}): {giorno_max['OFFERTO']:,.0f}\n")
        if isinstance(giorno_min, pd.Series):
            f.write('Giorno con minore volume: ' + f"{giorno_min['DATA'].date()} ({giorno_min['GG SETT']}): {giorno_min['OFFERTO']:,.0f}\n")
        if fascia_picco is not None and fascia_picco_val is not None:
            f.write(f"Fascia di picco: {fascia_picco} ({fascia_picco_val:,.1f} chiamate medie)\n")
        f.write('\n')

        f.write('Anomalie principali\n')
        f.write('-' * 80 + '\n')
        if not top_anom_high.empty:
            f.write('Picchi alti:\n')
            for _, row in top_anom_high.iterrows():
                f.write(f"  {row['DATA'].date()}: {row['TOTALE']:,.0f}\n")
        else:
            f.write('Picchi alti: n.d.\n')
        if not top_anom_low.empty:
            f.write('Picchi bassi:\n')
            for _, row in top_anom_low.iterrows():
                f.write(f"  {row['DATA'].date()}: {row['TOTALE']:,.0f}\n")
        else:
            f.write('Picchi bassi: n.d.\n')

    print('Report salvato: report_statistico.txt')
def crea_dashboard_excel(df, fascia_stats, giorno_stats, week_stats, mese_stats, curve, forecast_df, kpi, output_dir):
    print("\nDASHBOARD EXCEL")
    print("=" * 60)
    
    file_path = Path(output_dir) / 'dashboard_completa.xlsx'
    
    with safe_excel_writer(file_path, engine='xlsxwriter') as (writer, actual_path):
        # KPI
        kpi_df = pd.DataFrame([
            ['Totale Chiamate', f"{kpi['totale_chiamate']:,.0f}"],
            ['Media Giornaliera', f"{kpi['media_giornaliera']:,.0f}"],
            ['Trend', kpi['trend']],
            ['Fascia Picco', kpi['fascia_picco']],
        ], columns=['INDICATORE', 'VALORE'])
        kpi_df.to_excel(writer, sheet_name='KPI', index=False)
        
        # Curve
        curve['intraday_generale'].to_excel(writer, sheet_name='Curva_Intraday', index=False)
        fascia_stats.to_excel(writer, sheet_name='Stats_Fascia', index=False)
        giorno_stats.to_excel(writer, sheet_name='Stats_Giorno', index=False)
        week_stats.to_excel(writer, sheet_name='Stats_Settimana', index=False)
        forecast_df.to_excel(writer, sheet_name='Forecast', index=False)
        
        # Dati raw
        df[['DATA', 'FASCIA', 'GG SETT', 'week', 'OFFERTO']].to_excel(writer, sheet_name='Dati_Raw', index=False)
    
    print(f"Dashboard salvata: {actual_path.name}")
    return file_path

# =============================================================================
# REPORT FINALE
# =============================================================================

def genera_report_finale(df, kpi, forecast_giornaliero_df, output_dir):
    print("\nREPORT FINALE")
    
    with open(f'{output_dir}/report_finale.txt', 'w', encoding='utf-8') as f:
        f.write("REPORT FINALE\n")
        f.write("=" * 80 + "\n")
        f.write(f"Totale chiamate: {kpi['totale_chiamate']:,.0f}\n")
        f.write(f"Trend: {kpi['trend']}\n")
        f.write(f"Fascia picco: {kpi['fascia_picco']}\n")
        f.write("\nFORECAST GIORNALIERO:\n")
        f.write(f"Periodo: {forecast_giornaliero_df['DATA'].min().strftime('%Y-%m-%d')} - {forecast_giornaliero_df['DATA'].max().strftime('%Y-%m-%d')}\n")
        f.write(f"Totale previsto: {forecast_giornaliero_df['FORECAST'].sum():,.0f} chiamate\n")
        f.write(f"Media giornaliera: {forecast_giornaliero_df['FORECAST'].mean():,.1f} chiamate\n")
        f.write("\nTop 5 giorni previsti:\n")
        top5 = forecast_giornaliero_df.nlargest(5, 'FORECAST')[['DATA', 'GG_SETT', 'FORECAST']]
        for _, row in top5.iterrows():
            f.write(f"  {row['DATA'].strftime('%Y-%m-%d')} ({row['GG_SETT']}): {row['FORECAST']:,.0f} chiamate\n")
    
    print("Report finale salvato")


def _genera_report_pdf(df, forecast_modelli, backtest_metrics, kpi, output_dir):
    """
    Genera report esecutivo PDF di 1 pagina con tutte le info chiave.
    Usa matplotlib backend PDF (nessuna dipendenza esterna).

    Args:
        df: DataFrame dati storici
        forecast_modelli: dict risultati forecast (da genera_forecast_modelli)
        backtest_metrics: dict metriche affidabilità
        kpi: dict KPI consuntivi
        output_dir: cartella output
    """
    from matplotlib.backends.backend_pdf import PdfPages

    print("\nGENERAZIONE REPORT PDF ESECUTIVO")
    print("=" * 80)

    pdf_path = Path(output_dir) / 'report_esecutivo_forecast.pdf'

    # Estrai miglior modello
    best_model = None
    best_mape = None
    if backtest_metrics:
        valid_models = {m: v.get('MAPE') for m, v in backtest_metrics.items()
                       if v.get('MAPE') is not None and np.isfinite(v.get('MAPE'))}
        if valid_models:
            best_model = min(valid_models, key=valid_models.get)
            best_mape = valid_models[best_model]

    # Estrai forecast del miglior modello
    confronto_df = forecast_modelli.get('confronto_df')
    forecast_best = None
    if confronto_df is not None and best_model and best_model in confronto_df.columns:
        forecast_best = confronto_df[['DATA', best_model]].copy()
        forecast_best.rename(columns={best_model: 'FORECAST'}, inplace=True)
    elif confronto_df is not None:
        # Fallback: usa prima colonna disponibile
        first_col = [c for c in confronto_df.columns if c != 'DATA'][0]
        forecast_best = confronto_df[['DATA', first_col]].copy()
        forecast_best.rename(columns={first_col: 'FORECAST'}, inplace=True)
        best_model = first_col

    if forecast_best is None:
        print("⚠️ Nessun forecast disponibile per PDF")
        return None

    # Calcola metriche derivate
    totale_forecast = forecast_best['FORECAST'].sum()
    media_daily_forecast = forecast_best['FORECAST'].mean()

    # Confronto con storico
    daily_historical = df.groupby('DATA')['OFFERTO'].sum()
    media_storica = daily_historical.mean()
    variazione_pct = ((media_daily_forecast - media_storica) / media_storica * 100) if media_storica > 0 else 0

    # Identifica picchi (top 5 giorni)
    top_days = forecast_best.nlargest(5, 'FORECAST')

    # Trend (crescita/stabile/decrescita)
    trend_coeff = np.polyfit(range(len(forecast_best)), forecast_best['FORECAST'].values, 1)[0]
    if trend_coeff > media_daily_forecast * 0.01:  # >1% crescita
        trend = "CRESCITA"
        trend_icon = "↗"
    elif trend_coeff < -media_daily_forecast * 0.01:
        trend = "DECRESCITA"
        trend_icon = "↘"
    else:
        trend = "STABILE"
        trend_icon = "→"

    # Crea PDF multi-pagina
    with PdfPages(pdf_path) as pdf:
        # PAGINA 1: Executive Summary
        fig = plt.figure(figsize=(8.27, 11.69))  # A4
        fig.suptitle('REPORT ESECUTIVO FORECAST', fontsize=20, fontweight='bold', y=0.96)

        # Sezione 1: KPI Principali (griglia 3x2)
        ax1 = plt.subplot2grid((6, 2), (0, 0), colspan=2, rowspan=1)
        ax1.axis('off')

        affidabilita_text = f"{best_mape:.1f}%" if best_mape is not None else "N/D"
        if best_mape is not None:
            if best_mape < 5:
                affidabilita_label = "✅ ECCELLENTE"
            elif best_mape < 10:
                affidabilita_label = "⚠️ BUONA"
            else:
                affidabilita_label = "🔴 BASSA"
        else:
            affidabilita_label = ""

        summary_text = f"""
PERIODO FORECAST: {forecast_best['DATA'].min().strftime('%d/%m/%Y')} - {forecast_best['DATA'].max().strftime('%d/%m/%Y')} ({len(forecast_best)} giorni)
MODELLO UTILIZZATO: {best_model.upper() if best_model else 'N/D'}
AFFIDABILITÀ (MAPE): {affidabilita_text} {affidabilita_label}
        """
        ax1.text(0.05, 0.5, summary_text.strip(), fontsize=11, verticalalignment='center',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))

        # KPI Box grande
        ax2 = plt.subplot2grid((6, 2), (1, 0), colspan=1, rowspan=1)
        ax2.axis('off')
        kpi_box = f"""
╔══════════════════════════════════╗
║  TOTALE CHIAMATE PREVISTE        ║
║  {totale_forecast:,.0f}                       ║
╚══════════════════════════════════╝
        """
        ax2.text(0.5, 0.5, kpi_box.strip(), fontsize=14, fontweight='bold',
                ha='center', va='center', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='#C6F6D5', alpha=0.8))

        ax3 = plt.subplot2grid((6, 2), (1, 1), colspan=1, rowspan=1)
        ax3.axis('off')
        variazione_color = '#C6F6D5' if variazione_pct > 0 else '#FED7D7'
        var_text = f"""
╔══════════════════════════════════╗
║  VARIAZIONE vs STORICO {trend_icon}       ║
║  {variazione_pct:+.1f}%                       ║
╚══════════════════════════════════╝
        """
        ax3.text(0.5, 0.5, var_text.strip(), fontsize=14, fontweight='bold',
                ha='center', va='center', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor=variazione_color, alpha=0.8))

        # Sezione 2: Grafico Forecast
        ax4 = plt.subplot2grid((6, 2), (2, 0), colspan=2, rowspan=2)
        ax4.plot(forecast_best['DATA'], forecast_best['FORECAST'],
                linewidth=2.5, color='#2E86AB', marker='o', markersize=3)
        ax4.fill_between(forecast_best['DATA'], 0, forecast_best['FORECAST'], alpha=0.2, color='#2E86AB')
        ax4.set_title('Forecast Giornaliero', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Chiamate Previste')
        ax4.grid(True, alpha=0.3)
        ax4.tick_params(axis='x', rotation=45)

        # Sezione 3: Top 5 Picchi
        ax5 = plt.subplot2grid((6, 2), (4, 0), colspan=1, rowspan=2)
        ax5.axis('off')
        picchi_text = "TOP 5 GIORNI DI PICCO:\n" + "-" * 35 + "\n"
        for idx, row in top_days.iterrows():
            picchi_text += f"{row['DATA'].strftime('%d/%m/%Y')}: {row['FORECAST']:>8,.0f}\n"
        ax5.text(0.05, 0.95, picchi_text, fontsize=10, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='#FEFCBF', alpha=0.5))

        # Sezione 4: Metriche modelli
        ax6 = plt.subplot2grid((6, 2), (4, 1), colspan=1, rowspan=2)
        ax6.axis('off')

        if backtest_metrics:
            metrics_text = "AFFIDABILITÀ MODELLI:\n" + "-" * 35 + "\n"
            sorted_models = sorted(backtest_metrics.items(), key=lambda x: x[1].get('MAPE', 999))[:5]
            for model, vals in sorted_models:
                mape = vals.get('MAPE')
                if mape is not None:
                    metrics_text += f"{model[:12]:12s}: {mape:5.1f}% MAPE\n"
                else:
                    metrics_text += f"{model[:12]:12s}:   N/D  MAPE\n"
        else:
            metrics_text = "Metriche non disponibili"

        ax6.text(0.05, 0.95, metrics_text, fontsize=10, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='#E9D8FD', alpha=0.5))

        # Footer
        fig.text(0.5, 0.02, f'Generato il {datetime.now().strftime("%d/%m/%Y %H:%M")} | Script v{SCRIPT_VERSION}',
                ha='center', fontsize=8, style='italic', color='gray')

        plt.tight_layout(rect=[0, 0.03, 1, 0.94])
        pdf.savefig(fig, dpi=150)
        plt.close()

        # PAGINA 2: Confronto Modelli (se disponibili)
        if confronto_df is not None and len([c for c in confronto_df.columns if c != 'DATA']) > 1:
            fig2, ax = plt.subplots(figsize=(8.27, 11.69))

            model_cols = [c for c in confronto_df.columns if c != 'DATA']
            colors = ['#2E86AB', '#A23B72', '#F18F01', '#6A994E', '#BC4B51', '#8B5CF6', '#C73E1D']

            for i, model in enumerate(model_cols[:7]):
                ax.plot(confronto_df['DATA'], confronto_df[model],
                       label=model.upper(), linewidth=2, alpha=0.8,
                       color=colors[i % len(colors)])

            ax.set_title('Confronto Forecast tra Modelli', fontsize=14, fontweight='bold')
            ax.set_xlabel('Data')
            ax.set_ylabel('Chiamate Previste')
            ax.legend(loc='best')
            ax.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            pdf.savefig(fig2, dpi=150)
            plt.close()

        # PAGINA 3: Breakdown settimanale
        fig3, (ax1, ax2) = plt.subplots(2, 1, figsize=(8.27, 11.69))

        # Aggregazione settimanale
        forecast_weekly = forecast_best.copy()
        forecast_weekly['DATA'] = pd.to_datetime(forecast_weekly['DATA'])
        forecast_weekly = forecast_weekly.set_index('DATA')
        weekly = forecast_weekly.resample('W-MON').sum().reset_index()
        weekly['SETTIMANA'] = weekly['DATA'].dt.strftime('Sett. %d/%m')

        ax1.bar(range(len(weekly)), weekly['FORECAST'], color='#2E86AB', alpha=0.7)
        ax1.set_title('Breakdown Settimanale', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Chiamate Totali')
        ax1.set_xticks(range(len(weekly)))
        ax1.set_xticklabels(weekly['SETTIMANA'], rotation=45, ha='right')
        ax1.grid(True, alpha=0.3, axis='y')

        # Distribuzione giorni settimana (da storico)
        if 'GG SETT' in df.columns:
            gg_dist = df.groupby('GG SETT')['OFFERTO'].mean().reindex(['lun', 'mar', 'mer', 'gio', 'ven', 'sab', 'dom'])
            ax2.bar(gg_dist.index, gg_dist.values, color='#A23B72', alpha=0.7)
            ax2.set_title('Pattern Settimanale (Media Storica)', fontsize=12, fontweight='bold')
            ax2.set_ylabel('Chiamate Medie')
            ax2.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()
        pdf.savefig(fig3, dpi=150)
        plt.close()

    print(f"✅ Report PDF salvato: {pdf_path.name}")
    return pdf_path


# =============================================================================
# PROCESSING SINGOLO FILE
# =============================================================================

def processa_singolo_file(file_path, output_dir, giorni_forecast=28, escludi_festivita=None, metodi=None):
    """
    Elabora un singolo file Excel e genera tutti gli output.

    Args:
        file_path: path completo del file Excel
        output_dir: cartella output per questo file
        giorni_forecast: numero di giorni da prevedere nel forecast
        escludi_festivita: lista festività da escludere da Prophet

    Returns:
        dict con risultati chiave (df, kpi, forecast, ecc.)
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        t_file = time.time()

        print("\n" + "=" * 80)
        print("ANALISI COMPLETA TRAFFICO CALL CENTER")
        print("=" * 80)
        print(f"Giorni forecast: {giorni_forecast}")
        print()

        print("\n[1/16] Caricamento dati...")
        t_step = time.time()
        df = carica_dati(file_path)
        _log_step_time("Caricamento dati", t_step)

        print("\n[2/16] Analisi fascia oraria...")
        t_step = time.time()
        fascia_stats = analisi_fascia_oraria(df, output_dir)
        _log_step_time("Analisi fascia oraria", t_step)

        print("\n[3/16] Analisi giorno settimana...")
        t_step = time.time()
        giorno_stats = analisi_giorno_settimana(df, output_dir)
        _log_step_time("Analisi giorno settimana", t_step)

        print("\n[4/16] Analisi settimana...")
        t_step = time.time()
        week_stats = analisi_settimana(df, output_dir)
        _log_step_time("Analisi settimana", t_step)

        print("\n[5/16] Analisi mese...")
        t_step = time.time()
        mese_stats = analisi_mese(df, output_dir)
        _log_step_time("Analisi mese", t_step)

        print("\n[6/16] Heatmap...")
        t_step = time.time()
        crea_heatmap(df, output_dir)
        _log_step_time("Heatmap", t_step)

        print("\n[7/16] Curve previsionali...")
        t_step = time.time()
        curve = genera_curve_previsionali(df, output_dir)
        _log_step_time("Curve previsionali", t_step)

        print("\n[8/16] Trend storico...")
        t_step = time.time()
        daily_trend = analisi_consuntiva_trend(df, output_dir)
        _log_step_time("Trend storico", t_step)

        print("\n[9/16] Confronto periodi...")
        t_step = time.time()
        week_comp, month_comp = analisi_confronto_periodi(df, output_dir)
        _log_step_time("Confronto periodi", t_step)

        print("\n[10/16] Anomalie...")
        t_step = time.time()
        anomalie_alte, anomalie_basse = identifica_anomalie(df, output_dir)
        _log_step_time("Anomalie", t_step)

        print("\n[11/16] KPI...")
        t_step = time.time()
        kpi = dashboard_kpi_consuntivi(df, output_dir)
        _log_step_time("KPI", t_step)

        print("\n[12/16] Valutazione forecast (backtest Holt-Winters)...")
        t_step = time.time()
        valutazione = valuta_modelli_forecast(df, output_dir, giorni_forecast=giorni_forecast, fast_mode=FAST_MODE)
        _log_step_time("Valutazione forecast", t_step)

        print("\n[13/16] Forecast multi-modello...")
        t_step = time.time()
        forecast_modelli = genera_forecast_modelli(
            df,
            output_dir,
            giorni_forecast=giorni_forecast,
            metodi=metodi,
            escludi_festivita=escludi_festivita,
            fast_mode=FAST_MODE
        )
        _log_step_time("Forecast multi-modello", t_step)
        forecast_completo = forecast_modelli.get('holtwinters')
        if forecast_completo is None:
            # usa il primo disponibile come fallback per i passi successivi
            forecast_completo = next(iter(forecast_modelli.values()))

        print("\n[14/16] Report statistico...")
        t_step = time.time()
        genera_report_statistico(df, fascia_stats, giorno_stats, week_stats, mese_stats, week_comp, month_comp, anomalie_alte, anomalie_basse, kpi, output_dir)
        _log_step_time("Report statistico", t_step)

        print("\n[15/16] Dashboard Excel...")
        t_step = time.time()
        excel_path = crea_dashboard_excel(df, fascia_stats, giorno_stats, week_stats, mese_stats, curve, forecast_completo['giornaliero'], kpi, output_dir)
        _log_step_time("Dashboard Excel", t_step)

        print("\n[16/19] Report finale...")
        t_step = time.time()
        genera_report_finale(df, kpi, forecast_completo['giornaliero'], output_dir)
        _log_step_time("Report finale", t_step)

        # [17/19] Alert automatici
        print("\n[17/19] Alert automatici...")
        t_step = time.time()
        forecast_giornaliero = forecast_completo.get('giornaliero') if forecast_completo else None
        alerts = _rileva_alert(df, forecast_giornaliero, forecast_modelli.get('backtest'), output_dir) if forecast_giornaliero is not None else []
        _log_step_time("Alert automatici", t_step)

        # [18/19] Report PDF esecutivo
        print("\n[18/19] Report PDF esecutivo...")
        t_step = time.time()
        pdf_path = _genera_report_pdf(df, forecast_modelli, forecast_modelli.get('backtest'), kpi, output_dir)
        _log_step_time("Report PDF esecutivo", t_step)

        # [19/19] Pipeline completata
        print("\n[19/19] Pipeline completata (confronto consuntivo disponibile su richiesta GUI)")

        print("\n" + "=" * 80)
        print("✅ FILE COMPLETATO!")
        print("=" * 80)
        print(f"File salvati in: {output_dir}")
        _log_step_time("Elaborazione file", t_file)

        return {
            'file_path': file_path,
            'output_dir': output_dir,
            'df': df,
            'kpi': kpi,
            'forecast': forecast_completo,
            'valutazione': valutazione,
            'forecast_modelli': forecast_modelli,
            'alerts': alerts,          # NUOVO
            'pdf_path': pdf_path,      # NUOVO
            'success': True
        }

    except Exception as e:
        print(f"\n❌ ERRORE durante elaborazione di {os.path.basename(file_path)}:")
        print(str(e))
        import traceback
        traceback.print_exc()
        return {
            'file_path': file_path,
            'success': False,
            'error': str(e)
        }


# =============================================================================
# MAIN - BATCH PROCESSING
# =============================================================================

def main(giorni_forecast=28, escludi_festivita=None, input_dirs=None, metodi=None):
    """
    Elabora tutti i file Excel trovati nella cartella dello script.
    Per ogni file crea una cartella output separata.

    Args:
        giorni_forecast: numero di giorni da prevedere nel forecast (default 28)
        escludi_festivita: lista festività da escludere da Prophet (default None)
        input_dirs: percorsi personalizzati per cercare i file Excel (default None)

    Returns:
        list di dict con risultati per ogni file
    """
    print("\n" + "=" * 80)
    print("🚀 AVVIO BATCH PROCESSING - ANALISI MULTIPLA")
    print("=" * 80)

    # Trova tutti i file Excel
    file_excel_list = trova_file_excel(custom_dirs=input_dirs)

    if len(file_excel_list) == 0:
        print("❌ Nessun file Excel trovato!")
        return []

    print(f"Trovati {len(file_excel_list)} file da elaborare.")
    
    # Determina numero di worker (max CPU - 1 per lasciare respiro al sistema, min 1)
    import multiprocessing
    max_workers = max(1, multiprocessing.cpu_count() - 1)
    
    # Se abbiamo pochi file, non serve sprecare risorse in troppi processi
    max_workers = min(max_workers, len(file_excel_list))
    
    print(f"Avvio elaborazione parallela con {max_workers} processi...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    risultati = []

    # OTTIMIZZAZIONE: Se c'è solo un file, evita l'overhead del multiprocessing
    if len(file_excel_list) == 1:
        print("Elaborazione sequenziale (singolo file)...")
        file_path = file_excel_list[0]
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        output_dir = os.path.join(script_dir, "output", file_name)
        res = processa_singolo_file(
            file_path, output_dir, giorni_forecast, escludi_festivita, metodi
        )
        risultati.append(res)
    else:
        print(f"Avvio elaborazione parallela con {max_workers} processi...")
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for file_path in file_excel_list:
                file_name = os.path.splitext(os.path.basename(file_path))[0]
                output_dir = os.path.join(script_dir, "output", file_name)
                
                future = executor.submit(
                    processa_singolo_file,
                    file_path, 
                    output_dir, 
                    giorni_forecast, 
                    escludi_festivita, 
                    metodi
                )
                futures[future] = file_path

            for future in as_completed(futures):
                path = futures[future]
                try:
                    res = future.result()
                    risultati.append(res)
                except Exception as exc:
                    print(f"Errore generico nel worker per {path}: {exc}")
                    risultati.append({'file_path': path, 'success': False, 'error': str(exc)})

    # Ordina risultati per nome file per coerenza
    risultati.sort(key=lambda x: os.path.basename(x.get('file_path', '')))
    
    # Genera report riassuntivo finale

    # Genera report riassuntivo finale
    print(f"\n{'='*80}")
    print("📊 GENERAZIONE REPORT RIASSUNTIVO")
    print(f"{'='*80}")
    genera_report_riassuntivo(risultati, script_dir)

    # Stampa riepilogo finale
    print(f"\n{'='*80}")
    print("🎉 BATCH PROCESSING COMPLETATO!")
    print(f"{'='*80}")
    print(f"\nFile processati: {len(file_excel_list)}")
    success_count = sum(1 for r in risultati if r.get('success', False))
    error_count = len(file_excel_list) - success_count
    print(f"✅ Successi: {success_count}")
    if error_count > 0:
        print(f"❌ Errori: {error_count}")

    print(f"\n📂 Output salvati in:")
    print(f"   {os.path.join(script_dir, 'output')}/")
    for r in risultati:
        if r.get('success'):
            folder_name = os.path.basename(r['output_dir'])
            print(f"   ├── {folder_name}/")
    print(f"   └── _report_riassuntivo.xlsx")
    print(f"{'='*80}\n")

    return risultati


def genera_report_riassuntivo(risultati, script_dir):
    """
    Genera un report Excel riassuntivo con i KPI di tutti i file processati.

    Args:
        risultati: lista di dict con risultati per ogni file
        script_dir: directory dello script
    """
    try:
        report_data = []

        for r in risultati:
            if r.get('success'):
                kpi = r.get('kpi', {})
                forecast = r.get('forecast', {})

                file_name = os.path.basename(r['file_path'])
                giornaliero = forecast.get('giornaliero', pd.DataFrame())

                report_data.append({
                    'File': file_name,
                    'Status': '✅ OK',
                    'Totale Chiamate Storiche': kpi.get('totale_chiamate', 0),
                    'Media Giornaliera': kpi.get('media_giornaliera', 0),
                    'Trend': kpi.get('trend', 'N/D'),
                    'Fascia Picco': kpi.get('fascia_picco', 'N/D'),
                    'MAPE (%)': kpi.get('cv', 0),
                    'Forecast Totale (90gg)': giornaliero['FORECAST'].sum() if not giornaliero.empty else 0,
                    'Forecast Media/Giorno': giornaliero['FORECAST'].mean() if not giornaliero.empty else 0,
                })
            else:
                report_data.append({
                    'File': os.path.basename(r['file_path']),
                    'Status': '❌ ERRORE',
                    'Errore': r.get('error', 'Sconosciuto')
                })

        df_report = pd.DataFrame(report_data)

        output_path = os.path.join(script_dir, 'output', '_report_riassuntivo.xlsx')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with safe_excel_writer(output_path, engine='xlsxwriter') as (writer, actual_path):
            df_report.to_excel(writer, sheet_name='Riepilogo', index=False)

            # Aggiungi foglio con dettagli timestamp
            info_df = pd.DataFrame([
                ['Data Elaborazione', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ['File Processati', len(risultati)],
                ['Successi', sum(1 for r in risultati if r.get('success'))],
                ['Errori', sum(1 for r in risultati if not r.get('success'))]
            ], columns=['Parametro', 'Valore'])
            info_df.to_excel(writer, sheet_name='Info', index=False)

        print(f"✅ Report riassuntivo salvato: {actual_path.name}")

    except Exception as e:
        print(f"⚠️  Errore generazione report riassuntivo: {e}")


class _GuiLogWriter:
    """Scrive i log sia in buffer sia in una coda per aggiornamenti live GUI."""

    def __init__(self, queue_obj, buffer=None, mirror_stream=None):
        self.queue = queue_obj
        self.buffer = buffer
        self.mirror_stream = mirror_stream  # opzionale: duplica su stdout/stderr originale

    def write(self, msg):
        if not msg:
            return
        if self.buffer is not None:
            self.buffer.write(msg)
        if self.mirror_stream is not None:
            try:
                self.mirror_stream.write(msg)
            except Exception:
                pass
        try:
            self.queue.put_nowait(msg)
        except Exception:
            # Se la coda è chiusa o piena, ignoriamo per non bloccare l'esecuzione.
            pass

    def flush(self):
        if self.buffer is not None:
            self.buffer.flush()
        if self.mirror_stream is not None:
            try:
                self.mirror_stream.flush()
            except Exception:
                pass


class _QueueLogHandler(logging.Handler):
    """Handler logging che inoltra i messaggi alla coda della GUI."""

    def __init__(self, queue_obj):
        super().__init__()
        self.queue = queue_obj

    def emit(self, record):
        try:
            msg = self.format(record)
            if not msg.endswith("\n"):
                msg += "\n"
            self.queue.put_nowait(msg)
        except Exception:
            # Non bloccare l'esecuzione della GUI in caso di problemi con la coda
            pass


class ForecastGUI:
    """Interfaccia grafica minimale per lanciare il forecast e visualizzare output."""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Forecast Call Center - GUI")
        self.root.geometry("960x700")
        self.root.configure(bg="#f7f7f7")

        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.input_dir_var = tk.StringVar(value=script_dir)
        self.forecast_days_var = tk.StringVar(value="90")
        self.holidays_var = tk.StringVar(value="")
        self.best_model_var = tk.StringVar(value="N/D")
        self.fast_mode_var = tk.BooleanVar(value=FAST_MODE)

        self.model_vars = {m: tk.BooleanVar(value=True) for m in (
            'holtwinters', 'pattern', 'naive', 'sarima', 'prophet', 'tbats', 'intraday_dinamico', 'ensemble_hybrid'
        )}
        self.holiday_flags_vars = {h: tk.BooleanVar(value=False) for h in HOLIDAY_FLAGS}
        self.confronto_df = None
        self.backtest_metrics = None
        self.log_queue = queue.Queue()
        self._log_polling = False
        self._log_polling_forced_stop = False
        self._last_log_ts = None
        self._last_heartbeat_ts = None
        self._worker_thread = None
        self._buffer_sync_active = False
        self._buffer_sync_ref = None
        self._buffer_sync_len = 0

        self.run_button = None
        self.results_box = None
        self.log_widget = None
        self.plot_combo = None
        self.plot_var = tk.StringVar()
        self.image_label = None
        self.image_caption = None
        self.current_image = None
        self.last_output_dirs = []
        self.compare_tree = None
        self.metric_tree = None
        self.metric_horizon_tree = None
        self.best_detail_var = tk.StringVar(value="N/D")
        self.output_tree = None
        self.output_files = []
        self.period_var = tk.StringVar(value='giorno')

        # Nuove variabili per funzionalità avanzate
        self.consuntivo_path_var = tk.StringVar(value="")
        self.confronto_consuntivo_df = None
        self.alerts_list = []
        self.kpi_forecast = None
        self.kpi_widgets = {}
        self.alert_tree = None
        self.consuntivo_tree = None
        self.consuntivo_status = None
        self.plot_type_var = tk.StringVar(value="forecast_comparison")
        self.interactive_fig = None
        self.interactive_ax = None
        self.interactive_canvas = None
        self.interactive_toolbar = None
        self.dashboard_preview_label = None

        # Zoom functionality
        self.zoom_level = 0  # -5 to +5 range
        self.base_font_sizes = {}  # Store original font sizes

        self._build_layout()
        # Avvia subito il polling della coda log per evitare corse tra thread
        # e consentire di mostrare qualsiasi messaggio già emesso sul prompt.
        self._ensure_log_polling()

    def _build_layout(self):
        header = ttk.Label(self.root, text=f"Forecast WFM - versione {SCRIPT_VERSION}", font=("Helvetica", 16, "bold"))
        header.pack(pady=8)

        form_frame = ttk.Frame(self.root, padding=10)
        form_frame.pack(fill="x")

        ttk.Label(form_frame, text="Cartella input Excel:").grid(row=0, column=0, sticky="w")
        input_entry = ttk.Entry(form_frame, textvariable=self.input_dir_var, width=80)
        input_entry.grid(row=0, column=1, padx=5, pady=2, sticky="we")
        ttk.Button(form_frame, text="Sfoglia", command=self.browse_input).grid(row=0, column=2, padx=5)

        ttk.Label(form_frame, text="Giorni forecast:").grid(row=1, column=0, sticky="w")
        ttk.Entry(form_frame, textvariable=self.forecast_days_var, width=10).grid(row=1, column=1, sticky="w", pady=2)

        ttk.Label(form_frame, text="Festività da escludere (virgola):").grid(row=2, column=0, sticky="w")
        ttk.Entry(form_frame, textvariable=self.holidays_var, width=60).grid(row=2, column=1, columnspan=2, sticky="we", pady=2)

        flags_frame = ttk.Frame(form_frame)
        flags_frame.grid(row=3, column=0, columnspan=3, sticky="we", pady=2)
        ttk.Label(flags_frame, text="Flag rapidi festività da escludere:").grid(row=0, column=0, sticky="w")
        for idx, holiday in enumerate(HOLIDAY_FLAGS):
            r = idx // 4 + 1
            c = idx % 4
            ttk.Checkbutton(flags_frame, text=holiday, variable=self.holiday_flags_vars[holiday]).grid(row=r, column=c, sticky="w")

        models_frame = ttk.Frame(form_frame)
        models_frame.grid(row=4, column=0, columnspan=3, sticky="we", pady=4)
        ttk.Label(models_frame, text="Seleziona i modelli da eseguire:").grid(row=0, column=0, sticky="w")
        for idx, modello in enumerate(self.model_vars.keys()):
            r = idx // 4 + 1
            c = idx % 4
            ttk.Checkbutton(models_frame, text=modello, variable=self.model_vars[modello]).grid(row=r, column=c, sticky="w")

        ttk.Checkbutton(
            form_frame,
            text="Modalita veloce (--fast)",
            variable=self.fast_mode_var
        ).grid(row=5, column=0, sticky="w")

        self.run_button = ttk.Button(form_frame, text="Esegui forecast", command=self.run_analysis)
        self.run_button.grid(row=5, column=0, pady=8, sticky="e")

        ttk.Label(form_frame, text="Miglior modello rilevato:").grid(row=5, column=1, sticky="e")
        ttk.Label(form_frame, textvariable=self.best_model_var, font=("Helvetica", 10, "bold"), foreground="#2c7a7b").grid(row=5, column=2, sticky="w")

        form_frame.columnconfigure(1, weight=1)

        results_frame = ttk.LabelFrame(self.root, text="Risultati ultimi run", padding=10)
        results_frame.pack(fill="both", expand=False, padx=10, pady=5)
        self.results_box = tk.Listbox(results_frame, height=6)
        self.results_box.pack(fill="x", expand=True)

        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True, padx=10, pady=5)

        # Nuovo ordine tab con nuove funzionalità
        tab_dashboard = ttk.Frame(notebook)
        tab_model_selector = ttk.Frame(notebook)  # NUOVO TAB
        tab_alert = ttk.Frame(notebook)
        tab_plots_interactive = ttk.Frame(notebook)
        tab_plots = ttk.Frame(notebook)
        tab_compare = ttk.Frame(notebook)
        tab_consuntivo = ttk.Frame(notebook)
        tab_output = ttk.Frame(notebook)
        tab_log = ttk.Frame(notebook)
        tab_guide = ttk.Frame(notebook)

        notebook.add(tab_dashboard, text="📊 Dashboard KPI")
        notebook.add(tab_model_selector, text="🎯 Quale Modello Usare?")  # NUOVO TAB
        notebook.add(tab_alert, text="⚠️ Alert")
        notebook.add(tab_plots_interactive, text="📈 Grafici Interattivi")
        notebook.add(tab_plots, text="🖼️ Grafici PNG")
        notebook.add(tab_compare, text="⚖️ Confronti & Affidabilità")
        notebook.add(tab_consuntivo, text="✅ Forecast vs Consuntivo")
        notebook.add(tab_output, text="📁 File & Metriche")
        notebook.add(tab_log, text="📝 Log live")
        notebook.add(tab_guide, text="📚 Guida modelli")

        # ========== TAB DASHBOARD KPI ==========
        dashboard_canvas, dashboard_scrollable = self._create_scrollable_frame(tab_dashboard)

        ttk.Label(dashboard_scrollable, text="Dashboard KPI Forecast", font=("Helvetica", 16, "bold")).pack(pady=10)

        kpi_frame = ttk.Frame(dashboard_scrollable, padding=10)
        kpi_frame.pack(fill="both", expand=True)

        # Griglia 2x3 per KPI cards
        self.kpi_widgets['totale'] = self._create_kpi_card(kpi_frame, "Totale Forecast", "0", "#2E86AB", 0, 0)
        self.kpi_widgets['variazione'] = self._create_kpi_card(kpi_frame, "Variazione vs Storico", "N/D", "#A23B72", 0, 1)
        self.kpi_widgets['picchi'] = self._create_kpi_card(kpi_frame, "Giorni di Picco", "0", "#F18F01", 0, 2)
        self.kpi_widgets['affidabilita'] = self._create_kpi_card(kpi_frame, "Affidabilità (MAPE)", "N/D", "#6A994E", 1, 0)
        self.kpi_widgets['trend'] = self._create_kpi_card(kpi_frame, "Trend", "N/D", "#8B5CF6", 1, 1)
        self.kpi_widgets['modello'] = self._create_kpi_card(kpi_frame, "Miglior Modello", "N/D", "#C73E1D", 1, 2)

        # ========== TAB QUALE MODELLO USARE ==========
        ttk.Label(tab_model_selector, text="🎯 Quale Modello di Forecast Usare?",
                 font=("Helvetica", 16, "bold")).pack(pady=10)

        # Container principale con scrollbar
        selector_canvas = tk.Canvas(tab_model_selector, highlightthickness=0)
        selector_scrollbar = ttk.Scrollbar(tab_model_selector, orient="vertical", command=selector_canvas.yview)
        selector_scrollable = ttk.Frame(selector_canvas)

        # Aggiorna scrollregion quando il contenuto cambia
        def on_frame_configure(event):
            selector_canvas.configure(scrollregion=selector_canvas.bbox("all"))

        selector_scrollable.bind("<Configure>", on_frame_configure)

        # Crea finestra nel canvas
        canvas_window = selector_canvas.create_window((0, 0), window=selector_scrollable, anchor="nw")

        # Adatta larghezza frame alla larghezza canvas
        def on_canvas_configure(event):
            canvas_width = event.width
            selector_canvas.itemconfig(canvas_window, width=canvas_width)

        selector_canvas.bind("<Configure>", on_canvas_configure)
        selector_canvas.configure(yscrollcommand=selector_scrollbar.set)

        selector_canvas.pack(side="left", fill="both", expand=True)
        selector_scrollbar.pack(side="right", fill="y")

        # Abilita scroll con mouse wheel
        def on_mousewheel(event):
            selector_canvas.yview_scroll(int(-1*(event.delta/120)), "units")

        selector_canvas.bind_all("<MouseWheel>", on_mousewheel)

        # SEZIONE 1: RACCOMANDAZIONE AUTOMATICA
        raccomandazione_frame = ttk.LabelFrame(selector_scrollable, text="✅ RACCOMANDAZIONE AUTOMATICA", padding=15)
        raccomandazione_frame.pack(fill="x", padx=10, pady=10)

        self.recommendation_text = tk.Text(raccomandazione_frame, height=8, width=100, wrap="word",
                                          font=("Courier", 10), bg="#E8F5E9", relief="solid", borderwidth=1)
        self.recommendation_text.pack(fill="both", expand=True)
        self.recommendation_text.insert("1.0", "⏳ Esegui un forecast per vedere la raccomandazione automatica...")
        self.recommendation_text.config(state="disabled")

        # SEZIONE 2: TABELLA COMPARATIVA SEMPLIFICATA
        comparativa_frame = ttk.LabelFrame(selector_scrollable, text="📊 COMPARAZIONE RAPIDA MODELLI", padding=15)
        comparativa_frame.pack(fill="x", padx=10, pady=10)

        # Crea Treeview per tabella
        columns = ("Modello", "Rating", "Velocità", "Accuratezza", "Quando Usarlo")
        self.model_comparison_tree = ttk.Treeview(comparativa_frame, columns=columns, show="headings", height=8)

        self.model_comparison_tree.heading("Modello", text="Modello")
        self.model_comparison_tree.heading("Rating", text="Rating")
        self.model_comparison_tree.heading("Velocità", text="Velocità")
        self.model_comparison_tree.heading("Accuratezza", text="Accuratezza")
        self.model_comparison_tree.heading("Quando Usarlo", text="Quando Usarlo")

        self.model_comparison_tree.column("Modello", width=120)
        self.model_comparison_tree.column("Rating", width=80)
        self.model_comparison_tree.column("Velocità", width=100)
        self.model_comparison_tree.column("Accuratezza", width=100)
        self.model_comparison_tree.column("Quando Usarlo", width=350)

        self.model_comparison_tree.pack(fill="both", expand=True)

        # Scrollbar per treeview
        comp_scrollbar = ttk.Scrollbar(comparativa_frame, orient="vertical", command=self.model_comparison_tree.yview)
        self.model_comparison_tree.configure(yscrollcommand=comp_scrollbar.set)
        comp_scrollbar.pack(side="right", fill="y")

        # SEZIONE 3: CASI D'USO PRATICI
        casiduso_frame = ttk.LabelFrame(selector_scrollable, text="💡 GUIDA PRATICA - Quando Usare Ogni Modello", padding=15)
        casiduso_frame.pack(fill="x", padx=10, pady=10)

        casiduso_text = tk.Text(casiduso_frame, height=20, width=100, wrap="word",
                               font=("Courier", 9), relief="solid", borderwidth=1)
        casiduso_text.pack(fill="both", expand=True)

        guida_pratica = """
🏆 PROPHET - Il Più Completo
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ USA QUANDO:
   • Hai dati con TREND marcato (crescita o decrescita)
   • Ci sono FESTIVITÀ che impattano il volume (Natale, Ferragosto, ecc.)
   • Vuoi la MASSIMA PRECISIONE possibile
   • Il tempo di calcolo non è un problema (3-5 minuti)

❌ NON USARE QUANDO:
   • Hai pochi dati storici (< 3 mesi)
   • Serve velocità immediata

📊 CARATTERISTICHE:
   • Gestisce automaticamente festività italiane
   • Cattura trend crescenti/decrescenti
   • Include effetti weekend
   • Ottimo per pianificazione strategica


⚡ HOLT-WINTERS - Il Più Veloce e Affidabile
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ USA QUANDO:
   • Serve VELOCITÀ (pochi secondi)
   • Dati con pattern settimanale regolare
   • Vuoi un buon compromesso velocità/accuratezza
   • Forecast giornaliero o settimanale

❌ NON USARE QUANDO:
   • Ci sono festività importanti da gestire
   • Il trend cambia frequentemente

📊 CARATTERISTICHE:
   • Velocissimo (< 10 secondi)
   • Buona accuratezza su dati stabili
   • Ideale per forecast rapidi quotidiani
   • Cattura stagionalità settimanale


🔮 TBATS - Il Più Sofisticato
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ USA QUANDO:
   • Hai MULTIPLE STAGIONALITÀ (settimanale + mensile)
   • Dati complessi con pattern sovrapposti
   • Vuoi esplorare pattern nascosti
   • Il tempo non è un limite (5-10 minuti)

❌ NON USARE QUANDO:
   • Hai fretta (molto lento)
   • Dati semplici con pattern regolari

📊 CARATTERISTICHE:
   • Cattura stagionalità multiple automaticamente
   • Ottimo per analisi approfondite
   • Robusto a outlier
   • Richiede più dati storici (almeno 6 mesi)


📈 PATTERN - La Baseline Semplice
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ USA QUANDO:
   • Vuoi un forecast "di sicurezza" semplice
   • Come BASELINE per confronto
   • Dati molto stabili senza cambiamenti

❌ NON USARE QUANDO:
   • Ci sono trend o cambiamenti
   • Serve precisione elevata

📊 CARATTERISTICHE:
   • Replica la media storica per giorno settimana
   • Velocissimo
   • Utile come confronto, non come previsione principale


🎯 INTRADAY DINAMICO - Per Distribuzione Oraria
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ USA QUANDO:
   • Serve DISTRIBUZIONE PER FASCIA ORARIA (staffing)
   • Pianificazione turni operatori
   • Hai dati intraday dettagliati

❌ NON USARE QUANDO:
   • Ti serve solo volume giornaliero totale
   • Non hai dati per fascia oraria

📊 CARATTERISTICHE:
   • 24 modelli separati (uno per ogni ora)
   • Ideale per workforce management
   • Include pattern giorno settimana per fascia


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 CONSIGLIO GENERALE:
   1. Usa PROPHET per pianificazione strategica (mensile/trimestrale)
   2. Usa HOLT-WINTERS per forecast operativo quotidiano (veloce)
   3. Usa INTRADAY DINAMICO per pianificazione turni
   4. Confronta sempre almeno 2-3 modelli per validazione
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

        casiduso_text.insert("1.0", guida_pratica)
        casiduso_text.config(state="disabled")

        # ========== TAB ALERT ==========
        alert_canvas, alert_scrollable = self._create_scrollable_frame(tab_alert)

        ttk.Label(alert_scrollable, text="Alert e Avvisi Automatici", font=("Helvetica", 14, "bold")).pack(pady=10)

        alert_info = ttk.Label(alert_scrollable, text="Sistema di monitoraggio automatico per identificare condizioni di attenzione",
                              font=("Helvetica", 9, "italic"))
        alert_info.pack()

        self.alert_tree = ttk.Treeview(alert_scrollable, columns=("icona", "severita", "titolo", "descrizione"),
                                       show='headings', height=15)
        self.alert_tree.heading("icona", text="")
        self.alert_tree.heading("severita", text="Severità")
        self.alert_tree.heading("titolo", text="Alert")
        self.alert_tree.heading("descrizione", text="Descrizione")

        self.alert_tree.column("icona", width=40, anchor='center')
        self.alert_tree.column("severita", width=100, anchor='center')
        self.alert_tree.column("titolo", width=200)
        self.alert_tree.column("descrizione", width=500)

        self.alert_tree.tag_configure('alta', background='#FED7D7')
        self.alert_tree.tag_configure('media', background='#FEFCBF')
        self.alert_tree.tag_configure('bassa', background='#E6FFFA')

        self.alert_tree.pack(fill="both", expand=True, padx=10, pady=10)

        legend_frame = ttk.Frame(alert_scrollable)
        legend_frame.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Label(legend_frame, text="🔴 Alta  |  ⚠️ Media  |  ℹ️ Bassa", font=("Helvetica", 9)).pack()

        # ========== TAB GRAFICI INTERATTIVI ==========
        interactive_canvas, interactive_scrollable = self._create_scrollable_frame(tab_plots_interactive)

        ttk.Label(interactive_scrollable, text="Grafici Interattivi con Zoom/Pan",
                 font=("Helvetica", 14, "bold")).pack(pady=10)

        toolbar_frame = ttk.Frame(interactive_scrollable)
        toolbar_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(toolbar_frame, text="Seleziona grafico:").pack(side="left", padx=5)
        plot_types = [
            ("Confronto Modelli", "forecast_comparison"),
            ("Serie Storica", "historical"),
            ("Trend", "trend")
        ]
        for text, val in plot_types:
            ttk.Radiobutton(toolbar_frame, text=text, variable=self.plot_type_var,
                           value=val, command=self.update_interactive_plot).pack(side="left", padx=5)

        self.interactive_fig = Figure(figsize=(12, 6), dpi=100)
        self.interactive_ax = self.interactive_fig.add_subplot(111)
        self.interactive_canvas = FigureCanvasTkAgg(self.interactive_fig, master=interactive_scrollable)
        self.interactive_canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=5)

        self.interactive_toolbar = NavigationToolbar2Tk(self.interactive_canvas, interactive_scrollable)
        self.interactive_toolbar.update()

        self.interactive_ax.text(0.5, 0.5, 'Esegui forecast per visualizzare grafici interattivi',
                                ha='center', va='center', fontsize=12, color='gray')
        self.interactive_canvas.draw()

        # ========== TAB CONFRONTO CONSUNTIVO ==========
        consuntivo_canvas, consuntivo_scrollable = self._create_scrollable_frame(tab_consuntivo)

        ttk.Label(consuntivo_scrollable, text="Confronto Forecast vs Consuntivo Effettivo",
                 font=("Helvetica", 14, "bold")).pack(pady=10)

        consuntivo_frame = ttk.Frame(consuntivo_scrollable, padding=10)
        consuntivo_frame.pack(fill="x")

        ttk.Label(consuntivo_frame, text="File Excel Consuntivo:").pack(side="left")
        ttk.Entry(consuntivo_frame, textvariable=self.consuntivo_path_var, width=60).pack(side="left", padx=5)
        ttk.Button(consuntivo_frame, text="Sfoglia", command=self.browse_consuntivo).pack(side="left", padx=2)
        ttk.Button(consuntivo_frame, text="Esegui Confronto", command=self.run_confronto_consuntivo).pack(side="left", padx=5)

        self.consuntivo_tree = ttk.Treeview(consuntivo_scrollable,
                                            columns=("modello", "mae", "mape", "smape", "n_giorni"),
                                            show='headings', height=10)
        self.consuntivo_tree.heading("modello", text="Modello")
        self.consuntivo_tree.heading("mae", text="MAE")
        self.consuntivo_tree.heading("mape", text="MAPE (%)")
        self.consuntivo_tree.heading("smape", text="SMAPE (%)")
        self.consuntivo_tree.heading("n_giorni", text="Giorni Confrontati")

        for col in self.consuntivo_tree['columns']:
            self.consuntivo_tree.column(col, width=120, anchor='center')

        self.consuntivo_tree.pack(fill="both", expand=True, padx=10, pady=10)

        self.consuntivo_status = ttk.Label(consuntivo_scrollable, text="Seleziona file consuntivo per avviare confronto",
                                          font=("Helvetica", 9, "italic"))
        self.consuntivo_status.pack(pady=5)

        # ========== TAB GRAFICI PNG ==========
        plots_canvas, plots_scrollable = self._create_scrollable_frame(tab_plots)

        combo_frame = ttk.Frame(plots_scrollable, padding=10)
        combo_frame.pack(fill="x")
        ttk.Label(combo_frame, text="Seleziona grafico PNG dall'ultimo output:").pack(side="left")
        self.plot_combo = ttk.Combobox(combo_frame, textvariable=self.plot_var, width=70, state="readonly")
        self.plot_combo.pack(side="left", padx=5, fill="x", expand=True)
        ttk.Button(combo_frame, text="Mostra", command=self.show_plot).pack(side="left")

        self.image_label = ttk.Label(plots_scrollable)
        self.image_label.pack(pady=6)
        self.image_caption = ttk.Label(plots_scrollable, font=("Helvetica", 9, "italic"))
        self.image_caption.pack()

        compare_canvas, compare_scrollable = self._create_scrollable_frame(tab_compare)

        compare_controls = ttk.Frame(compare_scrollable, padding=10)
        compare_controls.pack(fill="x")
        ttk.Label(compare_controls, text="Granularità confronto:").pack(side="left")
        ttk.Combobox(compare_controls, textvariable=self.period_var, values=['giorno', 'settimana', 'mese'], state='readonly', width=12, justify='center').pack(side="left", padx=5)
        ttk.Button(compare_controls, text="Aggiorna confronto", command=self.refresh_comparisons).pack(side="left")

        self.compare_tree = ttk.Treeview(compare_scrollable, columns=("periodo", "modello", "forecast", "mape"), show='headings', height=10)
        for col, lbl in zip(self.compare_tree['columns'], ["Periodo", "Modello", "Forecast", "MAPE (%)"]):
            self.compare_tree.heading(col, text=lbl)
        self.compare_tree.pack(fill="both", expand=True, padx=10, pady=4)
        
        # Configura tag per i colori del semaforo
        self.compare_tree.tag_configure('green', background='#C6F6D5')   # Verde chiaro
        self.compare_tree.tag_configure('yellow', background='#FEFCBF')  # Giallo chiaro
        self.compare_tree.tag_configure('red', background='#FED7D7')     # Rosso chiaro

        ttk.Label(compare_scrollable, text="Indici di affidabilità (backtest)", font=("Helvetica", 10, "bold")).pack(pady=(6, 0))
        self.metric_tree = ttk.Treeview(compare_scrollable, columns=("modello", "mae", "mape", "smape"), show='headings', height=6)
        for col, lbl in zip(self.metric_tree['columns'], ["Modello", "MAE", "MAPE", "SMAPE"]):
            self.metric_tree.heading(col, text=lbl)
        self.metric_tree.pack(fill="both", expand=True, padx=10, pady=4)

        ttk.Label(compare_scrollable, text="Metriche per orizzonte (MAPE/MAE/SMAPE)", font=("Helvetica", 10, "bold")).pack(pady=(6, 0))
        self.metric_horizon_tree = ttk.Treeview(compare_scrollable, columns=("modello", "horizon", "mae", "mape", "smape"), show='headings', height=7)
        for col, lbl in zip(self.metric_horizon_tree['columns'], ["Modello", "Orizzonte", "MAE", "MAPE", "SMAPE"]):
            self.metric_horizon_tree.heading(col, text=lbl)
        self.metric_horizon_tree.pack(fill="both", expand=True, padx=10, pady=4)

        # --- NEW: Best Bet & Insights Section ---
        insight_frame = ttk.LabelFrame(compare_scrollable, text="💡 Best Bet & Actionable Insights", padding=10)
        insight_frame.pack(fill="x", padx=10, pady=10)

        # Best Bet Highlight
        best_bet_container = ttk.Frame(insight_frame)
        best_bet_container.pack(fill="x", pady=(0, 10))
        
        lbl_best = ttk.Label(best_bet_container, text="🏆 MODELLO CONSIGLIATO:", font=("Helvetica", 11, "bold"), foreground="#2c5282")
        lbl_best.pack(side="left")
        
        self.lbl_best_model_name = ttk.Label(best_bet_container, textvariable=self.best_model_var, font=("Helvetica", 14, "bold"), foreground="#276749")
        self.lbl_best_model_name.pack(side="left", padx=10)

        # Recommendation Text Area
        ttk.Label(insight_frame, text="✅ Raccomandazioni operative:", font=("Helvetica", 10, "bold")).pack(anchor="w")
        self.txt_recommendation = tk.Text(insight_frame, height=4, wrap=tk.WORD, bg="#f0fff4", relief="flat", font=("Helvetica", 10))
        self.txt_recommendation.pack(fill="x", pady=5)
        self.txt_recommendation.insert(tk.END, "In attesa di elaborazione...")
        self.txt_recommendation.configure(state="disabled")
        # ----------------------------------------

        ttk.Label(compare_scrollable, textvariable=self.best_detail_var, font=("Helvetica", 10, "italic"), foreground="#555").pack(pady=(2, 8))

        guide_canvas, guide_scrollable = self._create_scrollable_frame(tab_guide)

        guide_text = ScrolledText(guide_scrollable, wrap=tk.WORD, height=20)
        guide_text.pack(fill="both", expand=True, padx=10, pady=10)
        guide_text.insert(tk.END, """Guida rapida ai modelli disponibili:\n\n"
                                 "- holtwinters: stagionalità settimanale, veloce e robusto.\n"
                                 "- pattern: media delle stagionalità storiche, baseline semplice.\n"
                                 "- naive: replica l'ultimo valore o media breve periodo, controllo qualità.\n"
                                 "- sarima: trend + stagionalità con correlazione autoregressiva.\n"
                                 "- prophet: stagionalità multiple e festività personalizzabili.\n"
                                 "- tbats: multiple stagionalità complesse (richiede tbats).\n"
                                 "- intraday_dinamico: distribuzione per fascia oraria, utile per staffing.\n"
                                 "- ensemble_top2: media dei due modelli con MAPE più bassa.\n\n"
                                 "Suggerimento: scegli i modelli dal pannello iniziale, escludi le festività non più valide e confronta le curve per giorno/settimana/mese insieme agli indici di affidabilità.""")
        guide_text.configure(state='disabled')

        output_canvas, output_scrollable = self._create_scrollable_frame(tab_output)

        output_intro = ttk.Label(output_scrollable, text="File principali generati (doppio click per aprire)", font=("Helvetica", 10, "bold"))
        output_intro.pack(pady=(8, 4))
        self.output_tree = ttk.Treeview(output_scrollable, columns=("nome", "tipo", "path"), show='headings', height=12)
        for col, lbl, width in zip(self.output_tree['columns'], ["File", "Tipo", "Percorso"], [200, 100, 400]):
            self.output_tree.heading(col, text=lbl)
            self.output_tree.column(col, width=width, anchor='w')
        self.output_tree.pack(fill="both", expand=True, padx=10, pady=4)
        self.output_tree.bind('<Double-1>', self._open_selected_output)
        ttk.Button(output_scrollable, text="Apri file selezionato", command=self._open_selected_output).pack(pady=(0, 10))

        log_canvas, log_scrollable = self._create_scrollable_frame(tab_log)

        log_hint = ttk.Label(log_scrollable, text="Avanzamento in tempo reale (replica il prompt)", font=("Helvetica", 10, "bold"))
        log_hint.pack(pady=(8, 4))
        log_actions = ttk.Frame(log_scrollable)
        log_actions.pack(fill="x", padx=10)
        ttk.Button(log_actions, text="Cancella log", command=lambda: self.log_widget.delete("1.0", tk.END)).pack(side="left")
        ttk.Button(log_actions, text="Copia log", command=self._copy_log).pack(side="left", padx=(6, 0))
        self.log_widget = ScrolledText(log_scrollable, height=18)
        self.log_widget.pack(fill="both", expand=True, padx=10, pady=6)

        # Setup zoom keybindings
        self.root.bind("<Control-plus>", lambda e: self._zoom_in())
        self.root.bind("<Control-equal>", lambda e: self._zoom_in())  # Ctrl+= (same key as +)
        self.root.bind("<Control-minus>", lambda e: self._zoom_out())
        self.root.bind("<Control-0>", lambda e: self._zoom_reset())

    def _zoom_in(self):
        """Aumenta lo zoom (Ctrl+)"""
        if self.zoom_level < 5:
            self.zoom_level += 1
            self._apply_zoom()

    def _zoom_out(self):
        """Diminuisce lo zoom (Ctrl-)"""
        if self.zoom_level > -5:
            self.zoom_level -= 1
            self._apply_zoom()

    def _zoom_reset(self):
        """Reset zoom (Ctrl+0)"""
        self.zoom_level = 0
        self._apply_zoom()

    def _apply_zoom(self):
        """Applica il livello di zoom corrente a tutti i widget"""
        # This is a simplified implementation that updates font sizes
        # For a complete implementation, we would need to traverse all widgets
        # and update their fonts. For now, this provides the framework.
        pass

    def browse_input(self):
        path = filedialog.askdirectory(title="Seleziona cartella input")
        if path:
            self.input_dir_var.set(path)

    def _copy_log(self):
        try:
            content = self.log_widget.get("1.0", tk.END).strip()
            self.root.clipboard_clear()
            self.root.clipboard_append(content)
            messagebox.showinfo("Log copiato", "Il log corrente è stato copiato negli appunti.")
        except Exception as exc:
            messagebox.showerror("Errore copia", str(exc))

    def _ensure_log_polling(self):
        """Mantiene attivo il polling della coda log in maniera resiliente."""
        if self._log_polling_forced_stop:
            return
        if not self._log_polling:
            self._log_polling = True
            self._poll_log_queue()

    def _start_log_polling(self):
        self._log_polling_forced_stop = False
        self._ensure_log_polling()

    def _stop_log_polling(self):
        self._log_polling = False
        self._log_polling_forced_stop = True

    def _poll_log_queue(self):
        while not self.log_queue.empty():
            try:
                msg = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_widget.insert(tk.END, msg)
            self.log_widget.see(tk.END)
            self._last_log_ts = time.time()
        if self._log_polling:
            self.root.after(200, self._poll_log_queue)

    def _start_heartbeat(self):
        self._last_log_ts = time.time()
        self._last_heartbeat_ts = time.time()
        self.root.after(1000, self._heartbeat_tick)

    def _stop_heartbeat(self):
        self._worker_thread = None

    def _heartbeat_tick(self):
        now = time.time()
        if self._worker_thread and self._worker_thread.is_alive():
            if self._last_log_ts and now - self._last_log_ts > 5 and (
                not self._last_heartbeat_ts or now - self._last_heartbeat_ts > 5
            ):
                self.log_widget.insert(tk.END, "⏳ Elaborazione in corso...\n")
                self.log_widget.see(tk.END)
                self._last_heartbeat_ts = now
            self.root.after(1000, self._heartbeat_tick)

    def run_analysis(self):
        try:
            giorni = int(self.forecast_days_var.get())
        except ValueError:
            messagebox.showerror("Valore non valido", "Inserisci un numero di giorni intero.")
            return

        holidays_list = [h.strip() for h in self.holidays_var.get().split(',') if h.strip()]
        holidays_list.extend([h for h, var in self.holiday_flags_vars.items() if var.get()])
        holidays_list = sorted(set(holidays_list))
        selected_models = [m for m, var in self.model_vars.items() if var.get()]
        if not selected_models:
            messagebox.showerror("Nessun modello selezionato", "Seleziona almeno un modello da eseguire.")
            return
        input_root = self.input_dir_var.get()

        self.run_button.config(state="disabled")
        self.log_widget.delete("1.0", tk.END)
        self.results_box.delete(0, tk.END)
        self.best_model_var.set("In esecuzione...")
        while not self.log_queue.empty():
            try:
                self.log_queue.get_nowait()
            except queue.Empty:
                break
        self._start_log_polling()
        self._start_heartbeat()

        self.log_widget.insert(tk.END, "Avvio elaborazione...\n")
        self.log_widget.insert(tk.END, f"  Giorni forecast: {giorni}\n")
        self.log_widget.insert(tk.END, f"  Modelli selezionati: {', '.join(selected_models)}\n")
        if self.fast_mode_var.get():
            self.log_widget.insert(tk.END, "  Modalita veloce (fast mode): ON (modelli leggeri e backtest ridotto)\n")
        if holidays_list:
            self.log_widget.insert(tk.END, f"  Festività escluse: {', '.join(holidays_list)}\n")
        self.log_widget.insert(tk.END, "  Log in tempo reale qui sotto...\n\n")
        self.log_widget.see(tk.END)

        fast_mode = bool(self.fast_mode_var.get())

        thread = threading.Thread(
            target=self._run_batch,
            args=(giorni, holidays_list, input_root, selected_models, fast_mode),
            daemon=True,
        )
        self._worker_thread = thread
        # Avvia il sync del buffer locale per mostrare subito i log anche se la coda è vuota
        self._start_buffer_sync()
        thread.start()

    def _run_batch(self, giorni, holidays, input_root, modelli, fast_mode):
        buffer = io.StringIO()
        self._buffer_sync_ref = buffer
        # Duplica i log anche sullo stdout/stderr originale per visibilità da terminale
        log_writer = _GuiLogWriter(self.log_queue, buffer, mirror_stream=sys.stdout)
        queue_handler = _QueueLogHandler(self.log_queue)
        queue_handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger = logging.getLogger()
        old_level = root_logger.level
        root_handlers = list(root_logger.handlers)
        if not root_handlers:
            logging.basicConfig(level=logging.INFO)
        root_logger.addHandler(queue_handler)
        if old_level > logging.INFO:
            root_logger.setLevel(logging.INFO)

        input_dirs = [input_root]
        nested = os.path.join(input_root, "file input")
        if os.path.isdir(nested):
            input_dirs.append(nested)

        risultati = []
        old_stdout, old_stderr = sys.stdout, sys.stderr
        try:
            sys.stdout, sys.stderr = log_writer, log_writer
            with redirect_stdout(log_writer), redirect_stderr(log_writer):
                log_writer.write("\n>>> Avvio batch forecast dalla GUI...\n")
                log_writer.write(f"Cartelle input: {', '.join(input_dirs)}\n")
                log_writer.write(f"Modelli attivi: {', '.join(modelli)}\n")
                log_writer.write(f"Profilo veloce (--fast): {'ON' if fast_mode else 'OFF'}\n")
                if holidays:
                    log_writer.write(f"Festività escluse: {', '.join(holidays)}\n")
                global FAST_MODE
                previous_fast = FAST_MODE
                FAST_MODE = fast_mode
                try:
                    risultati = main(
                        giorni_forecast=giorni,
                        escludi_festivita=holidays or None,
                        input_dirs=input_dirs,
                        metodi=modelli
                    )
                finally:
                    FAST_MODE = previous_fast
                log_writer.write("\n>>> Elaborazione completata, preparo il riepilogo...\n")

                # Conta successi
                successi = sum(1 for r in risultati if r.get('success', False))
                totali = len(risultati)
                log_writer.write(f"\n✅ Elaborazione completata con successo: {successi}/{totali} file processati.\n")
        except Exception as exc:
            log_writer.write(f"\n❌ Errore GUI: {exc}\n")
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr
            try:
                root_logger.removeHandler(queue_handler)
            except Exception:
                pass
            queue_handler.close()
            root_logger.setLevel(old_level)

        output_text = buffer.getvalue()
        self.root.after(0, lambda: self._on_run_complete(risultati, output_text, buffer))

    def _on_run_complete(self, risultati, output_text, buffer):
        self.run_button.config(state="normal")
        self._poll_log_queue()
        self._drain_log_queue()
        self._stop_heartbeat()
        self._stop_buffer_sync(buffer)
        if output_text:
            self.log_widget.insert(tk.END, output_text)
        self.log_widget.see(tk.END)

        self.results_box.delete(0, tk.END)
        best_models = []
        self.last_output_dirs = []
        self.confronto_df = None
        self.backtest_metrics = None
        self.output_files = []

        for r in risultati:
            status = "✅" if r.get('success') else "❌"
            name = os.path.basename(r.get('file_path', 'sconosciuto'))
            best = r.get('miglior_modello', 'N/D')
            line = f"{status} {name} | best: {best}"
            self.results_box.insert(tk.END, line)
            if r.get('success'):
                self.last_output_dirs.append(r.get('output_dir'))
                if self.backtest_metrics is None and r.get('forecast_modelli', {}):
                    self.backtest_metrics = r['forecast_modelli'].get('backtest')

                confronto_main = r.get('forecast_modelli', {}).get('confronto_df')
                confronto_legacy = r.get('confronto_df')

                if self.confronto_df is None:
                    if isinstance(confronto_main, pd.DataFrame) and not confronto_main.empty:
                        self.confronto_df = confronto_main
                    elif isinstance(confronto_legacy, pd.DataFrame) and not confronto_legacy.empty:
                        self.confronto_df = confronto_legacy
            if r.get('miglior_modello'):
                best_models.append(r['miglior_modello'])

        if best_models:
            self.best_model_var.set(", ".join(sorted(set(best_models))))
        else:
            best_from_metrics = self._best_model_name()
            self.best_model_var.set(best_from_metrics or "N/D")

        # Aggiorna tab "Quale Modello Usare?"
        self._refresh_model_selector(risultati)

        # Aggiorna Dashboard KPI
        self._refresh_dashboard_kpi(risultati)

        # Aggiorna Alert
        self._refresh_alerts(risultati)

        # Aggiorna grafici interattivi
        if self.confronto_df is not None:
            self.update_interactive_plot()

        # Refresh esistenti
        self.refresh_plots()
        self.refresh_comparisons()
        self.refresh_output_files()
        self._stop_log_polling()

        if not risultati:
            self.log_widget.insert(tk.END, "⚠️  Elaborazione terminata senza risultati. Controlla i log sopra.\n")
        else:
            self.log_widget.insert(tk.END, "\n✅ Elaborazione completata. Riepilogo aggiornato.\n")
        self.log_widget.see(tk.END)

    def _start_buffer_sync(self):
        # Attiva la sincronizzazione periodica del buffer locale verso la GUI
        if not self._worker_thread:
            return
        self._buffer_sync_active = True
        self._buffer_sync_len = 0
        self.root.after(200, self._sync_buffer_flush)

    def _sync_buffer_flush(self):
        if not self._buffer_sync_active or self._buffer_sync_ref is None:
            return
        try:
            content = self._buffer_sync_ref.getvalue()
            if len(content) > self._buffer_sync_len:
                delta = content[self._buffer_sync_len:]
                self.log_widget.insert(tk.END, delta)
                self.log_widget.see(tk.END)
                self._buffer_sync_len = len(content)
        except Exception:
            pass
        if self._buffer_sync_active:
            self.root.after(300, self._sync_buffer_flush)

    def _stop_buffer_sync(self, buffer_ref=None):
        if buffer_ref is not None:
            self._buffer_sync_ref = buffer_ref
        self._buffer_sync_active = False
        if self._buffer_sync_ref is not None:
            try:
                content = self._buffer_sync_ref.getvalue()
                if len(content) > self._buffer_sync_len:
                    delta = content[self._buffer_sync_len:]
                    self.log_widget.insert(tk.END, delta)
                    self.log_widget.see(tk.END)
                    self._buffer_sync_len = len(content)
            except Exception:
                pass

    def _drain_log_queue(self):
        # Assicura che nessun messaggio rimanga bloccato in coda quando si chiude il polling
        while not self.log_queue.empty():
            try:
                msg = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_widget.insert(tk.END, msg)
            self.log_widget.see(tk.END)

    def refresh_plots(self):
        pngs = []
        for out_dir in self.last_output_dirs:
            if out_dir and os.path.isdir(out_dir):
                pngs.extend(sorted(glob.glob(os.path.join(out_dir, "*.png"))))

        self.plot_combo['values'] = pngs
        if pngs:
            self.plot_var.set(pngs[0])
            self.show_plot()
        else:
            self.plot_var.set("")
            self.image_label.configure(image="")
            self.image_caption.configure(text="")

    def refresh_comparisons(self):
        if self.confronto_df is None:
            return
        period = self.period_var.get()
        df = self.confronto_df.copy()
        df['DATA'] = pd.to_datetime(df['DATA'])
        df = df.sort_values('DATA').set_index('DATA')

        if period == 'settimana':
            df = df.resample('W-MON').sum(numeric_only=True).reset_index().rename(columns={'DATA': 'PERIODO'})
            df['PERIODO'] = df['PERIODO'].dt.strftime('%Y-%m-%d (set)')
        elif period == 'mese':
            df = df.resample('MS').sum(numeric_only=True).reset_index().rename(columns={'DATA': 'PERIODO'})
            df['PERIODO'] = df['PERIODO'].dt.strftime('%Y-%m (mese)')
        else:
            df = df.reset_index().rename(columns={'DATA': 'PERIODO'})
            df['PERIODO'] = df['PERIODO'].dt.strftime('%Y-%m-%d')

        long_df = df.melt(id_vars=['PERIODO'], var_name='modello', value_name='forecast')
        reliability = self._reliability_map()

        for item in self.compare_tree.get_children():
            self.compare_tree.delete(item)
        for _, row in long_df.iterrows():
            mape_val = reliability.get(row['modello'])
            mape_str = f"{mape_val:.2f}" if mape_val is not None else "-"
            
            # --- NEW: Traffic Light Tag Assignment ---
            tag = ''
            if mape_val is not None:
                if mape_val < 5.0:
                    tag = 'green'
                elif mape_val < 10.0:
                    tag = 'yellow'
                else:
                    tag = 'red'
            # -----------------------------------------

            self.compare_tree.insert('', 'end', values=(row['PERIODO'], row['modello'], f"{row['forecast']:.1f}", mape_str), tags=(tag,))

        self._refresh_metric_tree()
        self._refresh_metric_horizon_tree()

    def _reliability_map(self):
        if not self.backtest_metrics:
            return {}
        mapping = {}
        for modello, vals in self.backtest_metrics.items():
            mape_val = vals.get('MAPE')
            if mape_val is None or not np.isfinite(mape_val):
                horizons = vals.get('by_horizon', {})
                if horizons:
                    best_h = min(horizons, key=lambda h: horizons[h].get('MAPE', np.inf))
                    mape_val = horizons[best_h].get('MAPE')
            if mape_val is None or not np.isfinite(mape_val):
                continue
            mapping[modello] = float(mape_val)
        return mapping

    def _best_model_name(self):
        reliability = self._reliability_map()
        if not reliability:
            return None
        return min(reliability, key=reliability.get)

    def _refresh_metric_tree(self):
        for item in self.metric_tree.get_children():
            self.metric_tree.delete(item)
        if not self.backtest_metrics:
            return
        for modello, vals in sorted(self.backtest_metrics.items()):
            mae = vals.get('MAE') or (min(vals.get('by_horizon', {}), key=lambda h: vals['by_horizon'][h].get('MAE', np.inf)) if vals.get('by_horizon') else None)
            mape = vals.get('MAPE') or (min(vals.get('by_horizon', {}), key=lambda h: vals['by_horizon'][h].get('MAPE', np.inf)) if vals.get('by_horizon') else None)
            smape = vals.get('SMAPE') or (min(vals.get('by_horizon', {}), key=lambda h: vals['by_horizon'][h].get('SMAPE', np.inf)) if vals.get('by_horizon') else None)
            def _extract(val, key):
                return vals['by_horizon'][val].get(key) if isinstance(val, (int, float)) else None
            mae_val = vals.get('MAE') if vals.get('MAE') is not None else _extract(mae, 'MAE')
            mape_val = vals.get('MAPE') if vals.get('MAPE') is not None else _extract(mape, 'MAPE')
            smape_val = vals.get('SMAPE') if vals.get('SMAPE') is not None else _extract(smape, 'SMAPE')
            self.metric_tree.insert('', 'end', values=(modello, _fmt(mae_val), _fmt(mape_val), _fmt(smape_val)))

    def _refresh_metric_horizon_tree(self):
        for item in self.metric_horizon_tree.get_children():
            self.metric_horizon_tree.delete(item)
        if not self.backtest_metrics:
            return
        for modello, valori in sorted(self.backtest_metrics.items()):
            for horizon, metriche in sorted(valori.get('by_horizon', {}).items()):
                self.metric_horizon_tree.insert('', 'end', values=(
                    modello,
                    f"{horizon} gg",
                    _fmt(metriche.get('MAE')),
                    _fmt(metriche.get('MAPE')),
                    _fmt(metriche.get('SMAPE')),
                ))

    def _best_summary_text(self):
        if not self.backtest_metrics:
            return "Miglior modello: N/D (nessuna metrica disponibile)"
        reliability = self._reliability_map()
        if not reliability:
            return "Miglior modello: N/D (MAPE non calcolata o infinita)"
        best_model = min(reliability, key=reliability.get)
        best_mape = reliability[best_model]
        horizon = None
        by_h = self.backtest_metrics.get(best_model, {}).get('by_horizon', {})
        if by_h:
            horizon = min(by_h, key=lambda h: by_h[h].get('MAPE', np.inf))
        ensemble_note = ""
        if 'ensemble_top2' in self.backtest_metrics:
            ensemble_note = " | ensemble_top2 disponibile"
        horizon_txt = f" (orizzonte {horizon} gg)" if horizon else ""
        return f"Miglior modello dal backtest: {best_model}{horizon_txt} — MAPE {best_mape:.2f}%{ensemble_note}"

    def refresh_output_files(self):
        for item in self.output_tree.get_children():
            self.output_tree.delete(item)
        self.output_files = self._collect_output_files()
        for name, tipo, path in self.output_files:
            self.output_tree.insert('', 'end', values=(name, tipo, path))

    def _collect_output_files(self):
        files = []
        for out_dir in self.last_output_dirs:
            if not out_dir or not os.path.isdir(out_dir):
                continue
            for pattern, tipo in [("*.xlsx", "Excel"), ("*.txt", "Testo")]:
                for path in sorted(glob.glob(os.path.join(out_dir, pattern))):
                    files.append((os.path.basename(path), tipo, path))
        return files

    def _open_selected_output(self, event=None):
        selection = self.output_tree.selection()
        if not selection:
            messagebox.showinfo("Nessun file", "Seleziona un file dalla lista per aprirlo.")
            return
        item = selection[0]
        path = self.output_tree.item(item, 'values')[2]
        if not os.path.isfile(path):
            messagebox.showerror("File non trovato", path)
            return
        self._open_path(path)

    def _open_path(self, path):
        try:
            if sys.platform.startswith('win'):
                os.startfile(path)
            elif sys.platform == 'darwin':
                subprocess.call(['open', path])
            else:
                subprocess.call(['xdg-open', path])
        except Exception as exc:
            messagebox.showerror("Impossibile aprire il file", str(exc))

    def show_plot(self):
        path = self.plot_var.get()
        if not path or not os.path.isfile(path):
            messagebox.showinfo("Nessun grafico", "Esegui una run per caricare i grafici disponibili.")
            return

        try:
            img = tk.PhotoImage(file=path)
        except Exception as exc:
            messagebox.showerror("Errore apertura grafico", str(exc))
            return

        max_w, max_h = 900, 520
        w, h = img.width(), img.height()
        scale = max(w / max_w, h / max_h, 1)
        displayed_w, displayed_h = w, h
        if scale > 1:
            factor = int(math.ceil(scale))
            img = img.subsample(factor, factor)
            displayed_w = max(1, w // factor)
            displayed_h = max(1, h // factor)

        self.current_image = img
        self.image_label.configure(image=img)
        caption = f"{os.path.basename(path)}  ({displayed_w}x{displayed_h} px visualizzati)"
        self.image_caption.configure(text=caption)

    def _create_scrollable_frame(self, parent):
        """
        Crea un frame scrollable riutilizzabile con supporto mouse wheel.
        Returns: tuple: (canvas, scrollable_frame)
        """
        canvas = tk.Canvas(parent, highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        def on_frame_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        scrollable_frame.bind("<Configure>", on_frame_configure)
        canvas_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        def on_canvas_configure(event):
            canvas_width = event.width
            canvas.itemconfig(canvas_window, width=canvas_width)

        canvas.bind("<Configure>", on_canvas_configure)
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Mouse wheel scrolling
        def on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")

        canvas.bind_all("<MouseWheel>", on_mousewheel)

        return canvas, scrollable_frame

    def _create_kpi_card(self, parent, title, initial_value, color, row, col):
        """Crea una card KPI stilizzata."""
        card = ttk.Frame(parent, relief="solid", borderwidth=1)
        card.grid(row=row, column=col, padx=10, pady=10, sticky="nsew")

        parent.columnconfigure(col, weight=1)
        parent.rowconfigure(row, weight=1)

        ttk.Label(card, text=title, font=("Helvetica", 10)).pack(pady=(10, 5))
        value_label = ttk.Label(card, text=initial_value, font=("Helvetica", 20, "bold"), foreground=color)
        value_label.pack(pady=(5, 10))

        return value_label

    def update_interactive_plot(self):
        """Aggiorna il grafico interattivo in base alla selezione."""
        if self.confronto_df is None or self.confronto_df.empty:
            return

        self.interactive_ax.clear()
        plot_type = self.plot_type_var.get()

        if plot_type == "forecast_comparison":
            model_cols = [c for c in self.confronto_df.columns if c != 'DATA']
            colors = ['#2E86AB', '#A23B72', '#F18F01', '#6A994E', '#BC4B51', '#8B5CF6', '#C73E1D']

            for i, model in enumerate(model_cols[:7]):
                self.interactive_ax.plot(self.confronto_df['DATA'], self.confronto_df[model],
                                        label=model.upper(), linewidth=2,
                                        color=colors[i % len(colors)], alpha=0.8)

            self.interactive_ax.set_title('Confronto Forecast tra Modelli', fontsize=12, fontweight='bold')
            self.interactive_ax.set_ylabel('Chiamate Previste')
            self.interactive_ax.legend(loc='best')

        elif plot_type == "historical":
            self.interactive_ax.text(0.5, 0.5, 'Serie storica non ancora implementata',
                                   ha='center', va='center')

        elif plot_type == "trend":
            self.interactive_ax.text(0.5, 0.5, 'Analisi trend non ancora implementata',
                                   ha='center', va='center')

        self.interactive_ax.grid(True, alpha=0.3)
        self.interactive_fig.autofmt_xdate()
        self.interactive_canvas.draw()

    def browse_consuntivo(self):
        """Apri dialog per selezionare file consuntivo."""
        path = filedialog.askopenfilename(
            title="Seleziona file Excel consuntivo",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
        )
        if path:
            self.consuntivo_path_var.set(path)

    def run_confronto_consuntivo(self):
        """Esegue confronto forecast vs consuntivo."""
        consuntivo_path = self.consuntivo_path_var.get()
        if not consuntivo_path or not os.path.isfile(consuntivo_path):
            messagebox.showerror("File non valido", "Seleziona un file Excel consuntivo valido")
            return

        if self.confronto_df is None or self.confronto_df.empty:
            messagebox.showerror("Nessun forecast", "Esegui prima un forecast prima del confronto")
            return

        if not self.last_output_dirs:
            messagebox.showerror("Output mancante", "Nessuna cartella output disponibile")
            return

        output_dir = self.last_output_dirs[0]

        try:
            self.consuntivo_status.configure(text="Elaborazione confronto in corso...")
            self.root.update()

            risultato = confronta_forecast_consuntivo(self.confronto_df, consuntivo_path, output_dir)

            if risultato is None:
                messagebox.showwarning("Nessuna sovrapposizione",
                                      "Nessuna data in comune tra forecast e consuntivo")
                self.consuntivo_status.configure(text="Nessuna sovrapposizione rilevata")
                return

            for item in self.consuntivo_tree.get_children():
                self.consuntivo_tree.delete(item)

            for modello, metriche in sorted(risultato['metriche'].items(), key=lambda x: x[1]['MAPE']):
                self.consuntivo_tree.insert('', 'end', values=(
                    modello,
                    f"{metriche['MAE']:.1f}",
                    f"{metriche['MAPE']:.2f}",
                    f"{metriche['SMAPE']:.2f}",
                    metriche['n_giorni']
                ))

            self.confronto_consuntivo_df = risultato['confronto_df']
            periodo = risultato['periodo_overlap']
            self.consuntivo_status.configure(
                text=f"✅ Confronto completato: {periodo[0].strftime('%d/%m/%Y')} - {periodo[1].strftime('%d/%m/%Y')}"
            )

            messagebox.showinfo("Confronto Completato",
                               f"Metriche calcolate per {len(risultato['metriche'])} modelli.\n"
                               f"File salvato: {risultato['output_path'].name}")

        except Exception as e:
            messagebox.showerror("Errore confronto", f"Errore durante confronto:\n{str(e)}")
            self.consuntivo_status.configure(text=f"❌ Errore: {str(e)}")

    def _refresh_dashboard_kpi(self, risultati):
        """Aggiorna il tab Dashboard KPI con i risultati forecast."""
        if not risultati:
            return

        r = next((x for x in risultati if x.get('success')), None)
        if not r:
            return

        kpi = r.get('kpi', {})
        forecast_modelli = r.get('forecast_modelli', {})

        confronto = forecast_modelli.get('confronto_df')
        if confronto is not None and not confronto.empty:
            best_model = r.get('miglior_modello')
            forecast_col = best_model if best_model and best_model in confronto.columns else \
                           [c for c in confronto.columns if c != 'DATA'][0]

            totale_forecast = confronto[forecast_col].sum()
            self.kpi_widgets['totale'].configure(text=f"{totale_forecast:,.0f}")

            df = r.get('df')
            if df is not None:
                daily_historical = df.groupby('DATA')['OFFERTO'].sum()
                media_storica = daily_historical.mean()
                media_forecast = confronto[forecast_col].mean()
                variazione_pct = ((media_forecast - media_storica) / media_storica * 100) if media_storica > 0 else 0

                arrow = "↗" if variazione_pct > 1 else "↘" if variazione_pct < -1 else "→"
                color = "#48BB78" if variazione_pct > 0 else "#F56565" if variazione_pct < 0 else "#718096"
                self.kpi_widgets['variazione'].configure(text=f"{variazione_pct:+.1f}% {arrow}", foreground=color)

                soglia_picco = media_storica + 2 * daily_historical.std()
                n_picchi = (confronto[forecast_col] > soglia_picco).sum()
                self.kpi_widgets['picchi'].configure(text=str(n_picchi))

                trend_coeff = np.polyfit(range(len(confronto)), confronto[forecast_col].values, 1)[0]
                if trend_coeff > media_forecast * 0.01:
                    trend_text = "CRESCITA ↗"
                    trend_color = "#48BB78"
                elif trend_coeff < -media_forecast * 0.01:
                    trend_text = "DECRESCITA ↘"
                    trend_color = "#F56565"
                else:
                    trend_text = "STABILE →"
                    trend_color = "#718096"
                self.kpi_widgets['trend'].configure(text=trend_text, foreground=trend_color)

        backtest = forecast_modelli.get('backtest')
        if backtest:
            valid_models = {m: v.get('MAPE') for m, v in backtest.items()
                           if v.get('MAPE') is not None and np.isfinite(v.get('MAPE'))}
            if valid_models:
                best_model = min(valid_models, key=valid_models.get)
                best_mape = valid_models[best_model]

                if best_mape < 5:
                    mape_text = f"{best_mape:.1f}% ✅"
                    mape_color = "#48BB78"
                elif best_mape < 10:
                    mape_text = f"{best_mape:.1f}% ⚠️"
                    mape_color = "#ECC94B"
                else:
                    mape_text = f"{best_mape:.1f}% 🔴"
                    mape_color = "#F56565"

                self.kpi_widgets['affidabilita'].configure(text=mape_text, foreground=mape_color)
                self.kpi_widgets['modello'].configure(text=best_model.upper()[:15])

    def _refresh_alerts(self, risultati):
        """Aggiorna il tab Alert con nuovi alert rilevati."""
        for item in self.alert_tree.get_children():
            self.alert_tree.delete(item)

        self.alerts_list = []
        for r in risultati:
            if r.get('success') and 'alerts' in r:
                self.alerts_list.extend(r['alerts'])

        for alert in self.alerts_list:
            self.alert_tree.insert('', 'end',
                                  values=(alert['icona'],
                                         alert['severita'].upper(),
                                         alert['titolo'],
                                         alert['descrizione']),
                                  tags=(alert['severita'],))

        if not self.alerts_list:
            self.alert_tree.insert('', 'end',
                                  values=("✅", "INFO", "Nessun Alert",
                                         "Nessuna condizione di attenzione rilevata"),
                                  tags=('bassa',))

    def _refresh_model_selector(self, risultati):
        """Aggiorna il tab 'Quale Modello Usare?' con raccomandazione e tabella comparativa."""
        # 1. AGGIORNA RACCOMANDAZIONE AUTOMATICA
        self.recommendation_text.configure(state="normal")
        self.recommendation_text.delete("1.0", tk.END)

        if not self.backtest_metrics:
            self.recommendation_text.insert("1.0", "⏳ Esegui un forecast per vedere la raccomandazione automatica...")
            self.recommendation_text.configure(state="disabled")
            return

        # Trova modello migliore
        valid_models = {m: v.get('MAPE') for m, v in self.backtest_metrics.items()
                       if v.get('MAPE') is not None and np.isfinite(v.get('MAPE'))}

        if not valid_models:
            self.recommendation_text.insert("1.0", "❌ Nessun modello valido trovato nel backtest.")
            self.recommendation_text.configure(state="disabled")
            return

        best_model = min(valid_models, key=valid_models.get)
        best_mape = valid_models[best_model]

        # Costruisci raccomandazione dettagliata
        rec_lines = []
        rec_lines.append("🏆 MODELLO RACCOMANDATO\n")
        rec_lines.append("=" * 80 + "\n\n")
        rec_lines.append(f"🎯 USA: {best_model.upper()}\n\n")

        # Affidabilità
        if best_mape < 5:
            rating = "★★★★★ ECCELLENTE"
            advice = "Questo forecast è MOLTO affidabile. Puoi usarlo per pianificazione dettagliata."
        elif best_mape < 10:
            rating = "★★★★☆ BUONO"
            advice = "Buona affidabilità. Monitora i picchi, ma il trend generale è solido."
        elif best_mape < 15:
            rating = "★★★☆☆ DISCRETO"
            advice = "Affidabilità moderata. Usa con margine di sicurezza del 10-15%."
        else:
            rating = "★★☆☆☆ BASSO"
            advice = "Affidabilità limitata. Considera come stima indicativa, non previsione precisa."

        rec_lines.append(f"📊 Affidabilità: {rating}\n")
        rec_lines.append(f"   Errore medio (MAPE): {best_mape:.1f}%\n\n")
        rec_lines.append(f"💡 Consiglio: {advice}\n\n")

        # Alternative
        sorted_models = sorted(valid_models.items(), key=lambda x: x[1])
        if len(sorted_models) > 1:
            rec_lines.append("📋 ALTERNATIVE:\n")
            for model, mape in sorted_models[1:4]:  # Top 3 alternative
                stars = "★" * max(1, int(5 - mape/5))
                rec_lines.append(f"   {stars:5s} {model.upper():15s} - MAPE {mape:.1f}%\n")

        rec_text = "".join(rec_lines)
        self.recommendation_text.insert("1.0", rec_text)
        self.recommendation_text.configure(state="disabled")

        # 2. AGGIORNA TABELLA COMPARATIVA
        # Pulisci tabella
        for item in self.model_comparison_tree.get_children():
            self.model_comparison_tree.delete(item)

        # Definizione caratteristiche modelli (info statiche)
        model_info = {
            'holtwinters': {
                'velocita': '⚡⚡⚡ Veloce',
                'quando': 'Pattern settimanale regolare, forecast rapido quotidiano'
            },
            'prophet': {
                'velocita': '⚡⚡☆ Medio',
                'quando': 'Trend marcato, festività importanti, massima precisione'
            },
            'tbats': {
                'velocita': '⚡☆☆ Lento',
                'quando': 'Multiple stagionalità, dati complessi, analisi approfondite'
            },
            'sarima': {
                'velocita': '⚡⚡☆ Medio',
                'quando': 'Correlazione autoregressiva, dati stabili e lunghi'
            },
            'pattern': {
                'velocita': '⚡⚡⚡ Veloce',
                'quando': 'Baseline semplice, confronto, dati molto stabili'
            },
            'naive': {
                'velocita': '⚡⚡⚡ Veloce',
                'quando': 'Controllo baseline, forecast sicurezza'
            },
            'intraday_dinamico': {
                'velocita': '⚡⚡☆ Medio',
                'quando': 'Distribuzione oraria, pianificazione turni, staffing'
            }
        }

        # Popola tabella con modelli effettivamente eseguiti
        for model, mape in sorted_models:
            # Rating basato su MAPE
            if mape < 5:
                rating = "★★★★★"
                accuratezza = "Eccellente"
            elif mape < 10:
                rating = "★★★★☆"
                accuratezza = "Buono"
            elif mape < 15:
                rating = "★★★☆☆"
                accuratezza = "Discreto"
            elif mape < 25:
                rating = "★★☆☆☆"
                accuratezza = "Sufficiente"
            else:
                rating = "★☆☆☆☆"
                accuratezza = "Basso"

            info = model_info.get(model, {'velocita': 'N/D', 'quando': 'N/D'})

            self.model_comparison_tree.insert('', 'end',
                values=(model.upper(), rating, info['velocita'], accuratezza, info['quando']))

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Analisi Traffico WFM')
    parser.add_argument('--input-dir', type=str, help='Cartella con file Excel di input', default=None)
    parser.add_argument('--fast', action='store_true', help='Modalità veloce')
    parser.add_argument('--gui', action='store_true', help='Avvia interfaccia grafica')
    args, unknown = parser.parse_known_args()

    # Aggiorna variabili globali in base agli argomenti
    if args.fast:
        FAST_MODE = True
        os.environ["FORECAST_FAST"] = "1"
    
    if args.gui or os.environ.get("FORECAST_GUI") == "1":
        gui = ForecastGUI()
        gui.run()
        sys.exit(0)

    input_dirs_cli = [args.input_dir] if args.input_dir else None

    print("\n" + "=" * 80)
    print(f"SCRIPT AGGIORNATO: versione {SCRIPT_VERSION} (ultimo update: {LAST_UPDATE})")
    print("=" * 80 + "\n")

    print("=" * 80)
    print("VERIFICA LIBRERIE DISPONIBILI")
    print("=" * 80)
    print(f"  pandas/numpy/matplotlib: ✅ OK (obbligatorie)")
    print(f"  scipy: {'✅ OK' if SCIPY_AVAILABLE else '❌ MANCANTE'}")
    print(f"  scikit-learn: {'✅ OK' if SKLEARN_AVAILABLE else '❌ MANCANTE'}")
    print(f"  statsmodels: {'✅ OK' if STATSMODELS_AVAILABLE else '❌ MANCANTE (pip install statsmodels)'}")
    print(f"  tbats: {'✅ OK' if TBATS_AVAILABLE else '⚠️  OPZIONALE (pip install tbats)'}")
    print()

    if not STATSMODELS_AVAILABLE:
        print("⚠️  ATTENZIONE: Per forecast Holt-Winters/SARIMA ottimale installa statsmodels:")
        print("  pip install statsmodels")
        print("Verra' usato un metodo fallback comunque funzionante.\n")

    if not TBATS_AVAILABLE:
        print("💡 SUGGERIMENTO: Per forecast con multiple stagionalità (TBATS):")
        print("  pip install tbats")
        print("TBATS è particolarmente efficace per call center con pattern complessi.\n")

    print("📦 Per installare tutte le dipendenze opzionali:")
    print("  pip install statsmodels tbats prophet holidays")
    print("=" * 80 + "\n")
    
    print("\n" + "=" * 80)
    print("CONFIGURAZIONE FORECAST")
    print("=" * 80)

    # =========================================================================
    # *** PERSONALIZZA QUI IL PERIODO FORECAST ***
    # =========================================================================
    GIORNI_FORECAST = 90  # <-- CAMBIA QUESTO VALORE!
                          # Es: 7 = 1 settimana
                          #     14 = 2 settimane
                          #     28 = 4 settimane (default)
                          #     60 = ~2 mesi
                          #     90 = ~3 mesi

    # =========================================================================
    # *** PERSONALIZZA GESTIONE FESTIVITÀ (solo per Prophet) ***
    # =========================================================================
    # Se nel 2025 APRI il servizio in giorni che prima erano CHIUSI,
    # escludi quelle festività dalla lista. Prophet imparerà dai dati storici
    # che erano chiuse, ma se cambi policy devi escluderle manualmente.
    #
    # Esempio: Se nel 2024 eri chiuso a Natale (0 chiamate) ma nel 2025 apri:
    # ESCLUDI_FESTIVITA = ['Natale', 'Santo_Stefano']
    #
    # Se mantieni le stesse policy dell'anno scorso, lascia vuoto:
    ESCLUDI_FESTIVITA = []  # <-- Lista festività da escludere

    # Festività disponibili:
    # 'Capodanno', 'Epifania', 'Festa_Liberazione', 'Festa_Lavoro',
    # 'Festa_Repubblica', 'Ferragosto', 'Ognissanti', 'Immacolata',
    # 'Natale', 'Santo_Stefano', 'Capodanno_Vigilia', 'Pasqua',
    # 'Venerdi_Santo', 'PostPasqua', 'Periodo_Natalizio', 'Post_Capodanno'
    # E tutti i pre-festivi/post-festivi (es. 'Natale_PreFestivo')
    # =========================================================================

    # Se vuoi selezionare solo alcuni modelli da CLI (oltre alla GUI):
    # Imposta la variabile d'ambiente FORECAST_MODELLI, es.:
    #   FORECAST_MODELLI=holtwinters,prophet,tbats
    # Se non impostata, verranno eseguiti tutti i modelli disponibili.
    env_modelli = os.environ.get("FORECAST_MODELLI")
    METODI_DA_ESEGUIRE = None
    if env_modelli:
        METODI_DA_ESEGUIRE = [m.strip() for m in env_modelli.split(',') if m.strip()]
    
    print(f"\n>>> FORECAST CONFIGURATO: {GIORNI_FORECAST} GIORNI <<<")
    print(f"    Equivalente a: {GIORNI_FORECAST/7:.1f} settimane")
    print(f"    Equivalente a: {GIORNI_FORECAST/30:.1f} mesi circa")
    if FAST_MODE:
        print("    Modalita veloce ATTIVA (fast mode): modelli leggeri e backtest ridotto")
    print("=" * 80 + "\n")
    
    # Esegui batch processing CON I PARAMETRI CORRETTI
    print(f"Avvio batch processing con forecast per {GIORNI_FORECAST} giorni...\n")
    if ESCLUDI_FESTIVITA:
        print(f"⚠️  Festività escluse da Prophet: {', '.join(ESCLUDI_FESTIVITA)}\n")
    risultati = main(
        giorni_forecast=GIORNI_FORECAST,
        escludi_festivita=ESCLUDI_FESTIVITA,
        metodi=METODI_DA_ESEGUIRE,
        input_dirs=input_dirs_cli
    )
    
    print("\n" + "=" * 80)
    print("ANALISI COMPLETATA CON SUCCESSO!")
    print("=" * 80)
    print(f"\nForecast generato per: {GIORNI_FORECAST} giorni")
    print("\nPer modificare il periodo, cambia GIORNI_FORECAST a riga ~975")
    print("\nEsempi comuni:")
    print("  GIORNI_FORECAST = 7   -> 1 settimana")
    print("  GIORNI_FORECAST = 14  -> 2 settimane")
    print("  GIORNI_FORECAST = 21  -> 3 settimane")
    print("  GIORNI_FORECAST = 28  -> 4 settimane")
    print("  GIORNI_FORECAST = 60  -> ~2 mesi")
    print("  GIORNI_FORECAST = 90  -> ~3 mesi")
    print("=" * 80 + "\n")









