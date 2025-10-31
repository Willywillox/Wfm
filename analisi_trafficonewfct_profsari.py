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

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from contextlib import contextmanager
import os
import glob
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

@contextmanager
def safe_excel_writer(path, **kwargs):
    path = Path(path)
    try:
        writer = pd.ExcelWriter(path, **kwargs)
    except PermissionError:
        fallback = path.with_name(f"{path.stem}_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
        print(f"  File {path.name} in uso, salvo come {fallback.name}")
        writer = pd.ExcelWriter(fallback, **kwargs)
        path = fallback
    try:
        yield writer, path
    finally:
        writer.close()


# Verifica librerie opzionali
try:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
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
    from tbats import TBATS
    TBATS_AVAILABLE = True
except ImportError:
    TBATS_AVAILABLE = False
    print("NOTA: TBATS non disponibile (opzionale)")
    print("Per multiple stagionalità avanzate: pip install tbats")

# Configurazione grafica
plt.rcParams['figure.figsize'] = (15, 8)
plt.rcParams['font.size'] = 10
sns.set_style("whitegrid")
sns.set_palette("husl")

# =============================================================================
# TROVA FILE EXCEL
# =============================================================================

def trova_file_excel():
    """Trova automaticamente il file Excel nella cartella dello script"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_excel = glob.glob(os.path.join(script_dir, '*.xlsx'))
    file_excel = [f for f in file_excel if not os.path.basename(f).startswith('~$')]
    
    if len(file_excel) == 0:
        raise FileNotFoundError("Nessun file Excel trovato nella cartella dello script")
    elif len(file_excel) == 1:
        print(f"File trovato: {os.path.basename(file_excel[0])}")
        return file_excel[0]
    else:
        print(f"Trovati {len(file_excel)} file Excel, uso il primo")
        return file_excel[0]

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
    plt.savefig(f'{output_dir}/curva_fascia_oraria.png', dpi=300, bbox_inches='tight')
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
    plt.savefig(f'{output_dir}/curva_giorno_settimana.png', dpi=300, bbox_inches='tight')
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
    plt.savefig(f'{output_dir}/curva_settimana.png', dpi=300, bbox_inches='tight')
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
    plt.savefig(f'{output_dir}/curva_mese.png', dpi=300, bbox_inches='tight')
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
    plt.savefig(f'{output_dir}/heatmap_giorno_fascia.png', dpi=300, bbox_inches='tight')
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
    with safe_excel_writer(output_path, engine='openpyxl') as (writer, actual_path):
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
    plt.savefig(f'{output_dir}/analisi_trend_storico.png', dpi=300, bbox_inches='tight')
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
    plt.savefig(f'{output_dir}/confronto_periodi.png', dpi=300, bbox_inches='tight')
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
    plt.savefig(f"{output_dir}/identificazione_anomalie.png", dpi=300, bbox_inches='tight')
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
            resid_std = float(np.nanstd(resid_week, ddof=1))
            if not np.isfinite(resid_std) or resid_std == 0.0:
                resid_std = float(np.nanstd(weekly['TOTALE'].values - fit_week.fittedvalues, ddof=1))
            if not np.isfinite(resid_std) or resid_std == 0.0:
                resid_std = float(np.nanstd(weekly['TOTALE'].values, ddof=1) * 0.1)

            ci_delta = 1.96 * resid_std
            lower = np.clip(forecast_week - ci_delta, a_min=0, a_max=None)
            upper = forecast_week + ci_delta

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
            resid_std = float(np.nanstd(resid_daily, ddof=1))
            if not np.isfinite(resid_std) or resid_std == 0.0:
                resid_std = float(np.nanstd(daily['OFFERTO'].values - fit_daily.fittedvalues, ddof=1))
            if not np.isfinite(resid_std) or resid_std == 0.0:
                resid_std = float(np.nanstd(daily['OFFERTO'].values, ddof=1) * 0.1)

            ci_delta = 1.96 * resid_std
            forecast_daily_df['CI_LOWER'] = np.clip(forecast_daily_df['FORECAST'] - ci_delta, a_min=0, a_max=None)
            forecast_daily_df['CI_UPPER'] = forecast_daily_df['FORECAST'] + ci_delta

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
            plt.savefig(f"{output_dir}/forecast_settimanale.png", dpi=300, bbox_inches='tight')

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
            plt.savefig(f"{output_dir}/forecast_giornaliero.png", dpi=300, bbox_inches='tight')

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
            plt.savefig(f"{output_dir}/forecast_intraday_esempio.png", dpi=300, bbox_inches='tight')
        else:
            print("   Nota: nessun forecast per fascia disponibile, grafico intraday saltato.")

        print("   Grafici salvati")

        excel_path = Path(output_dir) / 'forecast_completo.xlsx'
        with safe_excel_writer(excel_path, engine='openpyxl') as (writer, actual_path):
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
    return pd.DataFrame({
        'SETTIMANA': range(int(ultima_settimana) + 1, int(ultima_settimana) + weeks_ahead + 1),
        'FORECAST': forecast_values,
        'CI_LOWER': [v * 0.85 for v in forecast_values],
        'CI_UPPER': [v * 1.15 for v in forecast_values]
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

    return pd.DataFrame({
        'DATA': future_dates,
        'FORECAST': forecasts,
        'GG_SETT': [['lun','mar','mer','gio','ven','sab','dom'][d.weekday()] for d in future_dates],
        'CI_LOWER': [v * 0.85 for v in forecasts],
        'CI_UPPER': [v * 1.15 for v in forecasts]
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

        for fascia in fasce_uniche:
            df_questa_fascia = df_fascia[df_fascia['FASCIA'] == fascia].copy()

            if len(df_questa_fascia) < 14:
                # Dati insufficienti, usa media storica
                media_per_dow = df_questa_fascia.groupby('DOW')['OFFERTO'].mean().to_dict()
                for future_date in future_dates:
                    dow = future_date.dayofweek
                    forecast_val = media_per_dow.get(dow, df_questa_fascia['OFFERTO'].mean())
                    forecast_results.append({
                        'DATA': future_date,
                        'FASCIA': fascia,
                        'MINUTI': df_questa_fascia['MINUTI'].iloc[0] if len(df_questa_fascia) > 0 else 0,
                        'GG_SETT': ['lun','mar','mer','gio','ven','sab','dom'][dow],
                        'FORECAST': max(0, forecast_val)
                    })
                continue

            # Crea serie temporale per questa fascia
            ts = df_questa_fascia.groupby('DATA')['OFFERTO'].mean().sort_index()
            ts = ts.asfreq('D', fill_value=0)

            try:
                # Modello Holt-Winters con stagionalità settimanale
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
                media_per_dow = df_questa_fascia.groupby('DOW')['OFFERTO'].mean().to_dict()
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
                    'MINUTI': df_questa_fascia['MINUTI'].iloc[0] if len(df_questa_fascia) > 0 else 0,
                    'GG_SETT': ['lun','mar','mer','gio','ven','sab','dom'][dow],
                    'FORECAST': max(0, forecast_vals[i] if i < len(forecast_vals) else 0)
                })

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
    ci_window = max(0.15 * last_value, 1)

    forecast_daily_df = pd.DataFrame({
        'DATA': future_dates,
        'FORECAST': forecasts,
        'GG_SETT': [['lun','mar','mer','gio','ven','sab','dom'][d.weekday()] for d in future_dates],
        'CI_LOWER': np.clip(forecasts - ci_window, a_min=0, a_max=None),
        'CI_UPPER': forecasts + ci_window
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
    Include festività fisse e mobili (Pasqua).
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
        'Santo Stefano': (12, 26)
    }

    festivita_list = []

    # Festività fisse
    for anno in range(anno_inizio, anno_fine + 1):
        for nome, (mese, giorno) in festivita_fisse.items():
            festivita_list.append({
                'holiday': nome,
                'ds': pd.Timestamp(anno, mese, giorno),
                'lower_window': 0,
                'upper_window': 0
            })

    # Pasqua (calcolo approssimativo - per produzione usare libreria holidays)
    try:
        import holidays
        it_holidays = holidays.Italy(years=range(anno_inizio, anno_fine + 1))
        for data, nome in it_holidays.items():
            if 'Pasqua' in nome or 'Easter' in nome:
                festivita_list.append({
                    'holiday': 'Pasqua',
                    'ds': pd.Timestamp(data),
                    'lower_window': 0,
                    'upper_window': 1  # Lunedì dell'Angelo
                })
    except ImportError:
        # Se holidays non disponibile, usa solo festività fisse
        pass

    return pd.DataFrame(festivita_list)


def _forecast_prophet(df, giorni_forecast=28, produce_outputs=False):
    """Forecast con Prophet (se disponibile) - con gestione festività."""
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

    model = Prophet(
        holidays=festivita,  # ✅ NOVITÀ: Gestione festività
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
        estimator = TBATS(
            seasonal_periods=[7, 30.5],
            use_trend=True,
            use_box_cox=False,  # Box-Cox può essere instabile con dati call center
            n_jobs=1
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
    with safe_excel_writer(output_path, engine='openpyxl') as (writer, actual_path):
        forecast_data['giornaliero'].to_excel(writer, sheet_name='Forecast_Giornaliero', index=False)
        if 'per_fascia' in forecast_data:
            forecast_data['per_fascia'].to_excel(writer, sheet_name='Forecast_per_Fascia', index=False)
    return actual_path


def genera_forecast_modelli(df, output_dir, giorni_forecast=28, metodi=None):
    """
    Esegue più modelli di forecast in parallelo e produce un confronto.

    Args:
        df: dataframe sorgente
        output_dir: cartella output
        giorni_forecast: orizzonte di forecast
        metodi: iterabile con i metodi da eseguire
                 (valori supportati: 'holtwinters', 'pattern', 'naive')
    """
    if metodi is None:
        metodi = ('holtwinters', 'pattern', 'naive', 'sarima', 'prophet', 'tbats', 'intraday_dinamico')

    risultati = {}
    confronto_frames = []

    for metodo in metodi:
        metodo = metodo.lower()
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
            risultati[metodo] = _forecast_prophet(df, giorni_forecast, produce_outputs=False)
            if risultati[metodo] is not None:
                actual_path = _salva_forecast_excel(output_dir, 'forecast_prophet.xlsx', risultati[metodo])
                print(f"   Forecast Prophet salvato: {actual_path.name}")
        elif metodo == 'tbats':
            print(f"   Avvio TBATS...")
            risultati[metodo] = _forecast_tbats(df, giorni_forecast, produce_outputs=True)
            if risultati[metodo] is not None:
                actual_path = _salva_forecast_excel(output_dir, 'forecast_tbats.xlsx', risultati[metodo])
                print(f"   ✅ Forecast TBATS salvato: {actual_path.name}")
            else:
                print(f"   ⚠️  TBATS non generato (verifica messaggi sopra)")
        elif metodo == 'intraday_dinamico':
            print(f"   Avvio Forecast Intraday Dinamico...")
            risultati[metodo] = _forecast_intraday_dinamico(df, giorni_forecast, produce_outputs=True)
            if risultati[metodo] is not None:
                actual_path = _salva_forecast_excel(output_dir, 'forecast_intraday_dinamico.xlsx', risultati[metodo])
                print(f"   ✅ Forecast Intraday Dinamico salvato: {actual_path.name}")
            else:
                print(f"   ⚠️  Forecast Intraday Dinamico non generato (verifica messaggi sopra)")
        else:
            print(f"   Metodo forecast '{metodo}' non riconosciuto, ignorato.")
            risultati[metodo] = None

        result = risultati.get(metodo)
        if result is None:
            continue
        daily_df = result['giornaliero'][['DATA', 'FORECAST']].copy()
        daily_df.rename(columns={'FORECAST': metodo}, inplace=True)
        confronto_frames.append(daily_df)

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
        'intraday_dinamico': '#8B5CF6'
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
    plt.savefig(f'{output_dir}/confronto_modelli_forecast.png', dpi=300, bbox_inches='tight')
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


def valuta_modelli_forecast(df, output_dir, giorni_forecast=28, min_train_giorni=56, step_giorni=7):
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

    for cutoff in range(min_train, len(daily) - giorni_forecast + 1, step):
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
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
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
    
    with safe_excel_writer(file_path, engine='openpyxl') as (writer, actual_path):
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

# =============================================================================
# MAIN
# =============================================================================

def main(file_path=None, output_dir='output', giorni_forecast=28):
    """
    Esegue analisi completa
    
    Args:
        file_path: path del file Excel (None = ricerca automatica)
        output_dir: cartella output (default 'output')
        giorni_forecast: numero di giorni da prevedere nel forecast (default 28 = 4 settimane)
    """
    try:
        if file_path is None:
            file_path = trova_file_excel()
        
        script_dir = os.path.dirname(os.path.abspath(file_path))
        output_full_path = os.path.join(script_dir, output_dir)
        os.makedirs(output_full_path, exist_ok=True)
        
        print("\n" + "=" * 80)
        print("ANALISI COMPLETA TRAFFICO CALL CENTER")
        print("=" * 80)
        print(f"Giorni forecast: {giorni_forecast}")
        print()
        
        print("\n[1/16] Caricamento dati...")
        df = carica_dati(file_path)

        print("\n[2/16] Analisi fascia oraria...")
        fascia_stats = analisi_fascia_oraria(df, output_full_path)

        print("\n[3/16] Analisi giorno settimana...")
        giorno_stats = analisi_giorno_settimana(df, output_full_path)

        print("\n[4/16] Analisi settimana...")
        week_stats = analisi_settimana(df, output_full_path)

        print("\n[5/16] Analisi mese...")
        mese_stats = analisi_mese(df, output_full_path)

        print("\n[6/16] Heatmap...")
        crea_heatmap(df, output_full_path)

        print("\n[7/16] Curve previsionali...")
        curve = genera_curve_previsionali(df, output_full_path)

        print("\n[8/16] Trend storico...")
        daily_trend = analisi_consuntiva_trend(df, output_full_path)

        print("\n[9/16] Confronto periodi...")
        week_comp, month_comp = analisi_confronto_periodi(df, output_full_path)

        print("\n[10/16] Anomalie...")
        anomalie_alte, anomalie_basse = identifica_anomalie(df, output_full_path)

        print("\n[11/16] KPI...")
        kpi = dashboard_kpi_consuntivi(df, output_full_path)

        print("\n[12/16] Valutazione forecast (backtest Holt-Winters)...")
        valutazione = valuta_modelli_forecast(df, output_full_path, giorni_forecast=giorni_forecast)

        print("\n[13/16] Forecast multi-modello...")
        forecast_modelli = genera_forecast_modelli(
            df,
            output_full_path,
            giorni_forecast=giorni_forecast,
            metodi=('holtwinters', 'pattern', 'naive', 'sarima', 'prophet', 'tbats', 'intraday_dinamico')
        )
        forecast_completo = forecast_modelli.get('holtwinters')
        if forecast_completo is None:
            # usa il primo disponibile come fallback per i passi successivi
            forecast_completo = next(iter(forecast_modelli.values()))

        print("\n[14/16] Report statistico...")
        genera_report_statistico(df, fascia_stats, giorno_stats, week_stats, mese_stats, week_comp, month_comp, anomalie_alte, anomalie_basse, kpi, output_full_path)

        print("\n[15/16] Dashboard Excel...")
        excel_path = crea_dashboard_excel(df, fascia_stats, giorno_stats, week_stats, mese_stats, curve, forecast_completo['giornaliero'], kpi, output_full_path)

        print("\n[16/16] Report finale...")
        genera_report_finale(df, kpi, forecast_completo['giornaliero'], output_full_path)
        
        print("\n" + "=" * 80)
        print("COMPLETATO!")
        print("=" * 80)
        print(f"\nFile salvati in: {output_full_path}")
        print("\nFILE GENERATI:")
        print("\n  CURVE PREVISIONALI:")
        print("    - curva_fascia_oraria.png")
        print("    - curva_giorno_settimana.png")
        print("    - curva_settimana.png")
        print("    - curva_mese.png")
        print("    - heatmap_giorno_fascia.png")
        print("    - curve_previsionali.xlsx")
        print("\n  CONSUNTIVI:")
        print("    - analisi_trend_storico.png")
        print("    - confronto_periodi.png")
        print("    - identificazione_anomalie.png")
        print("    - kpi_consuntivi.txt")
        print("\n  FORECAST AVANZATO:")
        print("    - forecast_settimanale.png")
        print("    - forecast_giornaliero.png")
        print("    - forecast_intraday_esempio.png")
        print("    - forecast_completo.xlsx (3 livelli: settimanale, giornaliero, per fascia)")
        print("    - valutazione_forecast.xlsx (backtest Holt-Winters)")
        print("    - forecast_pattern.xlsx")
        print("    - forecast_naive.xlsx")
        print("    - forecast_sarima.xlsx (SARIMA con stagionalità settimanale)")
        print("    - forecast_prophet.xlsx (✨ NUOVO: Prophet con festività italiane)")
        print("    - forecast_tbats.xlsx (✨ NUOVO: TBATS multiple stagionalità)")
        print("    - forecast_intraday_dinamico.xlsx (✨ NUOVO: Forecast per fascia dinamico)")
        print("    - forecast_confronto_modelli.xlsx")
        print("    - confronto_modelli_forecast.png (✨ NUOVO: Grafico comparativo)")
        print("\n  DASHBOARD:")
        print("    - dashboard_completa.xlsx")
        print("    - report_statistico.txt")
        print("    - report_finale.txt")
        print("\n" + "=" * 80)
        
        return {'df': df, 'kpi': kpi, 'forecast': forecast_completo, 'valutazione': valutazione, 'forecast_modelli': forecast_modelli}
        
    except Exception as e:
        print("\nERRORE:")
        print(str(e))
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
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
    
    print(f"\n>>> FORECAST CONFIGURATO: {GIORNI_FORECAST} GIORNI <<<")
    print(f"    Equivalente a: {GIORNI_FORECAST/7:.1f} settimane")
    print(f"    Equivalente a: {GIORNI_FORECAST/30:.1f} mesi circa")
    print("=" * 80 + "\n")
    
    # Esegui analisi CON IL PARAMETRO CORRETTO
    print(f"Avvio analisi con forecast per {GIORNI_FORECAST} giorni...\n")
    risultati = main(giorni_forecast=GIORNI_FORECAST)
    
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









