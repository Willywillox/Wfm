# WFM Turni Generator - GUI Edition

Interfaccia grafica per la generazione automatica dei turni di lavoro.

## 🚀 Quick Start

### Opzione 1: Eseguire con Python

```bash
# Installa le dipendenze
pip install -r requirements.txt

# Avvia la GUI
python wfm_gui.py
```

### Opzione 2: Creare un EXE (Windows)

```bash
# Installa PyInstaller (se non già installato)
pip install pyinstaller

# Esegui lo script di build
python build_exe.py
```

L'eseguibile sarà creato in `dist/WFM_Turni_Generator.exe`

**Oppure manualmente:**

```bash
pyinstaller --name=WFM_Turni_Generator --onefile --windowed --add-data="wfm_claudegit6.py:." wfm_gui.py
```

Su Windows usa `;` invece di `:` in add-data:
```bash
pyinstaller --name=WFM_Turni_Generator --onefile --windowed --add-data="wfm_claudegit6.py;." wfm_gui.py
```

## 📋 Come Usare la GUI

1. **Seleziona File Input**: Clicca "Browse..." per selezionare il file Excel di input (`.xlsm` o `.xlsx`)

2. **Seleziona File Output**: Scegli dove salvare il risultato

3. **Configura Parametri**:
   - **Grid Step**: Intervallo slot in minuti (15, 30, o 60)
   - **Prefer Phase**: Minuti preferiti per l'inizio turni (es. `0,15,45`)
   - **Overcap**: Limite sovraccapacità (opzionale)
   - **Overcap Penalty**: Penalità per sovraccapacità (opzionale)

4. **Opzioni**:
   - ☑ **Strict Phase Mode**: Forza i turni ai minuti preferiti
   - ☑ **Force Overtime**: Forza straordinari se necessario
   - ☑ **Force Balance**: Forza bilanciamento tra giorni

5. **Genera Turni**: Clicca il pulsante verde "GENERA TURNI"

6. **Controlla il Log**: Segui il progresso nell'area log in basso

## 📁 File Necessari

Per far funzionare la GUI servono:
- `wfm_gui.py` - Interfaccia grafica
- `wfm_claudegit6.py` - Script principale di generazione turni

## 🎯 Caratteristiche

✅ Interfaccia user-friendly
✅ File picker integrato
✅ Tutti i parametri configurabili
✅ Log in tempo reale
✅ Progress bar
✅ Validazione input
✅ Messaggi di errore chiari
✅ Esportabile come .exe standalone

## 🛠 Troubleshooting

### "Script WFM non trovato"
Assicurati che `wfm_claudegit6.py` sia nella stessa cartella di `wfm_gui.py`

### L'exe non si avvia
- Controlla che tutte le dipendenze siano incluse nel build
- Prova a ricostruire con `--onedir` invece di `--onefile` per debug

### Errori durante la generazione
Controlla il log nella GUI per dettagli. Assicurati che:
- Il file di input sia valido
- Il formato Excel sia corretto
- I parametri siano nei range validi

## 📦 Distribuzione

Per distribuire l'applicazione:

1. **Build dell'exe**: Esegui `python build_exe.py`
2. **Testa l'exe**: Prova `dist/WFM_Turni_Generator.exe`
3. **Distribuisci**: Copia solo il file `.exe` - non serve Python installato!

## 🔧 Personalizzazione

Puoi modificare:
- `wfm_gui.py` per cambiare l'interfaccia
- `wfm_claudegit6.py` per modificare la logica di generazione
- `build_exe.py` per aggiungere icone o risorse

## 📝 Note

- L'exe funziona solo su Windows
- Per Mac/Linux usa direttamente Python
- L'exe sarà di dimensioni ~100MB (include Python e dipendenze)

## ❓ Supporto

Per problemi o domande, controlla il log della GUI o il file di output generato.
