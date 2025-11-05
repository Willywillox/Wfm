#!/bin/bash
# Avvia WFM Turni Generator GUI
# Questo script verifica che Python sia installato e avvia la GUI

echo "========================================"
echo "WFM Turni Generator - GUI"
echo "========================================"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "ERRORE: Python3 non trovato!"
    echo ""
    echo "Installa Python3 con:"
    echo "  Ubuntu/Debian: sudo apt install python3 python3-pip"
    echo "  Mac: brew install python3"
    exit 1
fi

echo "Python version:"
python3 --version
echo ""

# Check if dependencies are installed
echo "Controllo dipendenze..."
if ! python3 -c "import pandas" &> /dev/null; then
    echo ""
    echo "Installazione dipendenze..."
    pip3 install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo ""
        echo "ERRORE: Impossibile installare le dipendenze"
        exit 1
    fi
fi

# Run GUI
echo ""
echo "Avvio GUI..."
echo ""
python3 wfm_gui.py

if [ $? -ne 0 ]; then
    echo ""
    echo "ERRORE durante l'esecuzione della GUI"
    exit 1
fi
