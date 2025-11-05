@echo off
REM Avvia WFM Turni Generator GUI
REM Questo script verifica che Python sia installato e avvia la GUI

echo ========================================
echo WFM Turni Generator - GUI
echo ========================================
echo.

REM Check Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERRORE: Python non trovato!
    echo.
    echo Scarica Python da: https://www.python.org/downloads/
    echo Assicurati di selezionare "Add Python to PATH" durante l'installazione
    pause
    exit /b 1
)

REM Check if dependencies are installed
echo Controllo dipendenze...
pip show pandas >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo Installazione dipendenze...
    pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo.
        echo ERRORE: Impossibile installare le dipendenze
        pause
        exit /b 1
    )
)

REM Run GUI
echo.
echo Avvio GUI...
echo.
python wfm_gui.py

if %errorlevel% neq 0 (
    echo.
    echo ERRORE durante l'esecuzione della GUI
    pause
)
