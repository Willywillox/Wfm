#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build script per creare l'eseguibile WFM Turni Generator
Usa PyInstaller per creare un .exe standalone
"""

import subprocess
import sys
import os
from pathlib import Path

def check_pyinstaller():
    """Check if PyInstaller is installed"""
    try:
        import PyInstaller
        print(f"✓ PyInstaller {PyInstaller.__version__} trovato")
        return True
    except ImportError:
        print("✗ PyInstaller non trovato!")
        print("\nInstallalo con:")
        print("  pip install pyinstaller")
        return False

def build_exe():
    """Build the executable"""
    if not check_pyinstaller():
        return False

    print("\n" + "="*60)
    print("BUILDING WFM TURNI GENERATOR EXECUTABLE")
    print("="*60 + "\n")

    # PyInstaller command
    cmd = [
        "pyinstaller",
        "--name=WFM_Turni_Generator",
        "--onefile",                    # Single exe file
        "--windowed",                   # No console window (GUI mode)
        "--icon=NONE",                  # Add icon if you have one
        "--add-data=wfm_claudegit6.py:.",  # Include the main script
        "--clean",                      # Clean cache
        "--noconfirm",                  # Overwrite without asking
        "wfm_gui.py"
    ]

    # On Windows, adjust add-data separator
    if sys.platform == "win32":
        cmd[6] = "--add-data=wfm_claudegit6.py;."

    print(f"Comando: {' '.join(cmd)}\n")

    try:
        result = subprocess.run(cmd, check=True)

        print("\n" + "="*60)
        print("✓ BUILD COMPLETATO CON SUCCESSO!")
        print("="*60)
        print(f"\nL'eseguibile si trova in: dist/WFM_Turni_Generator.exe")
        print("\nPuoi distribuire questo file senza bisogno di Python installato!")

        return True

    except subprocess.CalledProcessError as e:
        print("\n" + "="*60)
        print("✗ ERRORE DURANTE IL BUILD")
        print("="*60)
        print(f"\n{e}")
        return False

def main():
    """Main function"""
    print("WFM Turni Generator - Build Script")
    print("=" * 60)

    # Check we're in the right directory
    if not Path("wfm_gui.py").exists():
        print("✗ Errore: wfm_gui.py non trovato!")
        print("  Esegui questo script dalla directory del progetto.")
        return 1

    if not Path("wfm_claudegit6.py").exists():
        print("⚠ Warning: wfm_claudegit6.py non trovato!")
        print("  L'exe potrebbe non funzionare senza il file principale.")
        response = input("Continuare comunque? (s/n): ")
        if response.lower() != 's':
            return 1

    success = build_exe()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
