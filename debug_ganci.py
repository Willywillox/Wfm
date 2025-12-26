
import pandas as pd
import traceback

def normalize_name(n):
    if pd.isna(n): return ""
    return str(n).strip().lower()

def inspect_data():
    input_file = "input wfmmacro multiskill.xlsx"
    target_name = "ganci elvira"
    
    print(f"--- INSPECTING INPUT: {input_file} ---")
    try:
        ris = pd.read_excel(input_file, sheet_name='Risorse')
        print(f"Loaded Risorse. Columns: {list(ris.columns)}")
        name_col = None
        # Prioritize explicit 'Nome' column
        if 'Nome' in ris.columns:
            name_col = 'Nome'
        else:
             for c in ris.columns:
                if 'nome' in str(c).lower() or 'dipendente' in str(c).lower():
                    name_col = c
                    break
        
        if name_col:
            print(f"Using name column: {name_col}")
            # Filter
            mask = ris[name_col].apply(lambda x: target_name in normalize_name(x))
            target_row = ris[mask]
            if not target_row.empty:
                print("Found Entry in Risorse:")
                row = target_row.iloc[0]
                # Print key columns - adjust for lowercase in actual file
                cols_to_check = ['id dipendente', 'Nome', 'Ore', 'combinazione', 'Skill', 'Riposi', 'Inizio fascia', 'Fine fascia', 'sabato', 'domenica']
                
                if len(ris.columns) >= 4:
                    print(f"  Column D (Riposi implied): {ris.iloc[target_row.index[0], 3]}")
                
                for c in cols_to_check:
                    if c in ris.columns:
                        print(f"  {c}: {row[c]}")
                    elif c.capitalize() in ris.columns:
                        print(f"  {c.capitalize()}: {row[c.capitalize()]}")
            else:
                print(f"Name '{target_name}' not found in Risorse.")
                print("First 5 names:", ris[name_col].head().tolist())
        else:
            print("Name column not found.")

    except Exception:
        print("Error reading input:")
        traceback.print_exc()

    print(f"\n--- INSPECTING OUTPUT: prova2.xlsx ---")
    try:
        ass = pd.read_excel("prova2.xlsx", sheet_name='Assegnazioni')
        print(f"Loaded Assegnazioni. Columns: {list(ass.columns)}")
        
        mask_out = pd.Series([False]*len(ass))
        for col in ass.columns:
            if 'operatore' in str(col).lower() or 'dipendente' in str(col).lower() or 'cf' in str(col).lower():
                 mask_out |= ass[col].apply(lambda x: target_name in normalize_name(x))
        
        target_shifts = ass[mask_out]
        if not target_shifts.empty:
            print(f"Found {len(target_shifts)} shifts assigned:")
            # Print simplified view
            print(target_shifts[['Giorno', 'Fascia_Ordinario', 'TurnoID']].to_string(index=False))
            
            worked_days = target_shifts['Giorno'].unique()
            print(f"Days worked: {list(worked_days)}")
        else:
            print(f"No shifts found for '{target_name}' in output.")

    except PermissionError:
        print("PERMISSION DENIED")
    except Exception:
        print("Error reading output:")
        traceback.print_exc()

    # Check Sheets
    try:
        xl = pd.ExcelFile(input_file)
        print("\n--- SHEETS IN INPUT FILE ---")
        print(xl.sheet_names)
    except:
        pass

if __name__ == "__main__":
    import sys
    with open("debug_output.txt", "w") as f:
        sys.stdout = f
        inspect_data()
        sys.stdout = sys.__stdout__
    print("Debug written to debug_output.txt")
