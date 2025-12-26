
import pandas as pd

def check_demand():
    try:
        req = pd.read_excel("input wfmmacro multiskill.xlsx", sheet_name="Requisiti_dc")
        # Simplify names map
        # Columns probably: Fasce, Lun, Mar, ...
        # Need to find rows where 'Fasce' corresponds to 09:00-13:00 or overlaps
        # Fasce format is usually HH:MM-HH:MM
        
        # Normalize columns
        req.columns = [str(c).strip().lower() for c in req.columns]
        
        # Find 'fasce' col
        fasce_col = None
        for c in req.columns:
            if 'fasce' in c:
                fasce_col = c
                break
        
        if not fasce_col:
            print("Fasce column not found")
            return

        days_to_check = ['lun', 'ven', 'dom']
        # Also check worked days to compare
        days_worked = ['mar', 'mer', 'gio', 'sab']
        
        print("--- DEMAND CHECK FOR 09:00-13:00 (approx) ---")
        
        # Iterate rows
        for idx, row in req.iterrows():
            f = str(row[fasce_col]).strip()
            # Simple check if 09:00 is in the string or range
            # Assuming standard format like "09:00-09:15"
            # We want to see demand for slots 09:00 to 13:00.
            # Let's print rows that start between 09:00 and 12:45
            if not f or f.lower() == 'nan': continue
            
            # extract start hour
            try:
                start_str = f.split('-')[0].strip()
                # parse HH:MM
                hh, mm = map(int, start_str.split(':')[:2])
                minutes = hh*60 + mm
                
                if 9*60 <= minutes < 13*60:
                    # Print demand for target days
                    s = f"Slot {f}: "
                    has_demand = False
                    for d in days_to_check:
                        col_d = None
                        # find column matching day
                        for c in req.columns:
                            if d in c and 'ot' not in c:
                                col_d = c
                                break
                        val = row[col_d] if col_d else 0
                        s += f"{d.upper()}={val}  "
                        if val > 0: has_demand = True
                    
                    if has_demand:
                        pass # It has demand, so why not assigned?
                    else:
                        s += " [NO DEMAND]"
                    
                    # Also print worked days for comparison
                    s += " | Worked: "
                    for d in days_worked:
                        col_d = None
                         # find column matching day
                        for c in req.columns:
                            if d in c and 'ot' not in c: # exclude partial matches if any
                                col_d = c
                                break
                            # try exact match
                            if d == c:
                                col_d = c
                                break
                        val = row[col_d] if col_d else 0
                        s += f"{d.upper()}={val} "

                    print(s)
            except:
                continue

    except Exception as e:
        print(e)

if __name__ == "__main__":
    import sys
    with open("demand_check.txt", "w") as f:
        sys.stdout = f
        check_demand()
        sys.stdout = sys.__stdout__
    print("Output written to demand_check.txt")
