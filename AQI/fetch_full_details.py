import pandas as pd
import requests
import os
import concurrent.futures
import time

# --- CONFIGURATION ---
INDEX_DIR = "data/stations/historical_index"   # Input (Stage 1)
FINAL_DIR = "data/stations/historical_full"    # Output (New Files)
MAX_WORKERS = 8  # Safe number for parallel requests

os.makedirs(FINAL_DIR, exist_ok=True)

def fetch_single_detail(row_dict):
    """
    Worker function: Takes a dictionary (row), fetches details, returns NEW dictionary.
    """
    record_id = row_dict.get('Record_ID')
    
    # Create a copy so we don't mess with original data
    new_row = row_dict.copy()

    # Default values for new columns
    new_row['Temperature'] = ""
    new_row['Humidity'] = ""
    new_row['Wind Speed'] = ""
    new_row['Wind Direction'] = ""
    new_row['Pressure'] = ""
    new_row['Radiation'] = ""
    new_row['Detailed_Status'] = "Pending"

    # Skip if ID is missing
    if pd.isna(record_id) or str(record_id) == "nan" or str(record_id) == "":
        new_row['Detailed_Status'] = "Skipped_NoID"
        return new_row

    # URL Construction
    url = f"https://tedp.vn/api/data_hour/{record_id}"

    try:
        resp = requests.get(url, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json().get('data', {})
            
            # Helper to find keys
            def get_val(*keys):
                for k in keys:
                    if k in data and data[k] is not None:
                        return data[k]
                return ""

            # FILL NEW DATA (Vietnamese keys prepended — see analysis/crawler_audit/)
            new_row['Temperature']    = get_val('Nhiệt độ', 'Temperature', 'Temp')
            new_row['Humidity']       = get_val('Độ ẩm', 'Humidity', 'RH', 'Humid')
            new_row['Wind Speed']     = get_val('Tốc độ gió', 'Wind Speed', 'WindSpd')
            new_row['Wind Direction'] = get_val('Hướng gió', 'Wind Direction', 'WinDir')
            new_row['Pressure']       = get_val('Áp suất khí quyển', 'Pressure', 'Barometer')
            new_row['Radiation']      = get_val('Bức xạ', 'Radiation')
            
            new_row['Detailed_Status'] = 'Done'
        
        elif resp.status_code == 429:
            new_row['Detailed_Status'] = 'RateLimit'
            time.sleep(2)
        else:
            new_row['Detailed_Status'] = f'Error_{resp.status_code}'

    except Exception:
        new_row['Detailed_Status'] = 'Error_Conn'
    
    return new_row

def process_station_file(filename):
    in_path = os.path.join(INDEX_DIR, filename)
    out_path = os.path.join(FINAL_DIR, filename)
    
    # Check if we already finished this station to skip it
    if os.path.exists(out_path):
        # Optional: Check if it's fully done, but for now let's just assume 
        # if the file exists, we might want to skip or resume. 
        # To be safe, let's just skip "Completed" files.
        print(f"⏭️  Skipping {filename} (File exists)")
        return

    print(f"⚡ Processing {filename}...")
    
    # Read the Index File
    try:
        df_index = pd.read_csv(in_path)
    except Exception:
        print(f"❌ Error reading {filename}")
        return

    # Convert to list of dicts (Easier to handle than DataFrame rows)
    rows_to_process = df_index.to_dict('records')
    
    final_rows = []
    processed_count = 0
    
    # Start Parallel Processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        futures = [executor.submit(fetch_single_detail, row) for row in rows_to_process]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result_row = future.result()
                final_rows.append(result_row)
                processed_count += 1
                
                # Progress Indicator
                if processed_count % 500 == 0:
                    print(f"   {filename}: {processed_count}/{len(rows_to_process)} done...")
                    
            except Exception as e:
                print(f"   Thread Error: {e}")

    # Create the NEW DataFrame and Save
    if final_rows:
        df_final = pd.DataFrame(final_rows)
        # Ensure columns are in a nice order
        cols = ['Timestamp', 'Station_ID', 'Record_ID', 
                'PM2.5', 'PM10', 'CO', 'NO2', 
                'Temperature', 'Humidity', 'Wind Speed', 'Wind Direction', 'Pressure', 'Radiation', 
                'Detailed_Status']
        
        # Reorder if keys exist, ignore if they don't
        existing_cols = [c for c in cols if c in df_final.columns]
        df_final = df_final[existing_cols]
        
        df_final.to_csv(out_path, index=False)
        print(f"🎉 Saved {out_path} ({len(df_final)} records)")
    else:
        print(f"⚠️ No data resulted for {filename}")

def main():
    files = sorted(os.listdir(INDEX_DIR))
    for f in files:
        if f.endswith(".csv"):
            process_station_file(f)

if __name__ == "__main__":
    main()