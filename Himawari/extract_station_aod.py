import sys
import os
import glob
import pandas as pd
import rasterio
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed

# Configuration paths
STATIONS_FILE = "/home/work1/projects/Air_Quality/AOD data/target_stations_aod.csv"
OUTPUT_DIR = "/home/slow_data/Air_Quality/AOD/station_aod_2_years"
TARGET_COLUMNS = ["AOT", "Uncertainty", "AE", "QA_flag", "SSA", "RF"]

def sanitize_filename(name):
    """Removes invalid characters to create safe filenames."""
    return str(name).replace(":", "").replace(" ", "_").replace("/", "_").replace('"', '')

def process_single_file(aod_file, stations_df):
    """Extracts all raw band values for target stations from a single GeoTIFF."""
    try:
        # Extract and format timestamp (Assuming format: prefix_prefix_prefix_prefix_YYYYMMDD_HHMM.tif)
        filename = os.path.basename(aod_file)
        parts = filename.split("_")
        date_str = parts[4]
        time_str = parts[5].replace(".tif", "")
        
        # Format to 'YYYY-MM-DD HH:MM:SS'
        if len(date_str) == 8 and len(time_str) >= 4:
            timestamp = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} {time_str[:2]}:{time_str[2:4]}:00"
        else:
            timestamp = f"{date_str}_{time_str}"

        # Initialize results container for this file
        file_results = {}

        with rasterio.open(aod_file) as src:
            all_bands = src.read()
            num_bands = src.count
            
            for _, row in stations_df.iterrows():
                lon, lat = row["longitude"], row["latitude"]
                station_id = sanitize_filename(row["stationId"])
                
                station_record = {'timestamp': timestamp}
                
                try:
                    rowcol = src.index(lon, lat)
                    r, c = rowcol[0], rowcol[1]
                    
                    # Map each band to its respective column name
                    for i, col_name in enumerate(TARGET_COLUMNS):
                        if i < num_bands:
                            val = all_bands[i, r, c]
                            station_record[col_name] = np.nan if np.isnan(val) else float(val)
                        else:
                            # Fill with NaN if the GeoTIFF has fewer bands than requested columns
                            station_record[col_name] = np.nan
                            
                except Exception:
                    # Handle coordinates falling outside the raster extent
                    for col_name in TARGET_COLUMNS:
                        station_record[col_name] = np.nan
                        
                file_results[station_id] = station_record
                
        return file_results
    except Exception as e:
        print(f"Error processing {os.path.basename(aod_file)}: {e}")
        return None

def main(target_directory):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stations_df = pd.read_csv(STATIONS_FILE)
    
    # Identify unique, sanitized station names
    stations_df['safe_id'] = stations_df['stationId'].apply(sanitize_filename)
    unique_stations = stations_df['safe_id'].unique()
    
    search_path = os.path.join(target_directory, "**", "*.tif")
    aod_files = glob.glob(search_path, recursive=True)
    
    if not aod_files:
        print(f"No .tif files found in {target_directory}.")
        return

    print(f"Found {len(aod_files)} files. Starting parallel extraction with 8 workers...")
    
    # Initialize dictionary to hold row data for each station
    station_data = {station: [] for station in unique_stations}
    
    processed_count = 0
    with ProcessPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_single_file, f, stations_df): f for f in aod_files}
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                for station_id, record in result.items():
                    if station_id in station_data:
                        station_data[station_id].append(record)
                    
            processed_count += 1
            if processed_count % 500 == 0:
                print(f"Processed {processed_count}/{len(aod_files)} files...")

    print("Extraction complete. Saving individual station CSV files...")
    
    # Compile and export results per station
    for station_id, records in station_data.items():
        if not records:
            continue
            
        new_df = pd.DataFrame(records)
        output_csv = os.path.join(OUTPUT_DIR, f"{station_id}.csv")
        
        # Append to existing historical data if present
        if os.path.exists(output_csv):
            df_old = pd.read_csv(output_csv)
            df_final = pd.concat([df_old, new_df], ignore_index=True)
            df_final = df_final.drop_duplicates(subset=['timestamp'])
        else:
            df_final = new_df
            
        # Ensure correct column order and chronological sorting
        cols = ['timestamp'] + TARGET_COLUMNS
        df_final = df_final[cols]
        df_final = df_final.sort_values('timestamp').reset_index(drop=True) 
        
        df_final.to_csv(output_csv, index=False)
        print(f"Saved data for {station_id} -> {os.path.basename(output_csv)}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_station_aod.py <target_directory_path>")
        sys.exit(1)
        
    target_dir = sys.argv[1]
    main(target_dir)