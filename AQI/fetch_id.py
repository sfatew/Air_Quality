import requests
import pandas as pd
import os
import time

# --- CONFIGURATION ---
INPUT_MAP = "data/stations/metadata/envisoft_station_map.csv"
OUTPUT_DIR = "data/stations/historical_index" # New folder for Stage 1
START_DATE = "2024-01-01T00:00:00"
END_DATE = "2025-12-31T23:59:59"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_index():
    stations = pd.read_csv(INPUT_MAP)
    print(f"🚀 Starting Indexing for {len(stations)} stations...")

    for index, row in stations.iterrows():
        s_id = str(row['stationId'])
        s_name = row['stationName']
        clean_name = s_name.replace(':', '').replace('/', '_').replace('\\', '_')
        file_path = os.path.join(OUTPUT_DIR, f"{clean_name}.csv")
        
        if os.path.exists(file_path):
            continue # Skip if done

        print(f"📡 Indexing: {s_name}...")
        url = "https://tedp.vn/api/data_hour/search/findByStationIdAndGetTimeBetweenOrderByGetTimeDesc"
        params = {"stationId": s_id, "from": START_DATE, "to": END_DATE}

        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code != 200:
                print(f"   ⚠️ Status {r.status_code}")
                continue

            data = r.json()
            records = data.get('_embedded', {}).get('data_hour', [])
            
            processed_data = []
            for item in records:
                # --- THE MAGIC KEY: EXTRACT ID ---
                # Link is: http://tedp.vn:8000/api/data_hour/6955...
                raw_link = item.get('_links', {}).get('self', {}).get('href', '')
                # We split by '/' and take the last part to get "6955..."
                record_id = raw_link.split('/')[-1] if raw_link else ""
                
                measurements = item.get('data', {})

                processed_data.append({
                    "Timestamp": item.get('getTime'),
                    "Record_ID": record_id, # <--- We need this for Stage 2
                    "PM2.5": measurements.get('PM-2-5') or measurements.get('PM2.5'),
                    "PM10":  measurements.get('PM-10'),
                    "CO":    measurements.get('CO'),
                    "NO2":   measurements.get('NO2'),
                    "Detailed_Status": "Pending" # Marker for Stage 2
                })

            if processed_data:
                pd.DataFrame(processed_data).to_csv(file_path, index=False)
                print(f"   ✅ Indexed {len(processed_data)} records.")

        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        time.sleep(1) # Safety delay

if __name__ == "__main__":
    fetch_index()