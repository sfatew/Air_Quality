import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
import os
import time

# --- CONFIGURATION ---
INPUT_MAP = "/home/work1/projects/Air_Quality/Masterdata/AERONET_sites.csv"
OUTPUT_DIR = "/home/slow_data/Air_Quality/weather"
START_DATE = "2023-01-01"
END_DATE = "2026-04-20"

os.makedirs(OUTPUT_DIR, exist_ok=True)

cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


def needs_fetch(file_path):
    """Check if file needs (re-)fetching: missing, old UTC format, or wrong date range."""
    if not os.path.exists(file_path):
        return True, "missing"
    try:
        df = pd.read_csv(file_path, nrows=2)
        ts0 = str(df['Timestamp'].iloc[0])
        if '+00:00' in ts0:
            return True, "old UTC format"
        if len(df) < 2:
            return True, "too few rows"
    except Exception:
        return True, "unreadable"
    return False, "ok"


def fetch_one(s_name, lat, lon, file_path):
    """Fetch weather for one station. Returns True on success."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "surface_pressure",
            "wind_speed_10m",
            "wind_direction_10m",
            "boundary_layer_height",
            "visibility",
            "cloud_cover"
        ],
        "timezone": "Asia/Bangkok"
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    hourly = response.Hourly()
    hourly_data = {"Timestamp": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )}

    hourly_data["Temperature"] = hourly.Variables(0).ValuesAsNumpy()
    hourly_data["Humidity"]    = hourly.Variables(1).ValuesAsNumpy()
    hourly_data["Pressure"]    = hourly.Variables(2).ValuesAsNumpy()
    hourly_data["Wind Speed"]  = hourly.Variables(3).ValuesAsNumpy()
    hourly_data["Wind Direction"] = hourly.Variables(4).ValuesAsNumpy()
    hourly_data["PBLH"]       = hourly.Variables(5).ValuesAsNumpy()
    hourly_data["Visibility"] = hourly.Variables(6).ValuesAsNumpy()
    hourly_data["Cloud Cover"] = hourly.Variables(7).ValuesAsNumpy()
    weather_df = pd.DataFrame(data=hourly_data)

    # Convert UTC timestamps to Vietnam time (UTC+7), strip timezone info
    weather_df["Timestamp"] = weather_df["Timestamp"] + pd.Timedelta(hours=7)
    weather_df["Timestamp"] = weather_df["Timestamp"].dt.tz_localize(None)

    weather_df["Weather_Source"] = "Open-Meteo_ERA5_Proxies"
    weather_df.to_csv(file_path, index=False)
    return len(weather_df)


def fetch_weather_proxies():
    stations = pd.read_csv(INPUT_MAP)
    n_total = len(stations)
    n_done = 0
    n_skip = 0
    n_fail = 0
    print(f"Fetching Weather + PROXIES for {n_total} stations ({START_DATE} to {END_DATE})...")

    for index, row in stations.iterrows():
        s_name = row['stationName']
        lat = row['latitude']
        lon = row['longitude']

        clean_name = s_name.replace(':', '').replace('/', '_').replace('\\', '_')
        file_path = os.path.join(OUTPUT_DIR, f"weather_{clean_name}.csv")

        need, reason = needs_fetch(file_path)
        if not need:
            n_skip += 1
            continue

        max_retries = 5
        for attempt in range(max_retries):
            try:
                n_rows = fetch_one(s_name, lat, lon, file_path)
                print(f"[{index+1}/{n_total}] {s_name[:60]} — {n_rows} rows ({reason})")
                n_done += 1
                time.sleep(3)
                break
            except Exception as e:
                err = str(e)
                if 'Minutely' in err:
                    wait = 65
                elif 'Hourly' in err:
                    wait = 600
                else:
                    print(f"[{index+1}/{n_total}] FAIL {s_name[:50]}: {err[:80]}")
                    n_fail += 1
                    time.sleep(5)
                    break

                if attempt < max_retries - 1:
                    print(f"  Rate limited — waiting {wait}s (attempt {attempt+1}/{max_retries})...")
                    time.sleep(wait)
                else:
                    print(f"[{index+1}/{n_total}] FAIL after {max_retries} retries: {s_name[:50]}")
                    n_fail += 1

    print(f"\nDone: {n_done} fetched, {n_skip} skipped, {n_fail} failed")


if __name__ == "__main__":
    fetch_weather_proxies()
