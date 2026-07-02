"""
Build daily AOD × PM2.5 pairs for the §8.2.6 / RQ4 analysis.

Products (one row per station per date per product):
    hima_only        Stage_A/gridded    AOD_himawari_l2
    stage_a_merged   Stage_A/merged     AOD_merged
    b1_st_kriging    Stage_B/output/st_kriging   aod_550nm
    b2_rf            Stage_B/output/rf           aod_550nm
    b3_rf_rk         Stage_B/output/rf_rk        aod_550nm

Physics normalization (v3.5.2 §8.2.6, γ=0.6, PBLH floor 50 m; ERA5 fields
already precomputed into the Stage_A merged files):
    AOD_phys = AOD * (1 - RH/100)^0.6 / max(PBLH, 50)

Output: parquet at output/pm25_pairs_daily.parquet with columns
    date, stationName, region, product, aod_daily, aod_phys_daily,
    slot_count, pm25_daily.

Also writes the filtered station list (>=85% PM2.5 completeness) alongside.

Time window: 2025-01-01 through 2026-04-30 (envisoft is local UTC+7; the AOD
grids are UTC, but the daily rollup is by-date in the same local calendar the
PM2.5 stations use so we align by local date at merge time).
"""
from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
from datetime import timedelta
import warnings
warnings.filterwarnings('ignore', category=xr.SerializationWarning)

# ---------- paths ----------
DATA_ROOT = Path('/home/slow_data/Air_Quality')
STAGE_A   = DATA_ROOT / 'Stage_A'
STAGE_B   = DATA_ROOT / 'Stage_B'
AQI_DIR   = DATA_ROOT / 'AQI/process'
STATION_MAP = Path('/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv')

OUT_DIR = Path(__file__).parent / 'output'
OUT_DIR.mkdir(exist_ok=True)

# ---------- time window ----------
WINDOW_START = pd.Timestamp('2025-01-01')
WINDOW_END   = pd.Timestamp('2026-05-01')   # exclusive
LOCAL_TZ_H   = 7                            # Vietnam = UTC+7

# ---------- physics ----------
GAMMA        = 0.6
PBLH_FLOOR_M = 50.0

# ---------- rollup policy ----------
# Minimum number of valid slot samples in a day for that day's AOD_daily to
# count. Plan says "recorded so downstream consumers can apply their own
# floor" — 3 is the conservative default: at least 90 min of coverage.
MIN_SLOTS_PER_DAY = 3

# ---------- product manifest ----------
PRODUCTS = {
    'hima_only':      (STAGE_A / 'gridded',                'gridded_',  'AOD_himawari_l2'),
    'stage_a_merged': (STAGE_A / 'merged',                 'merged_',   'AOD_merged'),
    'b1_st_kriging':  (STAGE_B / 'output' / 'st_kriging',  'aod_',      'aod_550nm'),
    'b2_rf':          (STAGE_B / 'output' / 'rf',          'aod_',      'aod_550nm'),
    'b3_rf_rk':       (STAGE_B / 'output' / 'rf_rk',       'aod_',      'aod_550nm'),
}

# ------------------------------------------------------------
# 1. Stations that pass the ≥85% PM2.5 completeness filter
# ------------------------------------------------------------
def find_aqi_csv(name: str) -> Path | None:
    p = AQI_DIR / f"{name.replace(':', '')}.csv"
    if p.exists():
        return p
    target = name.replace(':', '').strip()
    for c in AQI_DIR.glob('*.csv'):
        if c.stem.strip() == target:
            return c
    return None

def pm25_series(csv: Path) -> pd.Series:
    d = pd.read_csv(csv, usecols=['datetime', 'pm2.5'], parse_dates=['datetime'])
    d = d.dropna(subset=['pm2.5']).drop_duplicates('datetime')
    d = d.set_index('datetime')['pm2.5'].sort_index()
    return d

def select_stations() -> pd.DataFrame:
    env = pd.read_csv(STATION_MAP)
    total_hours = int((WINDOW_END - WINDOW_START) / pd.Timedelta('1h'))
    out = []
    for _, r in env.iterrows():
        csv = find_aqi_csv(r['stationName'])
        if csv is None:
            continue
        s = pm25_series(csv)
        s = s[(s.index >= WINDOW_START) & (s.index < WINDOW_END)]
        frac = len(s) / total_hours
        if frac >= 0.85:
            out.append({**r.to_dict(), 'completeness': frac, 'csv': str(csv)})
    df = pd.DataFrame(out).sort_values('completeness', ascending=False).reset_index(drop=True)
    # Region → North/Central/South (v3.5.2 convention)
    df['region3'] = df['region'].map({
        'NE': 'North', 'NW': 'North', 'RRD': 'North',
        'CH': 'Central', 'SCC': 'Central',
        'SE': 'South',
    })
    return df

# ------------------------------------------------------------
# 2. Locate each station in the 0.05° AOD grid
# ------------------------------------------------------------
def load_grid_reference() -> tuple[np.ndarray, np.ndarray]:
    # Any per-slot file will do; use one known-present slot.
    ref = STAGE_A / 'merged/2025/01/01/merged_20250101_0100.nc'
    with xr.open_dataset(ref) as ds:
        return ds['lat'].values.copy(), ds['lon'].values.copy()

def station_cell_indices(stations: pd.DataFrame, lats: np.ndarray, lons: np.ndarray) -> pd.DataFrame:
    ilat = np.abs(lats[None, :] - stations['latitude'].values[:, None]).argmin(axis=1)
    ilon = np.abs(lons[None, :] - stations['longitude'].values[:, None]).argmin(axis=1)
    out = stations.copy()
    out['ilat'] = ilat
    out['ilon'] = ilon
    return out

# ------------------------------------------------------------
# 3. Slot iterator + per-slot extraction
# ------------------------------------------------------------
def slot_path(product_key: str, ts: pd.Timestamp) -> Path:
    root, prefix, _ = PRODUCTS[product_key]
    return root / f"{ts:%Y}" / f"{ts:%m}" / f"{ts:%d}" / f"{prefix}{ts:%Y%m%d_%H%M}.nc"

def merged_path(ts: pd.Timestamp) -> Path:
    return slot_path('stage_a_merged', ts)

def daterange(start: pd.Timestamp, end: pd.Timestamp):
    d = start.normalize()
    end = end.normalize()
    while d < end:
        yield d
        d += pd.Timedelta('1D')

def extract_slot(ts: pd.Timestamp, cells: pd.DataFrame) -> list[dict]:
    """Return one record per (product, station) for this slot.

    Skips products whose per-slot NC is missing. ERA5 RH/PBLH are read
    from the merged file (if present) and reused for every product.
    """
    ilat = cells['ilat'].to_numpy()
    ilon = cells['ilon'].to_numpy()
    names = cells['stationName'].to_numpy()

    # ERA5 shared across products (from merged file when available)
    rh = pblh = None
    mpath = merged_path(ts)
    if mpath.exists():
        with xr.open_dataset(mpath) as ds:
            if 'ERA5_RH' in ds.data_vars:
                rh = ds['ERA5_RH'].values[ilat, ilon]
            if 'ERA5_PBLH' in ds.data_vars:
                pblh = ds['ERA5_PBLH'].values[ilat, ilon]

    records = []
    for prod, (_, _, var) in PRODUCTS.items():
        p = slot_path(prod, ts)
        if not p.exists():
            continue
        with xr.open_dataset(p) as ds:
            if var not in ds.data_vars:
                continue
            aod = ds[var].values[ilat, ilon]
        # Physics normalization at the slot level.
        if rh is not None and pblh is not None:
            pblh_c = np.maximum(pblh, PBLH_FLOOR_M)
            rh_c   = np.clip(rh, 0.0, 99.0)  # avoid negative under (1-rh/100)
            aod_phys = aod * (1 - rh_c / 100.0) ** GAMMA / pblh_c
        else:
            aod_phys = np.full_like(aod, np.nan, dtype=float)
        for i, name in enumerate(names):
            a = aod[i]
            if not np.isfinite(a):
                continue
            records.append({
                'ts': ts, 'product': prod, 'stationName': name,
                'aod': float(a),
                'aod_phys': float(aod_phys[i]) if np.isfinite(aod_phys[i]) else np.nan,
            })
    return records

# ------------------------------------------------------------
# 4. Daily rollup for each (product, station)
# ------------------------------------------------------------
def rollup_daily(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame.from_records(records)
    if df.empty:
        return df
    # UTC timestamps → local (UTC+7) date so daily join with PM2.5 aligns.
    df['date'] = (df['ts'] + pd.Timedelta(hours=LOCAL_TZ_H)).dt.normalize()
    g = df.groupby(['product', 'stationName', 'date'], sort=False)
    daily = g.agg(
        aod_daily      = ('aod', 'mean'),
        aod_phys_daily = ('aod_phys', 'mean'),
        slot_count     = ('aod', 'size'),
        phys_slot_count= ('aod_phys', lambda s: int(np.isfinite(s).sum())),
    ).reset_index()
    daily.loc[daily['slot_count'] < MIN_SLOTS_PER_DAY, ['aod_daily']] = np.nan
    daily.loc[daily['phys_slot_count'] < MIN_SLOTS_PER_DAY, ['aod_phys_daily']] = np.nan
    return daily

# ------------------------------------------------------------
# 5. PM2.5 daily-mean per station
# ------------------------------------------------------------
def pm25_daily(stations: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for _, r in stations.iterrows():
        s = pm25_series(Path(r['csv']))
        s = s[(s.index >= WINDOW_START) & (s.index < WINDOW_END)]
        d = s.resample('1D').mean().dropna()
        frames.append(pd.DataFrame({
            'date': d.index, 'stationName': r['stationName'], 'pm25_daily': d.values,
        }))
    return pd.concat(frames, ignore_index=True)

# ------------------------------------------------------------
# 6. Driver
# ------------------------------------------------------------
def build(subset_days: int | None = None):
    stations = select_stations()
    print(f'Selected {len(stations)} stations (≥85% PM2.5 completeness).')
    print(stations['region3'].value_counts().to_string())
    stations.to_csv(OUT_DIR / 'stations_selected.csv', index=False)

    lats, lons = load_grid_reference()
    cells = station_cell_indices(stations, lats, lons)

    # 30-min slot grid covering the window in UTC.
    end = WINDOW_END if subset_days is None else WINDOW_START + pd.Timedelta(days=subset_days)
    slots = pd.date_range(WINDOW_START, end, freq='30min', inclusive='left')
    print(f'Iterating {len(slots)} slots across 5 products × {len(cells)} stations…')

    all_recs: list[dict] = []
    tick = max(1, len(slots) // 40)
    for i, ts in enumerate(slots):
        all_recs.extend(extract_slot(ts, cells))
        if i % tick == 0:
            print(f'  slot {i:>6}/{len(slots)}  ts={ts}  records={len(all_recs):,}')

    print(f'Rolling up {len(all_recs):,} slot records → daily…')
    daily = rollup_daily(all_recs)
    print(f'Daily rows: {len(daily):,}')

    pm = pm25_daily(stations)
    print(f'PM2.5 daily rows: {len(pm):,}')

    merged = daily.merge(pm, on=['stationName', 'date'], how='inner')
    merged = merged.merge(stations[['stationName', 'region', 'region3']],
                          on='stationName', how='left')
    print(f'AOD × PM2.5 daily pairs: {len(merged):,}')

    out_path = OUT_DIR / ('pm25_pairs_daily.pkl' if subset_days is None
                          else f'pm25_pairs_daily_subset{subset_days}d.pkl')
    merged.to_pickle(out_path)
    print(f'wrote {out_path}')
    return merged

if __name__ == '__main__':
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else None
    build(n)
