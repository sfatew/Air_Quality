"""Match satellite AOD time series with AERONET observations.

Reads per-(sensor, site) raw CSV files produced by extract_satellite.py and
joins them with AERONET observations using a temporal window.

Temporal strategy per sensor (satellite-centric — one record per satellite snapshot):
    Himawari L2 : for each L2 snapshot, average all AERONET obs within ±LEO_WINDOW_MIN
                  of the snapshot's UTC timestamp.
    Himawari L3 : for each calendar date with L3 data, pair with the AERONET daily mean.
    VIIRS       : for each overpass row, average all AERONET obs within ±LEO_WINDOW_MIN.
    MODIS MAIAC : for each orbit row, average all AERONET obs within ±MODIS_WINDOW_MIN;
                  fallback rows (is_fallback=True) use the AERONET daily mean for that day.

Output:
    One CSV per (sensor, site) at COLLOCATE_DIR / {sensor}_{site}.csv
    Columns: sensor, site, region, season, month, lat, lon, date,
             timestamp_aeronet, timestamp_satellite,
             satellite_aod, aeronet_aod, aer_n, aer_std,
             dist_km, vza, sza, spatial_flag, box_std
"""

from __future__ import annotations
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from config import (
    AERONET_SITES, DRY_MONTHS,
    EXTRACT_DIR, COLLOCATE_DIR,
    LEO_WINDOW_MIN, MODIS_WINDOW_MIN,
    TZ_OFFSET_HOURS,
)
from aeronet import load_aeronet
from extract_satellite import extract_site

_TZ = pd.Timedelta(hours=TZ_OFFSET_HOURS)   # subtract from AERONET UTC+7 → UTC

_SENSOR_WINDOW: dict[str, int] = {
    'himawari_l2':  LEO_WINDOW_MIN,
    'viirs_snpp':   LEO_WINDOW_MIN,
    'viirs_noaa20': LEO_WINDOW_MIN,
    'modis_maiac':  MODIS_WINDOW_MIN,
}


# ── Matching helpers ──────────────────────────────────────────────────────────

def _match_leo(
    sat_df: pd.DataFrame,
    aer_df: pd.DataFrame,
    sensor: str,
    site: str,
    station_lat: float,
    station_lon: float,
) -> list[dict]:
    """Match satellite snapshots to AERONET observations by time window.

    For each satellite snapshot at t_sat (UTC), averages all AERONET observations
    within ±window_min of t_sat (after converting AERONET UTC+7 → UTC).
    Emits one record per satellite snapshot that has at least one AERONET match.

    MODIS fallback rows (is_fallback=True) match any AERONET obs for the same
    UTC calendar date and emit one record per fallback row.
    """
    window_td = pd.Timedelta(minutes=_SENSOR_WINDOW.get(sensor, LEO_WINDOW_MIN))

    # Pre-index AERONET in UTC for O(log N) window slicing via .loc[]
    aer_clean = aer_df[['datetime', 'aod_550']].dropna(subset=['aod_550']).copy()
    aer_clean['dt_utc'] = aer_clean['datetime'] - _TZ
    aer_idx = aer_clean.set_index('dt_utc').sort_index()   # sorted DatetimeIndex in UTC

    sat_df = sat_df.copy()
    sat_df['ts_utc'] = pd.to_datetime(
        sat_df['timestamp_sat'].replace('', float('nan')), errors='coerce'
    )
    is_fallback  = sat_df['is_fallback'].fillna(False).astype(bool)
    normal_sat   = sat_df[~is_fallback]
    fallback_sat = sat_df[is_fallback]

    has_fallback_date = 'utc_date' in sat_df.columns

    # Pre-compute date array once for fallback matching (avoid repeated .date() calls)
    aer_utc_dates = (
        pd.DatetimeIndex(aer_idx.index).date
        if (has_fallback_date and not fallback_sat.empty)
        else None
    )

    records = []

    # ── Time-window matching (Ichoku et al. 2002) ─────────────────────────────
    # Pre-extract numpy arrays to avoid per-row Pandas overhead; use
    # searchsorted on int64 nanosecond timestamps for O(log N) window lookup.
    aer_times_ns = aer_idx.index.values.astype(np.int64)
    aer_aods_arr = aer_idx['aod_550'].values
    window_ns    = window_td.value  # int64 nanoseconds

    valid_sat    = normal_sat[normal_sat['ts_utc'].notna()].reset_index(drop=True)
    sat_times_ns = valid_sat['ts_utc'].values.astype(np.int64)
    sat_ts_col   = valid_sat['timestamp_sat'].values
    sat_aod_col  = valid_sat['sat_aod'].values.astype(float)
    dist_col     = valid_sat['dist_km'].values.astype(float)
    vza_col      = valid_sat['vza'].values.astype(float)
    sza_col      = valid_sat['sza'].values.astype(float)
    sflag_col    = valid_sat['spatial_flag'].values.astype(int)
    bstd_col     = valid_sat['box_std'].values.astype(float)
    ts_local_dti = pd.DatetimeIndex(valid_sat['ts_utc'] + _TZ)
    region_val   = AERONET_SITES[site]['region']

    for i in range(len(valid_sat)):
        t_ns = sat_times_ns[i]
        lo   = np.searchsorted(aer_times_ns, t_ns - window_ns, side='left')
        hi   = np.searchsorted(aer_times_ns, t_ns + window_ns, side='right')
        vals = aer_aods_arr[lo:hi]
        if len(vals) == 0:
            continue

        aer_aod  = float(vals.mean())
        aer_n    = int(len(vals))
        aer_std  = float(vals.std()) if aer_n > 1 else 0.0
        tsl      = ts_local_dti[i]

        records.append({
            'sensor':              sensor,
            'site':                site,
            'region':              region_val,
            'season':              'dry' if tsl.month in DRY_MONTHS else 'wet',
            'month':               tsl.month,
            'lat':                 station_lat,
            'lon':                 station_lon,
            'date':                tsl.date().isoformat(),
            'timestamp_aeronet':   tsl.isoformat(),
            'timestamp_satellite': str(sat_ts_col[i]),
            'satellite_aod':       sat_aod_col[i],
            'aeronet_aod':         aer_aod,
            'aer_n':               aer_n,
            'aer_std':             aer_std,
            'dist_km':             dist_col[i],
            'vza':                 float(vza_col[i]) if np.isfinite(vza_col[i]) else np.nan,
            'sza':                 float(sza_col[i]) if np.isfinite(sza_col[i]) else np.nan,
            'spatial_flag':        sflag_col[i],
            'box_std':             bstd_col[i],
        })

    # ── Date-based matching for MODIS fallback rows ───────────────────────────
    if has_fallback_date and not fallback_sat.empty:
        for _, sat_row in fallback_sat.iterrows():
            utc_date_str = sat_row.get('utc_date', '')
            if not utc_date_str:
                continue
            try:
                utc_date = pd.Timestamp(utc_date_str).date()
            except Exception:
                continue

            day_mask = aer_utc_dates == utc_date
            day_aer  = aer_idx['aod_550'][day_mask]
            if len(day_aer) == 0:
                continue

            aer_aod  = float(day_aer.mean())
            aer_n    = int(len(day_aer))
            aer_std  = float(day_aer.std()) if aer_n > 1 else 0.0
            ts_local = pd.Timestamp(utc_date_str) + _TZ

            records.append({
                'sensor':              sensor,
                'site':                site,
                'region':              AERONET_SITES[site]['region'],
                'season':              'dry' if ts_local.month in DRY_MONTHS else 'wet',
                'month':               ts_local.month,
                'lat':                 station_lat,
                'lon':                 station_lon,
                'date':                utc_date_str,
                'timestamp_aeronet':   ts_local.isoformat(),
                'timestamp_satellite': '',
                'satellite_aod':       float(sat_row['sat_aod']),
                'aeronet_aod':         aer_aod,
                'aer_n':               aer_n,
                'aer_std':             aer_std,
                'dist_km':             float(sat_row['dist_km']),
                'vza':                 float(sat_row['vza'])
                                       if pd.notna(sat_row['vza']) else np.nan,
                'sza':                 float(sat_row['sza'])
                                       if pd.notna(sat_row['sza']) else np.nan,
                'spatial_flag':        int(sat_row['spatial_flag']),
                'box_std':             float(sat_row['box_std']),
            })

    return records


def _match_l3_daily(
    sat_df: pd.DataFrame,
    aer_df: pd.DataFrame,
    sensor: str,
    site: str,
    station_lat: float,
    station_lon: float,
) -> list[dict]:
    """Match Himawari L3 hourly composites to AERONET daily means.

    For each calendar date that has satellite data, averages all L3 snapshots
    for that day and pairs with the AERONET daily mean (computed in UTC).
    One record per satellite date.
    """
    sat_df = sat_df.copy()
    sat_df['sat_date'] = pd.to_datetime(sat_df['timestamp_sat']).dt.normalize()

    # Build AERONET daily means keyed by UTC midnight Timestamp
    aer_clean = aer_df[['datetime', 'aod_550']].dropna(subset=['aod_550']).copy()
    aer_clean['dt_utc'] = aer_clean['datetime'] - _TZ
    aer_daily = (
        aer_clean
        .assign(utc_date=lambda df: df['dt_utc'].dt.normalize())
        .groupby('utc_date')['aod_550']
        .agg(aer_aod='mean', aer_std='std', aer_n='count')
    )   # DataFrame indexed by UTC midnight Timestamps

    records = []
    for sat_date, day_sat in sat_df.groupby('sat_date'):
        # Try satellite UTC date first; fall back to UTC+7 date (midnight boundary)
        if sat_date in aer_daily.index:
            aer_row = aer_daily.loc[sat_date]
        else:
            utc7_date = sat_date + _TZ
            if utc7_date in aer_daily.index:
                aer_row = aer_daily.loc[utc7_date]
            else:
                continue

        ts_local = sat_date + _TZ
        d        = ts_local.date()

        records.append({
            'sensor':              sensor,
            'site':                site,
            'region':              AERONET_SITES[site]['region'],
            'season':              'dry' if d.month in DRY_MONTHS else 'wet',
            'month':               d.month,
            'lat':                 station_lat,
            'lon':                 station_lon,
            'date':                d.isoformat(),
            'timestamp_aeronet':   ts_local.isoformat(),
            'timestamp_satellite': day_sat.iloc[0]['timestamp_sat'],
            'satellite_aod':       float(day_sat['sat_aod'].mean()),
            'aeronet_aod':         float(aer_row['aer_aod']),
            'aer_n':               int(aer_row['aer_n']),
            'aer_std':             float(aer_row['aer_std'])
                                   if pd.notna(aer_row['aer_std']) else 0.0,
            'dist_km':             0.0,
            'vza':                 np.nan,
            'sza':                 np.nan,
            'spatial_flag':        int(day_sat['spatial_flag'].min()),
            'box_std':             float(day_sat['sat_aod'].std(ddof=0))
                                   if len(day_sat) > 1
                                   else float(day_sat['box_std'].iloc[0]),
        })
    return records


# ── Main matching entry points ────────────────────────────────────────────────

def match_site(
    site: str,
    sensors: tuple[str, ...] = (
        'himawari_l2', 'himawari_l3',
        'viirs_snpp', 'viirs_noaa20',
        'modis_maiac',
    ),
    extract_dir: Path | str = EXTRACT_DIR,
    output_dir:  Path | str = COLLOCATE_DIR,
) -> dict[str, pd.DataFrame]:
    """Match extracted satellite time series with AERONET for one site.

    Reads raw satellite CSVs from extract_dir (written by extract_site()),
    loads AERONET for the site, and writes matched-pair CSVs to output_dir.

    New pairs are appended and de-duplicated on (timestamp_aeronet,
    timestamp_satellite) so re-runs for overlapping periods are safe.

    Returns a dict mapping sensor → DataFrame of all matched pairs.
    """
    extract_dir = Path(extract_dir)
    output_dir  = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    meta        = AERONET_SITES[site]
    station_lat = meta['lat']
    station_lon = meta['lon']

    aer_all = load_aeronet(site)

    results: dict[str, pd.DataFrame] = {}
    for sensor in sensors:
        raw_path = extract_dir / f'{sensor}_{site}_raw.csv'
        if not raw_path.exists():
            continue

        sat_df = pd.read_csv(raw_path)
        if sat_df.empty:
            continue

        # Add utc_date column for MODIS fallback matching.
        # Non-fallback rows: compute from timestamp_sat.
        # Fallback rows: timestamp_sat is '' so we preserve the utc_date written
        # by _extract_modis(); overwriting it would always produce '' and break
        # the date-based AERONET matching in _match_leo().
        if sensor == 'modis_maiac':
            _ts = pd.to_datetime(sat_df['timestamp_sat'], errors='coerce')
            from_ts = _ts.dt.date.astype(str).where(_ts.notna(), '')
            is_fb = sat_df['is_fallback'].fillna(False).astype(bool)
            if 'utc_date' in sat_df.columns:
                sat_df['utc_date'] = sat_df['utc_date'].where(is_fb, from_ts)
            else:
                sat_df['utc_date'] = from_ts

        if sensor == 'himawari_l3':
            records = _match_l3_daily(
                sat_df, aer_all, sensor, site, station_lat, station_lon
            )
        else:
            records = _match_leo(
                sat_df, aer_all, sensor, site, station_lat, station_lon
            )

        if not records:
            continue

        df = pd.DataFrame(records)
        csv_path = output_dir / f'{sensor}_{site}.csv'

        if csv_path.exists():
            existing = pd.read_csv(csv_path)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(
                subset=['timestamp_aeronet', 'timestamp_satellite'],
                keep='last',
            )

        df.to_csv(csv_path, index=False)
        results[sensor] = df
        print(f'  [{site}] {sensor}: {len(records)} new pairs  (total {len(df)})')

    return results


def collocate_site(
    site: str,
    start_date: date,
    end_date: date,
    sensors: tuple[str, ...] = (
        'himawari_l2', 'himawari_l3',
        'viirs_snpp', 'viirs_noaa20',
        'modis_maiac',
    ),
    output_dir:  Path | str = COLLOCATE_DIR,
    extract_dir: Path | str = EXTRACT_DIR,
) -> dict[str, pd.DataFrame]:
    """Extract satellite data then match with AERONET for one site.

    Convenience wrapper that calls extract_site() followed by match_site().
    Use the two functions separately when you want to re-match with different
    AERONET data or time windows without re-reading satellite files.

    Returns the matched-pair DataFrames (same as match_site).
    """
    extract_site(
        site, start_date, end_date,
        sensors=sensors,
        output_dir=Path(extract_dir),
    )
    return match_site(
        site,
        sensors=sensors,
        extract_dir=Path(extract_dir),
        output_dir=Path(output_dir),
    )
