"""Match satellite AOD time series with AERONET observations.

Reads per-(sensor, site) raw CSV files produced by extract_satellite.py and
joins them with AERONET observations using a temporal window.

Temporal strategy per sensor:
    Himawari L2 : for each AERONET obs, average all L2 snapshots within ±LEO_WINDOW_MIN.
    Himawari L3 : for each calendar date, average all L3 snapshots on that day vs
                  the daily-mean AERONET AOD.
    VIIRS       : for each AERONET obs, average all overpass rows within ±LEO_WINDOW_MIN
                  (typically one overpass per obs).
    MODIS MAIAC : for each AERONET obs, average all orbit rows within ±MODIS_WINDOW_MIN;
                  fallback rows (is_fallback=True) match any AERONET obs for the day.

Output:
    One CSV per (sensor, site) at COLLOCATE_DIR / {sensor}_{site}.csv
    Columns: sensor, site, region, season, month, lat, lon, date,
             timestamp_aeronet, timestamp_satellite,
             satellite_aod, aeronet_aod, dist_km, vza, sza,
             spatial_flag, box_std
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

    For each AERONET observation at t_aer (UTC+7), finds all satellite rows
    within ±window_min of (t_aer converted to UTC) and averages their values.
    MODIS fallback rows (is_fallback=True) bypass the window and match any
    AERONET obs on the same UTC calendar date.

    Returns one record per AERONET observation that has at least one match.
    """
    window_td = pd.Timedelta(minutes=_SENSOR_WINDOW.get(sensor, LEO_WINDOW_MIN))

    # Parse UTC timestamps; fallback rows get NaT
    sat_df = sat_df.copy()
    sat_df['ts_utc'] = pd.to_datetime(
        sat_df['timestamp_sat'].replace('', float('nan')), errors='coerce'
    )
    is_fallback = sat_df['is_fallback'].fillna(False).astype(bool)
    normal_sat  = sat_df[~is_fallback]
    fallback_sat = sat_df[is_fallback]

    # Pre-extract a UTC date column for fallback matching
    # Fallback rows were generated from a specific date; recover it from the pool key
    # (timestamp_sat is empty, but we can infer date from surrounding context).
    # Since the raw CSV has no explicit date column for fallback rows, we derive it
    # from the sat_aod uniqueness — instead, we re-index fallback by row position
    # and match against AERONET date.  The is_fallback sentinel means "whole day",
    # but we need the day.  The extract step stores timestamp_sat='' for fallback;
    # we stored the date implicitly via the pool key 'fallback_{date}'.  Since we
    # can't recover that date directly from the CSV row, we require the caller to
    # pass a utc_date column.  If absent, we skip fallback matching.
    has_fallback_date = 'utc_date' in sat_df.columns

    # Pre-sort once for O(log N) binary search instead of O(N) boolean scan per row.
    normal_sat_sorted = normal_sat.sort_values('ts_utc').reset_index(drop=True)
    sat_times = normal_sat_sorted['ts_utc']  # sorted datetime64 Series

    records = []
    for _, aer_row in aer_df.iterrows():
        aer_dt  = aer_row['datetime']          # UTC+7
        aer_utc = aer_dt - _TZ                 # UTC
        aer_aod = float(aer_row['aod_550'])

        # Time-window match against non-fallback rows (searchsorted = O(log N))
        lo = int(sat_times.searchsorted(aer_utc - window_td))
        hi = int(sat_times.searchsorted(aer_utc + window_td, side='right'))
        matches = normal_sat_sorted.iloc[lo:hi] if lo < hi else pd.DataFrame()

        # Date-based match against fallback rows (MODIS only)
        if has_fallback_date and not fallback_sat.empty:
            utc_date = aer_utc.date()
            fb_mask  = fallback_sat['utc_date'] == str(utc_date)
            fb_matches = fallback_sat[fb_mask]
            matches = pd.concat([matches, fb_matches], ignore_index=True)

        if matches.empty:
            continue

        sat_ts = matches.iloc[0]['timestamp_sat']
        records.append({
            'sensor':              sensor,
            'site':                site,
            'region':              AERONET_SITES[site]['region'],
            'season':              'dry' if aer_dt.month in DRY_MONTHS else 'wet',
            'month':               aer_dt.month,
            'lat':                 station_lat,
            'lon':                 station_lon,
            'date':                aer_dt.date().isoformat(),
            'timestamp_aeronet':   aer_dt.isoformat(),
            'timestamp_satellite': sat_ts,
            'satellite_aod':       float(matches['sat_aod'].mean()),
            'aeronet_aod':         aer_aod,
            'dist_km':             float(matches['dist_km'].mean()),
            'vza':                 float(matches['vza'].mean())
                                   if matches['vza'].notna().any() else np.nan,
            'sza':                 float(matches['sza'].mean())
                                   if matches['sza'].notna().any() else np.nan,
            'spatial_flag':        int(matches['spatial_flag'].min()),
            # box_std: temporal std across matched snapshots (scene-change indicator);
            # collapses to the single-snapshot spatial std when only one row matches.
            'box_std':             float(matches['sat_aod'].std(ddof=0))
                                   if len(matches) > 1
                                   else float(matches['box_std'].iloc[0]),
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

    For each calendar date that appears in both the satellite time series and
    the AERONET record, averages all L3 snapshots for that day and pairs with
    the AERONET daily mean.  The satellite timestamp is UTC; the calendar date
    is taken from the satellite side (which is already in UTC).
    """
    sat_df = sat_df.copy()
    sat_df['sat_date'] = pd.to_datetime(sat_df['timestamp_sat']).dt.date

    aer_daily = (
        aer_df
        .assign(aer_date=aer_df['datetime'].dt.date)
        .groupby('aer_date')['aod_550'].mean()
        .reset_index()
        .rename(columns={'aer_date': 'date', 'aod_550': 'aer_aod'})
    )

    records = []
    for _, row in aer_daily.iterrows():
        d       = row['date']
        aer_aod = float(row['aer_aod'])

        # Satellite date: L3 timestamps are UTC; convert AERONET UTC+7 date → UTC
        # date by shifting back 7 h.  For simplicity we match both the UTC+7 date
        # and the UTC date (they can differ by one day around midnight).
        aer_utc_date = (
            pd.Timestamp(d) - _TZ
        ).date()
        day_sat = sat_df[sat_df['sat_date'].isin([d, aer_utc_date])]
        if day_sat.empty:
            continue

        dt_repr = pd.Timestamp(d)
        records.append({
            'sensor':              sensor,
            'site':                site,
            'region':              AERONET_SITES[site]['region'],
            'season':              'dry' if d.month in DRY_MONTHS else 'wet',
            'month':               d.month,
            'lat':                 station_lat,
            'lon':                 station_lon,
            'date':                d.isoformat(),
            'timestamp_aeronet':   dt_repr.isoformat(),
            'timestamp_satellite': day_sat.iloc[0]['timestamp_sat'],
            'satellite_aod':       float(day_sat['sat_aod'].mean()),
            'aeronet_aod':         aer_aod,
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

        # Add utc_date column for MODIS fallback matching
        if sensor == 'modis_maiac':
            _ts = pd.to_datetime(sat_df['timestamp_sat'], errors='coerce')
            sat_df['utc_date'] = _ts.dt.date.astype(str).where(_ts.notna(), '')

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
