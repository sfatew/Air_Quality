"""Stage A validation — Sections 8.1–8.6 of the thesis plan.

§8.1  Held-out AERONET validation
      Temporal split: train Sep 2022–Dec 2024 | test Jan 2025–Apr 2026
      Metrics: R, R², RMSE, MAE, Bias, %EE
      Strata : station × season × confidence_flag

§8.2  Internal-consistency check
      Where multiple sensors overlap, compute per-pair R² by region.
      Baseline from Nguyen 2025: MODIS–Himawari R² = 0.621/0.474/0.756 (N/C/S)

§8.3  Precipitation-aware validation  (requires ERA5 Precip from Stage A NetCDFs)
      Dry intervals (>24 h since rain) vs post-rain (0–12 h).
      GPM IMERG ≥ 0.1 mm/hr defines a rain event (uses ERA5 Precip as proxy here).

§8.5  Baseline comparison
      B1: VIIRS-only daily (extracted from per-sensor grid in merged NetCDFs)
      B4: Himawari-only baseline — R² = 0.293 (Nguyen 2025, RANSAC daily)

§8.6  RANSAC diagnostic
      Fit OLS and RANSAC to (merged_aod, aeronet_aod) pairs; report outlier fraction.
"""

from __future__ import annotations
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional
import glob
import warnings

import numpy as np
import pandas as pd
import netCDF4 as nc
from scipy import stats
from sklearn.linear_model import RANSACRegressor, LinearRegression

from config import (
    AERONET_SITES, DRY_MONTHS,
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
    MERGED_DIR,
    LATS, LONS, NLAT, NLON, GRID_RES, LAT_MAX, LON_MIN,
)
from aeronet import load_all_aeronet

# ── Study period splits ────────────────────────────────────────────────────────
TRAIN_END   = date(2024, 12, 31)   # inclusive training period end
TEST_START  = date(2025, 1, 1)     # held-out test period start
TEST_END    = date(2026, 4, 30)    # held-out test period end

# Expected-error envelope (MODIS Deep Blue / DT land standard)
EE_OFFSET   = 0.05
EE_SLOPE    = 0.15

# Nguyen 2025 inter-sensor R² baselines (MODIS–Himawari, per region)
NGUYEN2025_R2_BASELINES = {
    ('modis_maiac', 'himawari_l2', 'north'):   0.621,
    ('modis_maiac', 'himawari_l2', 'central'): 0.474,
    ('modis_maiac', 'himawari_l2', 'south'):   0.756,
}


# ── Station pixel helpers ──────────────────────────────────────────────────────

def _station_rc(site: str) -> tuple[int, int]:
    """Return (row, col) in the 0.05° config grid for a named AERONET site."""
    meta = AERONET_SITES[site]
    row  = int((LAT_MAX - GRID_RES / 2 - meta['lat']) / GRID_RES)
    col  = int((meta['lon'] - LON_MIN - GRID_RES / 2) / GRID_RES)
    return row, col


def _get_region(lat: float) -> str:
    if lat >= NORTH_CENTRAL_LAT:
        return 'north'
    if lat >= CENTRAL_SOUTH_LAT:
        return 'central'
    return 'south'


# ── Merged NetCDF file discovery ──────────────────────────────────────────────

def _merged_files_for_range(start: date, end: date) -> list[Path]:
    """Return all merged NetCDF files in chronological order for date range."""
    files = []
    d = start
    while d <= end:
        pattern = str(MERGED_DIR / d.strftime('%Y') / d.strftime('%m') /
                      d.strftime('%d') / 'merged_*.nc')
        files.extend(glob.glob(pattern))
        d += timedelta(days=1)
    return sorted(files)


def _parse_slot_utc(fpath: str) -> Optional[datetime]:
    """Parse UTC slot datetime from merged NetCDF filename."""
    stem = Path(fpath).stem   # merged_YYYYMMDD_HHMM
    try:
        return datetime.strptime(stem, 'merged_%Y%m%d_%H%M')
    except ValueError:
        return None


# ── §8.1 AERONET matched pairs extraction ─────────────────────────────────────

def extract_aeronet_pairs(
    start: date,
    end:   date,
    window_min: int = 30,
) -> pd.DataFrame:
    """Build matched (merged_aod, aeronet_aod) pairs for all slots in [start, end].

    For each merged NetCDF:
      1. Read AOD_merged at the two AERONET station pixels.
      2. Find AERONET observations within ±window_min minutes.
      3. Average AERONET values in the window → one row per (slot, site).

    Returns DataFrame with columns:
        slot_utc, site, region, season, month,
        merged_aod, aeronet_aod,
        confidence_flag, n_sensors, aeronet_n_obs
    """
    aer_all = load_all_aeronet()
    # Pre-index AERONET by site for fast look-up
    aer_by_site = {site: grp.copy() for site, grp in aer_all.groupby('site')}

    # Pre-compute station pixel indices
    station_rc = {site: _station_rc(site) for site in AERONET_SITES}

    records = []
    files   = _merged_files_for_range(start, end)

    for fpath in files:
        slot = _parse_slot_utc(fpath)
        if slot is None:
            continue

        try:
            with nc.Dataset(fpath) as ds:
                aod_merged  = ds.variables['AOD_merged'][:]
                conf_flag   = ds.variables['confidence_flag'][:]
                n_sensors   = ds.variables['n_sensors'][:]
        except Exception:
            continue

        aod_merged = np.array(aod_merged, dtype=np.float32)
        conf_flag  = np.array(conf_flag,  dtype=np.int8)
        n_sensors  = np.array(n_sensors,  dtype=np.int8)
        fill_val   = -9999.0
        aod_merged = np.where(aod_merged == fill_val, np.nan, aod_merged)

        slot_utc_ts = pd.Timestamp(slot)
        delta       = pd.Timedelta(minutes=window_min)

        for site, (row, col) in station_rc.items():
            if not (0 <= row < NLAT and 0 <= col < NLON):
                continue

            sat_aod  = float(aod_merged[row, col])
            if not np.isfinite(sat_aod):
                continue

            c_flag   = int(conf_flag[row, col])
            n_sens   = int(n_sensors[row, col])

            # Find AERONET obs in window
            site_df = aer_by_site.get(site)
            if site_df is None or site_df.empty:
                continue

            in_win = site_df[
                (site_df['datetime'] >= slot_utc_ts - delta) &
                (site_df['datetime'] <= slot_utc_ts + delta)
            ]
            if in_win.empty:
                continue

            aer_aod  = float(in_win['aod_550'].mean())
            n_aer    = len(in_win)

            records.append({
                'slot_utc':       slot,
                'site':           site,
                'region':         AERONET_SITES[site]['region'],
                'season':         'dry' if slot.month in DRY_MONTHS else 'wet',
                'month':          slot.month,
                'merged_aod':     sat_aod,
                'aeronet_aod':    aer_aod,
                'confidence_flag': c_flag,
                'n_sensors':      n_sens,
                'aeronet_n_obs':  n_aer,
            })

    return pd.DataFrame(records)


# ── §8.1 Metric calculations ──────────────────────────────────────────────────

def _pct_ee(sat: np.ndarray, aer: np.ndarray) -> float:
    """Fraction of retrievals within ±(EE_OFFSET + EE_SLOPE × AOD_AERONET) × 100."""
    ee  = EE_OFFSET + EE_SLOPE * np.abs(aer)
    within = np.abs(sat - aer) <= ee
    return float(within.mean() * 100)


def compute_metrics(
    sat: np.ndarray,
    aer: np.ndarray,
    label: str = '',
) -> dict:
    """Compute standard AOD validation metrics for a matched pair array.

    Returns dict with keys: N, R, R2, RMSE, MAE, Bias, pct_EE, label
    """
    mask = np.isfinite(sat) & np.isfinite(aer)
    sat  = sat[mask]
    aer  = aer[mask]
    N    = len(sat)
    if N < 3:
        return {'N': N, 'R': np.nan, 'R2': np.nan, 'RMSE': np.nan,
                'MAE': np.nan, 'Bias': np.nan, 'pct_EE': np.nan, 'label': label}

    r, _   = stats.pearsonr(sat, aer)
    bias   = float(np.mean(sat - aer))
    rmse   = float(np.sqrt(np.mean((sat - aer) ** 2)))
    mae    = float(np.mean(np.abs(sat - aer)))
    # R² as 1 - SS_res/SS_tot (different from r² for non-zero intercept)
    ss_res = np.sum((sat - aer) ** 2)
    ss_tot = np.sum((aer - aer.mean()) ** 2)
    r2     = float(1 - ss_res / ss_tot) if ss_tot > 0 else np.nan
    pct_ee = _pct_ee(sat, aer)

    return {
        'N':      N,
        'R':      float(r),
        'R2':     r2,
        'RMSE':   rmse,
        'MAE':    mae,
        'Bias':   bias,
        'pct_EE': pct_ee,
        'label':  label,
    }


def stratified_metrics(pairs: pd.DataFrame) -> pd.DataFrame:
    """Run §8.1 metrics for every (site × season × confidence_flag) stratum.

    Returns a tidy DataFrame with one row per stratum.
    """
    rows = []

    groupers = [
        # (stratum_cols, label_fn)
        (['site'],                            lambda r: r['site']),
        (['site', 'season'],                  lambda r: f"{r['site']} {r['season']}"),
        (['site', 'confidence_flag'],         lambda r: f"{r['site']} conf={r['confidence_flag']}"),
        (['site', 'season', 'confidence_flag'],
                                              lambda r: f"{r['site']} {r['season']} conf={r['confidence_flag']}"),
    ]

    for group_cols, _ in groupers:
        for keys, grp in pairs.groupby(group_cols):
            if isinstance(keys, str):
                keys = (keys,)
            label_parts = dict(zip(group_cols, keys))
            label       = '  '.join(f'{k}={v}' for k, v in label_parts.items())

            m = compute_metrics(
                grp['merged_aod'].values,
                grp['aeronet_aod'].values,
                label=label,
            )
            m.update(label_parts)
            rows.append(m)

    # Overall
    m = compute_metrics(
        pairs['merged_aod'].values,
        pairs['aeronet_aod'].values,
        label='ALL',
    )
    m['site'] = 'ALL'
    rows.append(m)

    return pd.DataFrame(rows)


# ── §8.2 Internal-consistency check ──────────────────────────────────────────

def inter_sensor_consistency(
    start: date,
    end:   date,
    sensor_pairs: list[tuple[str, str]] = (
        ('AOD_modis_maiac', 'AOD_himawari_l2'),
        ('AOD_viirs_noaa20', 'AOD_himawari_l2'),
        ('AOD_viirs_noaa20', 'AOD_modis_maiac'),
    ),
) -> pd.DataFrame:
    """§8.2: Compute inter-sensor R² by region using per-sensor grids in merged NetCDFs.

    Only slots where BOTH sensors have valid data for a given cell are used.

    Returns DataFrame: sensor_a, sensor_b, region, N, R2, RMSE, Bias
    """
    # Accumulate (a_val, b_val, region) pairs across all files
    data: dict[tuple[str, str], dict[str, list]] = {
        pair: {'north': [], 'central': [], 'south': []}
        for pair in sensor_pairs
    }

    # Build per-cell region mask (constant)
    lat_2d = np.meshgrid(LATS, LONS, indexing='ij')[0]
    region_grid = np.where(
        lat_2d >= NORTH_CENTRAL_LAT, 0,
        np.where(lat_2d < CENTRAL_SOUTH_LAT, 2, 1)
    )  # 0=north, 1=central, 2=south
    region_names = {0: 'north', 1: 'central', 2: 'south'}

    files = _merged_files_for_range(start, end)

    for fpath in files:
        try:
            with nc.Dataset(fpath) as ds:
                available = list(ds.variables.keys())
                slot_data = {}
                for sen_a, sen_b in sensor_pairs:
                    for s in (sen_a, sen_b):
                        if s in available and s not in slot_data:
                            arr = np.array(ds.variables[s][:], dtype=np.float32)
                            arr[arr == -9999.0] = np.nan
                            slot_data[s] = arr
        except Exception:
            continue

        for pair in sensor_pairs:
            sen_a, sen_b = pair
            if sen_a not in slot_data or sen_b not in slot_data:
                continue
            a = slot_data[sen_a]
            b = slot_data[sen_b]
            valid = np.isfinite(a) & np.isfinite(b)
            if not np.any(valid):
                continue

            for reg_idx, reg_name in region_names.items():
                mask = valid & (region_grid == reg_idx)
                if np.sum(mask) < 1:
                    continue
                data[pair][reg_name].extend(
                    zip(a[mask].tolist(), b[mask].tolist())
                )

    rows = []
    for (sen_a, sen_b), region_dict in data.items():
        for region, pairs_list in region_dict.items():
            if len(pairs_list) < 10:
                continue
            arr = np.array(pairs_list)
            a_vals, b_vals = arr[:, 0], arr[:, 1]
            r, _  = stats.pearsonr(a_vals, b_vals)
            rmse  = float(np.sqrt(np.mean((a_vals - b_vals) ** 2)))
            bias  = float(np.mean(a_vals - b_vals))
            baseline_key = (sen_a.replace('AOD_', ''), sen_b.replace('AOD_', ''), region)
            baseline_r2  = NGUYEN2025_R2_BASELINES.get(baseline_key, np.nan)
            rows.append({
                'sensor_a':    sen_a,
                'sensor_b':    sen_b,
                'region':      region,
                'N':           len(pairs_list),
                'R2':          float(r ** 2),
                'R2_baseline': baseline_r2,
                'R2_delta':    float(r ** 2) - baseline_r2 if not np.isnan(baseline_r2) else np.nan,
                'RMSE':        rmse,
                'Bias':        bias,
            })

    return pd.DataFrame(rows)


# ── §8.3 Precipitation-aware validation ───────────────────────────────────────

def precip_aware_validation(pairs: pd.DataFrame) -> pd.DataFrame:
    """§8.3: Validate separately for dry vs post-rain intervals.

    Uses ERA5 precipitation already stored in merged NetCDFs.
    For each matched pair, look up the ERA5_Precip at the AERONET pixel and
    reconstruct an approximate 'hours since last rain' flag.

    NOTE: Full implementation requires loading ERA5_Precip from the NetCDFs
    sequentially to compute time-since-last-rain.  This version uses a simplified
    approach: flag a slot as 'wet' if ERA5_Precip at the station pixel > 0.1 mm
    in the current or any of the previous 24 h.
    """
    station_rc = {site: _station_rc(site) for site in AERONET_SITES}
    slot_precip: dict[tuple[str, datetime], float] = {}

    # Load precip for each slot in the pairs
    unique_slots = pairs['slot_utc'].unique()
    slot_files   = {}
    for slot in unique_slots:
        d = slot.date() if hasattr(slot, 'date') else pd.Timestamp(slot).date()
        pattern = str(MERGED_DIR / pd.Timestamp(slot).strftime('%Y') /
                      pd.Timestamp(slot).strftime('%m') /
                      pd.Timestamp(slot).strftime('%d') /
                      f'merged_{pd.Timestamp(slot).strftime("%Y%m%d_%H%M")}.nc')
        slot_files[slot] = pattern

    for slot, fpath in slot_files.items():
        try:
            with nc.Dataset(fpath) as ds:
                if 'ERA5_Precip' not in ds.variables:
                    continue
                precip = np.array(ds.variables['ERA5_Precip'][:], dtype=np.float32)
                precip[precip == -9999.0] = np.nan
                for site, (row, col) in station_rc.items():
                    if 0 <= row < NLAT and 0 <= col < NLON:
                        slot_precip[(site, slot)] = float(precip[row, col])
        except Exception:
            continue

    if not slot_precip:
        return pd.DataFrame()

    # Build time-since-last-rain: scan 24 h back
    rain_thresh = 0.1  # mm

    def _is_wet(site: str, slot: datetime) -> bool:
        for lag_h in range(0, 25):
            t = pd.Timestamp(slot) - pd.Timedelta(hours=lag_h)
            prec = slot_precip.get((site, t.to_pydatetime()))
            if prec is not None and prec >= rain_thresh:
                return True
        return False

    pairs = pairs.copy()
    pairs['wet_flag'] = pairs.apply(
        lambda r: _is_wet(r['site'], r['slot_utc']), axis=1
    )

    rows = []
    for wet, label in [(False, 'dry (>24h since rain)'), (True, 'post-rain (≤24h)')]:
        for site in AERONET_SITES:
            sub = pairs[(pairs['site'] == site) & (pairs['wet_flag'] == wet)]
            if len(sub) < 3:
                continue
            m = compute_metrics(sub['merged_aod'].values, sub['aeronet_aod'].values,
                                 label=f'{site} {label}')
            m['site']     = site
            m['wet_flag'] = wet
            rows.append(m)
    return pd.DataFrame(rows)


# ── §8.5 Baseline comparison ──────────────────────────────────────────────────

def baseline_comparison(
    pairs: pd.DataFrame,
    sensor_key: str = 'AOD_viirs_noaa20',
    merged_nc_dir: Path = MERGED_DIR,
) -> pd.DataFrame:
    """§8.5 B1: Compare bias-corrected single-sensor VIIRS vs merged product.

    Reads the per-sensor grid from the merged NetCDFs, extracts at AERONET pixels,
    and computes metrics for (sensor_key) vs AERONET — the 'B1' baseline.
    """
    station_rc = {site: _station_rc(site) for site in AERONET_SITES}
    sensor_vals: dict[str, list[float]] = {site: [] for site in AERONET_SITES}
    merged_vals: dict[str, list[float]] = {site: [] for site in AERONET_SITES}
    aer_vals:    dict[str, list[float]] = {site: [] for site in AERONET_SITES}

    # Build quick AERONET lookup by (site, 30-min slot)
    aer_all = load_all_aeronet()
    aer_all['slot'] = aer_all['datetime'].dt.floor('30min')
    aer_slot = aer_all.groupby(['site', 'slot'])['aod_550'].mean().reset_index()
    aer_slot.columns = ['site', 'slot', 'aod_550']

    # Read pairs' slots
    for _, row in pairs.iterrows():
        site  = row['site']
        slot  = pd.Timestamp(row['slot_utc'])
        fpath = str(merged_nc_dir / slot.strftime('%Y') / slot.strftime('%m') /
                    slot.strftime('%d') / f"merged_{slot.strftime('%Y%m%d_%H%M')}.nc")
        try:
            with nc.Dataset(fpath) as ds:
                if sensor_key not in ds.variables:
                    continue
                sen_grid = np.array(ds.variables[sensor_key][:], dtype=np.float32)
                sen_grid[sen_grid == -9999.0] = np.nan
        except Exception:
            continue

        r, c = station_rc[site]
        val  = float(sen_grid[r, c])
        if not np.isfinite(val):
            continue

        sensor_vals[site].append(val)
        merged_vals[site].append(float(row['merged_aod']))
        aer_vals[site].append(float(row['aeronet_aod']))

    rows = []
    for site in AERONET_SITES:
        if len(aer_vals[site]) < 3:
            continue
        aer = np.array(aer_vals[site])
        m_merged = compute_metrics(np.array(merged_vals[site]), aer, label=f'{site} merged')
        m_merged['product'] = 'merged'
        m_merged['site']    = site
        rows.append(m_merged)

        m_single = compute_metrics(np.array(sensor_vals[site]), aer, label=f'{site} {sensor_key}')
        m_single['product'] = sensor_key
        m_single['site']    = site
        rows.append(m_single)

    return pd.DataFrame(rows)


# ── §8.6 RANSAC diagnostic ────────────────────────────────────────────────────

def ransac_diagnostic(pairs: pd.DataFrame) -> pd.DataFrame:
    """§8.6: OLS vs RANSAC regression for (merged_aod, aeronet_aod) pairs.

    Quantifies outlier influence — the merged product is NOT filtered,
    this is a diagnostic only.

    Returns one row per site with OLS R², RANSAC R², inlier fraction.
    """
    rows = []
    for site in (list(AERONET_SITES.keys()) + ['ALL']):
        sub = pairs if site == 'ALL' else pairs[pairs['site'] == site]
        sat = sub['merged_aod'].values.reshape(-1, 1)
        aer = sub['aeronet_aod'].values

        mask = np.isfinite(sat.ravel()) & np.isfinite(aer)
        sat  = sat[mask];  aer  = aer[mask]
        if len(sat) < 10:
            continue

        # OLS
        ols     = LinearRegression().fit(sat, aer)
        ols_r2  = float(ols.score(sat, aer))

        # RANSAC
        try:
            ransac      = RANSACRegressor(min_samples=0.5, random_state=42)
            ransac.fit(sat, aer)
            inlier_mask = ransac.inlier_mask_
            inlier_frac = float(inlier_mask.mean())
            ransac_r2   = float(ransac.score(sat[inlier_mask], aer[inlier_mask]))
            slope       = float(ransac.estimator_.coef_[0])
            intercept   = float(ransac.estimator_.intercept_)
        except Exception:
            inlier_frac = np.nan
            ransac_r2   = np.nan
            slope       = np.nan
            intercept   = np.nan

        rows.append({
            'site':         site,
            'N':            len(sat),
            'OLS_R2':       ols_r2,
            'RANSAC_R2':    ransac_r2,
            'RANSAC_R2_lift': float(ransac_r2 - ols_r2) if not np.isnan(ransac_r2) else np.nan,
            'inlier_frac':  inlier_frac,
            'ransac_slope': slope,
            'ransac_intercept': intercept,
        })

    return pd.DataFrame(rows)


# ── Coverage diagnostic ───────────────────────────────────────────────────────

def spatial_coverage_stats(start: date, end: date) -> pd.DataFrame:
    """Compute daily % of valid (non-NaN) AOD_merged cells over Vietnam.

    Reports daily, monthly, and seasonal mean coverage.
    """
    records = []
    total_cells = NLAT * NLON

    d = start
    while d <= end:
        day_files = sorted(glob.glob(str(
            MERGED_DIR / d.strftime('%Y') / d.strftime('%m') /
            d.strftime('%d') / 'merged_*.nc'
        )))
        if not day_files:
            d += timedelta(days=1)
            continue

        daily_valid = set()  # (row, col) cells valid in ANY slot
        for fpath in day_files:
            try:
                with nc.Dataset(fpath) as ds:
                    aod = np.array(ds.variables['AOD_merged'][:])
                    aod[aod == -9999.0] = np.nan
                    valid_mask = np.isfinite(aod)
                    rows_v, cols_v = np.where(valid_mask)
                    daily_valid.update(zip(rows_v.tolist(), cols_v.tolist()))
            except Exception:
                continue

        n_valid  = len(daily_valid)
        coverage = 100.0 * n_valid / total_cells

        records.append({
            'date':     d,
            'month':    d.month,
            'season':   'dry' if d.month in DRY_MONTHS else 'wet',
            'year':     d.year,
            'n_slots':  len(day_files),
            'n_valid_cells': n_valid,
            'coverage_pct':  coverage,
        })
        d += timedelta(days=1)

    return pd.DataFrame(records)
