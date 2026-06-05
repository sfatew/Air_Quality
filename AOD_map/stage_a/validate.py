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

try:
    from tqdm import tqdm as _tqdm_cls
    _HAS_TQDM = True
except ImportError:
    _tqdm_cls = None  # type: ignore[assignment]
    _HAS_TQDM = False


def _make_bar(iterable, **kwargs):
    if _HAS_TQDM and _tqdm_cls is not None:
        return _tqdm_cls(iterable, **kwargs)
    return None

from config import (
    AERONET_SITES, DRY_MONTHS,
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
    MERGED_DIR, TZ_OFFSET_HOURS,
    LATS, LONS, NLAT, NLON, GRID_RES, LAT_MAX, LON_MIN,
    DATA_ROOT,
)
from aeronet import load_all_aeronet

_TZ = pd.Timedelta(hours=TZ_OFFSET_HOURS)  # UTC → UTC+7 (AERONET datetimes are UTC+7)

# ── Study period splits ────────────────────────────────────────────────────────
TRAIN_END   = date(2024, 12, 31)   # inclusive training period end
TEST_START  = date(2025, 1, 1)     # held-out test period start
TEST_END    = date(2026, 4, 30)    # held-out test period end

# Expected-error envelope (MODIS Deep Blue / DT land standard)
EE_OFFSET   = 0.05
EE_SLOPE    = 0.15

# Nguyen 2025 inter-sensor R² baselines (MODIS–Himawari, per region).
# v3.2: a single 'AOD_himawari' grid replaces the separate L2/L3 grids,
# so the baseline key is keyed by 'himawari'.
NGUYEN2025_R2_BASELINES = {
    ('modis_maiac', 'himawari', 'north'):   0.621,
    ('modis_maiac', 'himawari', 'central'): 0.474,
    ('modis_maiac', 'himawari', 'south'):   0.756,
}


# ── Station pixel helpers ──────────────────────────────────────────────────────

def _station_rc(site: str) -> tuple[int, int]:
    """Return (row, col) in the 0.05° config grid for a named AERONET site."""
    meta = AERONET_SITES[site]
    row  = int(round((LAT_MAX - GRID_RES / 2 - meta['lat']) / GRID_RES))
    col  = int(round((meta['lon'] - LON_MIN - GRID_RES / 2) / GRID_RES))
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

    records  = []
    files    = _merged_files_for_range(start, end)
    bar      = _make_bar(files, desc='Matching AERONET pairs', unit='file', ncols=80)

    for fpath in (bar if bar is not None else files):
        slot = _parse_slot_utc(fpath)
        if slot is None:
            continue

        try:
            with nc.Dataset(fpath) as ds:
                aod_merged = np.ma.filled(ds.variables['AOD_merged'][:].astype(np.float32), np.nan)
                conf_flag  = np.ma.filled(ds.variables['confidence_flag'][:].astype(np.int8), 0)
                n_sensors  = np.ma.filled(ds.variables['n_sensors'][:].astype(np.int8), 0)
        except Exception:
            continue

        slot_utc_ts   = pd.Timestamp(slot)
        slot_local_ts = slot_utc_ts + _TZ   # AERONET datetimes are UTC+7
        delta         = pd.Timedelta(minutes=window_min)

        for site, (row, col) in station_rc.items():
            if not (0 <= row < NLAT and 0 <= col < NLON):
                continue

            sat_aod  = float(aod_merged[row, col])
            if not np.isfinite(sat_aod) or sat_aod < 0:
                continue

            c_flag   = int(conf_flag[row, col])
            n_sens   = int(n_sensors[row, col])

            # Find AERONET obs in window (compare UTC+7 to UTC+7)
            site_df = aer_by_site.get(site)
            if site_df is None or site_df.empty:
                continue

            in_win = site_df[
                (site_df['datetime'] >= slot_local_ts - delta) &
                (site_df['datetime'] <= slot_local_ts + delta)
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
    if N < 10:
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
        ['site'],
        ['site', 'season'],
        ['site', 'confidence_flag'],
        ['site', 'season', 'confidence_flag'],
    ]

    for group_cols in groupers:
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
        ('AOD_modis_maiac', 'AOD_himawari'),
        ('AOD_viirs_noaa20', 'AOD_himawari'),
        ('AOD_viirs_noaa20', 'AOD_modis_maiac'),
    ),
    stations_csv: Path = None,
) -> pd.DataFrame:
    """§8.2: Inter-sensor R² by region using daily means at Envisoft station pixels.

    Methodology (v3.2 — aligned with EDA Regional Comparison and Nguyen 2025):
      1. Extract each sensor's AOD at every Envisoft station pixel, every slot.
      2. Aggregate to daily means per (station, date, sensor).
      3. For each (sensor_a, sensor_b, region), regress all (station, date)
         daily-mean pairs within that region.

    The v3.1 all-cell-all-slot scan produced 10⁶-tick scatter dominated by
    QA-edge and IDW-blend artefacts; Nguyen 2025 reports station-level daily
    R², so the previous "vs baseline" deltas were apples-to-oranges.

    Returns DataFrame: sensor_a, sensor_b, region, N, R2, RMSE, Bias
    """
    if stations_csv is None:
        stations_csv = STATIONS_META
    meta = load_pm25_meta(stations_csv)

    # Station pixel → (row, col, region)
    station_rc: dict[str, tuple[int, int, str]] = {}
    for _, row in meta.iterrows():
        r, c = _latlon_rc(float(row['latitude']), float(row['longitude']))
        if 0 <= r < NLAT and 0 <= c < NLON:
            station_rc[row['stationName']] = (r, c, str(row['region']))
    if not station_rc:
        return pd.DataFrame()

    # Per (station, date, sensor) daily accumulators
    # daily[(station, date, sensor)] = [values from each valid slot]
    from collections import defaultdict
    daily: dict[tuple[str, date, str], list[float]] = defaultdict(list)

    sensor_vars = sorted({s for pair in sensor_pairs for s in pair})

    files = _merged_files_for_range(start, end)
    bar   = _make_bar(files, desc='Sensor overlap scan', unit='file', ncols=80)

    for fpath in (bar if bar is not None else files):
        slot = _parse_slot_utc(fpath)
        if slot is None:
            continue
        slot_date = slot.date()
        try:
            with nc.Dataset(fpath) as ds:
                available = set(ds.variables.keys())
                slot_data: dict[str, np.ndarray] = {}
                for s in sensor_vars:
                    if s in available:
                        slot_data[s] = np.ma.filled(
                            ds.variables[s][:].astype(np.float32), np.nan
                        )
        except Exception:
            continue

        for stn, (r, c, region) in station_rc.items():
            for s, grid in slot_data.items():
                v = float(grid[r, c])
                if np.isfinite(v) and v >= 0:
                    daily[(stn, slot_date, s)].append(v)

    if not daily:
        return pd.DataFrame()

    # Build a wide daily-mean DataFrame: rows = (station, date), cols = sensors
    records: list[dict] = []
    keys_by_sd: dict[tuple[str, date], dict[str, float]] = defaultdict(dict)
    for (stn, d_, s), vals in daily.items():
        keys_by_sd[(stn, d_)][s] = float(np.mean(vals))
    for (stn, d_), sd in keys_by_sd.items():
        region = station_rc[stn][2]
        rec = {'station_name': stn, 'date': d_, 'region': region}
        rec.update(sd)
        records.append(rec)
    daily_df = pd.DataFrame(records)

    rows: list[dict] = []
    for sen_a, sen_b in sensor_pairs:
        if sen_a not in daily_df.columns or sen_b not in daily_df.columns:
            continue
        for region in ('south', 'central', 'north'):
            sub = daily_df[(daily_df['region'] == region)
                           & daily_df[sen_a].notna()
                           & daily_df[sen_b].notna()]
            n = len(sub)
            if n < 10:
                continue
            a = sub[sen_a].to_numpy(dtype=np.float64)
            b = sub[sen_b].to_numpy(dtype=np.float64)
            r, _ = stats.pearsonr(a, b)
            r2   = float(r ** 2)
            rmse = float(np.sqrt(np.mean((a - b) ** 2)))
            bias = float(np.mean(a - b))
            baseline_key = (
                sen_a.replace('AOD_', ''),
                sen_b.replace('AOD_', ''),
                region,
            )
            baseline_r2 = NGUYEN2025_R2_BASELINES.get(baseline_key, np.nan)
            rows.append({
                'sensor_a':    sen_a,
                'sensor_b':    sen_b,
                'region':      region,
                'N':           int(n),
                'R2':          r2,
                'R2_baseline': baseline_r2,
                'R2_delta':    (r2 - baseline_r2)
                               if not np.isnan(baseline_r2) else np.nan,
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
        # Step in 30-min increments to match merged-file granularity
        for lag_steps in range(0, 49):  # 0–24 h
            t = pd.Timestamp(slot) - pd.Timedelta(minutes=30 * lag_steps)
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
    records     = []
    total_cells = NLAT * NLON

    all_days: list[date] = []
    d = start
    while d <= end:
        all_days.append(d)
        d += timedelta(days=1)

    bar = _make_bar(all_days, desc='Coverage scan', unit='day', ncols=80)

    for d in (bar if bar is not None else all_days):
        day_files = sorted(glob.glob(str(
            MERGED_DIR / d.strftime('%Y') / d.strftime('%m') /
            d.strftime('%d') / 'merged_*.nc'
        )))
        if not day_files:
            continue

        daily_valid = set()  # (row, col) cells valid in ANY slot
        for fpath in day_files:
            try:
                with nc.Dataset(fpath) as ds:
                    aod = np.ma.filled(ds.variables['AOD_merged'][:].astype(np.float32), np.nan)
                    valid_mask = np.isfinite(aod) & (aod >= 0)
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

    return pd.DataFrame(records)


# ── §8.4 Indirect validation via PM2.5 case studies ──────────────────────────
#
# AOD_phys_corrected = AOD_merged × (1 − RH/100)^0.6 / PBLH  [m⁻¹]
# Verified present in all merged NetCDFs with correct formula.
# Values are ~3×10⁻⁴ m⁻¹ (dimensionless AOD ÷ PBLH in metres).
# Pearson/Spearman/RANSAC are scale-invariant so the small magnitude is fine.

PM25_DIR      = DATA_ROOT / 'historical_full_v2'
STATIONS_META = Path('/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv')

# Nguyen 2025 §5.2 finding 9: daily Himawari RANSAC R² vs PM2.5 = 0.293
NGUYEN2025_PM25_RANSAC_R2 = 0.293
# Minimum fraction of study days with valid PM2.5 to include a station.
# Lowered from 0.85 — the test period (Jan 2025 – Apr 2026 = 510 days) had no
# Envisoft station meeting the 85 % bar, which is why §8.4 silently returned
# zero pairs.  0.50 keeps stations that cover at least half the period.
PM25_COMPLETENESS_MIN = 0.50


def _latlon_rc(lat: float, lon: float) -> tuple[int, int]:
    """Return (row, col) in the 0.05° config grid for an arbitrary lat/lon."""
    row = int(round((LAT_MAX - GRID_RES / 2 - lat) / GRID_RES))
    col = int(round((lon - LON_MIN - GRID_RES / 2) / GRID_RES))
    return row, col


def load_pm25_meta(stations_csv: Path = STATIONS_META) -> pd.DataFrame:
    """Load Envisoft station metadata with region and file-stem columns.

    Returns DataFrame with columns:
        stationId, stationName, latitude, longitude, region, file_stem
    where file_stem is the PM2.5 CSV basename without extension
    (stationName with ': ' → ' ').
    """
    df = pd.read_csv(stations_csv)
    df['region']    = df['latitude'].apply(_get_region)
    df['file_stem'] = df['stationName'].str.replace(': ', ' ', regex=False)
    return df


def _load_pm25_station(file_stem: str, pm25_dir: Path = PM25_DIR) -> pd.DataFrame:
    """Load and clean hourly PM2.5 for one Envisoft station.

    Returns DataFrame with columns [datetime, PM2.5], deduplicated,
    with sentinel values and out-of-range readings removed.
    """
    path = pm25_dir / f'{file_stem}.csv'
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, parse_dates=['Timestamp'])
    except Exception:
        return pd.DataFrame()
    df = df.rename(columns={'Timestamp': 'datetime'})
    df = df.replace([-9999, -999, 9999], np.nan)
    if 'PM2.5' in df.columns:
        df.loc[~df['PM2.5'].between(0, 500), 'PM2.5'] = np.nan
    df['_vc'] = df.notnull().sum(axis=1)
    df = (df.sort_values(['datetime', '_vc'], ascending=[True, False])
           .drop_duplicates(subset='datetime', keep='first')
           .drop(columns=['_vc'])
           .reset_index(drop=True))
    if 'PM2.5' not in df.columns:
        return pd.DataFrame()
    return df[['datetime', 'PM2.5']].dropna(subset=['PM2.5'])


def extract_aod_pm25_pairs(
    start: date,
    end: date,
    stations_csv: Path = STATIONS_META,
    pm25_dir: Path = PM25_DIR,
    completeness_min: float = PM25_COMPLETENESS_MIN,
) -> pd.DataFrame:
    """§8.4: Build daily (AOD_merged, AOD_phys_corrected, PM2.5) matched pairs.

    For each calendar day in [start, end]:
    1. Read all 30-min merged NetCDF slots → daily mean AOD_merged and
       AOD_phys_corrected at each station's nearest 0.05° grid cell.
       AOD_phys_corrected is the Step A3 field already written by run_stage_a.py
       (formula: AOD_merged × (1−RH/100)^0.6 / PBLH).
    2. Average hourly Envisoft PM2.5 to a daily mean.
    3. Merge on date; each row is one station × one day.

    Stations whose PM2.5 series covers fewer than
    completeness_min × total_study_days are dropped.

    Returns DataFrame with columns:
        date, station_name, region, season, month,
        aod_merged_daily, aod_phys_daily,
        pm25_daily, n_aod_slots, n_pm25_obs, confidence_flag_mode
    """
    meta = load_pm25_meta(stations_csv)

    pm25_by_stn: dict[str, pd.DataFrame]        = {}
    station_rc:  dict[str, tuple[int, int, str]] = {}

    for _, row in meta.iterrows():
        df_pm25 = _load_pm25_station(str(row['file_stem']), pm25_dir)
        if df_pm25.empty:
            continue
        mask = (
            (df_pm25['datetime'] >= pd.Timestamp(start)) &
            (df_pm25['datetime'] <  pd.Timestamp(end) + pd.Timedelta(days=1))
        )
        df_pm25 = df_pm25[mask].copy()
        if df_pm25.empty:
            continue
        r, c = _latlon_rc(float(row['latitude']), float(row['longitude']))
        if not (0 <= r < NLAT and 0 <= c < NLON):
            continue
        pm25_by_stn[row['stationName']] = df_pm25
        station_rc[row['stationName']]  = (r, c, str(row['region']))

    if not station_rc:
        return pd.DataFrame()

    all_days: list[date] = []
    d = start
    while d <= end:
        all_days.append(d)
        d += timedelta(days=1)

    records: list[dict] = []
    bar = _make_bar(all_days, desc='PM2.5 AOD scan', unit='day', ncols=80)

    for d in (bar if bar is not None else all_days):
        day_files = sorted(glob.glob(str(
            MERGED_DIR / d.strftime('%Y') / d.strftime('%m') /
            d.strftime('%d') / 'merged_*.nc'
        )))
        if not day_files:
            continue

        aod_m_acc: dict[str, list[float]] = {sn: [] for sn in station_rc}
        aod_p_acc: dict[str, list[float]] = {sn: [] for sn in station_rc}
        cflg_acc:  dict[str, list[int]]   = {sn: [] for sn in station_rc}

        for fpath in day_files:
            try:
                with nc.Dataset(fpath) as ds:
                    aod_m_g  = np.ma.filled(
                        ds.variables['AOD_merged'][:].astype(np.float32), np.nan)
                    has_phys = 'AOD_phys_corrected' in ds.variables
                    aod_p_g  = (
                        np.ma.filled(
                            ds.variables['AOD_phys_corrected'][:].astype(np.float32),
                            np.nan)
                        if has_phys else None
                    )
                    cflg_g   = np.ma.filled(
                        ds.variables['confidence_flag'][:].astype(np.int8), 0)
            except Exception:
                continue

            for sn, (r, c, _) in station_rc.items():
                vm = float(aod_m_g[r, c])
                if np.isfinite(vm) and vm >= 0:
                    aod_m_acc[sn].append(vm)
                    cflg_acc[sn].append(int(cflg_g[r, c]))
                    if aod_p_g is not None:
                        vp = float(aod_p_g[r, c])
                        aod_p_acc[sn].append(vp if np.isfinite(vp) else np.nan)
                    else:
                        aod_p_acc[sn].append(np.nan)

        day_ts      = pd.Timestamp(d)
        next_day_ts = day_ts + pd.Timedelta(days=1)

        for sn, (r, c, region) in station_rc.items():
            if not aod_m_acc[sn]:
                continue
            df_pm25 = pm25_by_stn.get(sn)
            if df_pm25 is None or df_pm25.empty:
                continue
            day_pm25 = df_pm25[
                (df_pm25['datetime'] >= day_ts) &
                (df_pm25['datetime'] <  next_day_ts)
            ]
            if day_pm25.empty or day_pm25['PM2.5'].isna().all():
                continue

            phys_vals = [v for v in aod_p_acc[sn] if np.isfinite(v)]
            cflg_arr  = np.clip(np.array(cflg_acc[sn], dtype=np.int64), 0, 10)
            cflg_mode = int(np.argmax(np.bincount(cflg_arr))) if len(cflg_arr) else 0

            records.append({
                'date':                 d,
                'station_name':         sn,
                'region':               region,
                'season':               'dry' if d.month in DRY_MONTHS else 'wet',
                'month':                d.month,
                'aod_merged_daily':     float(np.nanmean(aod_m_acc[sn])),
                'aod_phys_daily':       float(np.nanmean(phys_vals)) if phys_vals else np.nan,
                'pm25_daily':           float(day_pm25['PM2.5'].mean()),
                'n_aod_slots':          len(aod_m_acc[sn]),
                'n_pm25_obs':           int(day_pm25['PM2.5'].notna().sum()),
                'confidence_flag_mode': cflg_mode,
            })

    if not records:
        print(f'[extract_aod_pm25_pairs] no station-days produced — '
              f'check MERGED_DIR contents and station coordinates.')
        return pd.DataFrame()

    pairs_df = pd.DataFrame(records)

    if completeness_min > 0:
        study_days  = (end - start).days + 1
        day_counts  = pairs_df.groupby('station_name')['pm25_daily'].count()
        threshold   = completeness_min * study_days
        keep        = day_counts[day_counts >= threshold].index
        kept_pairs  = pairs_df[pairs_df['station_name'].isin(keep)]
        print(f'[extract_aod_pm25_pairs] {len(day_counts)} stations had pairs; '
              f'{len(keep)} pass completeness ≥ {completeness_min:.0%} '
              f'(≥ {int(threshold)} of {study_days} days).')
        if kept_pairs.empty and len(day_counts):
            top = day_counts.sort_values(ascending=False).head(5)
            print(f'  Top stations by N_days (none met threshold):')
            for sn, n in top.items():
                print(f'    {sn}: {int(n)}/{study_days} ({n/study_days:.0%})')
        pairs_df = kept_pairs

    return pairs_df.reset_index(drop=True)


def pm25_coupling_metrics(pairs: pd.DataFrame) -> pd.DataFrame:
    """§8.4: AOD–PM2.5 coupling metrics stratified by region, season, and station.

    Methodology (v3.2 — aligned with EDA_MODIS_AQI.ipynb):
      • Per-station strata: one row per (station, aod_type) with that station's
        own Pearson/Spearman/OLS/RANSAC fit.
      • Aggregate strata (ALL, region, region×season): compute the per-station
        fit *within* the stratum, then report the **mean and std across
        stations**.  The v3.1 pooled regression collapsed because
        between-station differences in mean PM2.5 (5–10 µg/m³) dominate the
        within-station AOD-PM2.5 signal.
      • 'ransac_r2_delta' = aggregate RANSAC R² minus the Nguyen 2025
        Himawari-only baseline (0.293).

    Returns a tidy DataFrame; aggregate rows carry extra `*_std` and
    `n_stations` columns.
    """

    def _fit_one(
        aod_col: str,
        sub: pd.DataFrame,
    ) -> Optional[dict]:
        """Per-station-or-stratum OLS+RANSAC+Pearson+Spearman fit, raw metrics."""
        df = sub[[aod_col, 'pm25_daily']].dropna()
        if len(df) < 10:
            return None
        x = df[aod_col].values.reshape(-1, 1)
        y = df['pm25_daily'].values

        ols    = LinearRegression().fit(x, y)
        ols_r2 = float(ols.score(x, y))

        try:
            ransac      = RANSACRegressor(min_samples=0.5, random_state=42)
            ransac.fit(x, y)
            inlier_mask      = ransac.inlier_mask_
            ransac_r2        = float(ransac.score(x[inlier_mask], y[inlier_mask]))
            inlier_frac      = float(inlier_mask.mean())
            ransac_slope     = float(ransac.estimator_.coef_[0])
            ransac_intercept = float(ransac.estimator_.intercept_)
        except Exception:
            ransac_r2 = inlier_frac = ransac_slope = ransac_intercept = np.nan

        pr, _ = stats.pearsonr(df[aod_col].values, y)
        sr, _ = stats.spearmanr(df[aod_col].values, y)

        return {
            'N':                len(df),
            'pearson_r':        float(pr),
            'spearman_r':       float(sr),
            'ols_r2':           ols_r2,
            'ransac_r2':        ransac_r2,
            'inlier_frac':      inlier_frac,
            'ransac_slope':     ransac_slope,
            'ransac_intercept': ransac_intercept,
        }

    def _aggregate_per_station(
        sub_df: pd.DataFrame,
        aod_col: str,
        label: str,
        **extra,
    ) -> Optional[dict]:
        """Run _fit_one per station within sub_df; return mean & std across stations."""
        per_station: list[dict] = []
        for _, grp in sub_df.groupby('station_name'):
            f = _fit_one(aod_col, grp)
            if f is not None:
                per_station.append(f)
        if not per_station:
            return None
        ps = pd.DataFrame(per_station)
        out = {
            'label':            label,
            'aod_type':         aod_col,
            'n_stations':       int(len(ps)),
            'N':                int(ps['N'].sum()),
            'pearson_r':        float(ps['pearson_r'].mean()),
            'pearson_r_std':    float(ps['pearson_r'].std()),
            'spearman_r':       float(ps['spearman_r'].mean()),
            'spearman_r_std':   float(ps['spearman_r'].std()),
            'ols_r2':           float(ps['ols_r2'].mean()),
            'ols_r2_std':       float(ps['ols_r2'].std()),
            'ransac_r2':        float(ps['ransac_r2'].mean()),
            'ransac_r2_std':    float(ps['ransac_r2'].std()),
            'ransac_r2_delta':  float(ps['ransac_r2'].mean() - NGUYEN2025_PM25_RANSAC_R2),
            'inlier_frac':      float(ps['inlier_frac'].mean()),
        }
        out.update(extra)
        return out

    rows: list[dict] = []
    aod_cols = ['aod_merged_daily', 'aod_phys_daily']

    # ALL — across every station
    for aod_col in aod_cols:
        r = _aggregate_per_station(pairs, aod_col, 'ALL')
        if r:
            rows.append(r)

    # Per region — across stations within region
    for region, grp in pairs.groupby('region'):
        for aod_col in aod_cols:
            r = _aggregate_per_station(grp, aod_col, f'region={region}',
                                       region=region)
            if r:
                rows.append(r)

    # Per region × season — across stations within stratum
    for (region, season), grp in pairs.groupby(['region', 'season']):
        for aod_col in aod_cols:
            r = _aggregate_per_station(
                grp, aod_col,
                f'region={region} season={season}',
                region=region, season=season,
            )
            if r:
                rows.append(r)

    # Per station — single fit, no across-station aggregation
    for sn, grp in pairs.groupby('station_name'):
        for aod_col in aod_cols:
            f = _fit_one(aod_col, grp)
            if f is None:
                continue
            f.update({
                'label':           f'station={sn}',
                'aod_type':        aod_col,
                'station_name':    sn,
                'n_stations':      1,
                'ransac_r2_delta': (f['ransac_r2'] - NGUYEN2025_PM25_RANSAC_R2)
                                   if not np.isnan(f['ransac_r2']) else np.nan,
            })
            rows.append(f)

    return pd.DataFrame(rows)


def pm25_case_studies(pairs: pd.DataFrame) -> pd.DataFrame:
    """§8.4: Evaluate 4 pollution episode types from paired (AOD, PM2.5) data.

    Episode types auto-detected from criteria in the pairs DataFrame:
    1. severe_hanoi_haze      — north, dry, PM2.5 > 100 µg/m³ and AOD_merged > 1.0
    2. biomass_burning_MarApr — March–April, PM2.5 > 50 µg/m³ (SEA transport)
    3. monsoon_gap_fill_stress — wet season, ≤ 5 valid AOD slots/day (cloud-dominated)
    4. precip_washout         — wet season, day-over-day PM2.5 drop ≥ 30 µg/m³

    For each episode group, Spearman rank correlation between aod_phys_daily and
    pm25_daily is computed per station (AOD_phys_corrected from Step A3).

    Pass criterion (§8.4): |spearman_r| > 0.3 for episodes 1, 2, 4;
    episode 3 tests gap-fill robustness so weaker coupling is expected.

    Returns one row per (episode_type, station_name).
    """
    if pairs.empty or 'aod_phys_daily' not in pairs.columns:
        return pd.DataFrame()

    pairs = pairs.copy()
    pairs['date'] = pd.to_datetime(pairs['date'])

    def _ep_row(sub: pd.DataFrame, ep_type: str, sn: str) -> Optional[dict]:
        df = sub[['aod_phys_daily', 'pm25_daily']].dropna()
        if len(df) < 3:
            return None
        sr, sp = stats.spearmanr(df['aod_phys_daily'], df['pm25_daily'])
        pr, _  = stats.pearsonr( df['aod_phys_daily'], df['pm25_daily'])
        return {
            'episode_type':    ep_type,
            'station_name':    sn,
            'region':          sub['region'].iloc[0] if 'region' in sub.columns else '',
            'N_days':          len(df),
            'spearman_r':      float(sr),
            'spearman_p':      float(sp),
            'pearson_r':       float(pr),
            'peak_aod_merged': float(sub['aod_merged_daily'].max()),
            'peak_pm25':       float(sub['pm25_daily'].max()),
            'mean_aod_phys':   float(df['aod_phys_daily'].mean()),
            'mean_pm25':       float(df['pm25_daily'].mean()),
        }

    rows: list[dict] = []

    # Episode 1 — severe Hanoi haze
    e1 = pairs[
        (pairs['region'] == 'north') &
        (pairs['season'] == 'dry') &
        (pairs['pm25_daily'] > 100) &
        (pairs['aod_merged_daily'] > 1.0)
    ]
    for sn, grp in e1.groupby('station_name'):
        r = _ep_row(grp, 'severe_hanoi_haze', str(sn))
        if r:
            rows.append(r)

    # Episode 2 — biomass-burning transport (March–April)
    e2 = pairs[
        (pairs['month'].isin([3, 4])) &
        (pairs['pm25_daily'] > 50)
    ]
    for sn, grp in e2.groupby('station_name'):
        r = _ep_row(grp, 'biomass_burning_MarApr', str(sn))
        if r:
            rows.append(r)

    # Episode 3 — monsoon gap-fill stress (≤ 5 valid AOD slots per day)
    e3 = pairs[
        (pairs['season'] == 'wet') &
        (pairs['n_aod_slots'] <= 5)
    ]
    for sn, grp in e3.groupby('station_name'):
        r = _ep_row(grp, 'monsoon_gap_fill_stress', str(sn))
        if r:
            rows.append(r)

    # Episode 4 — precipitation washout (PM2.5 drops ≥ 30 µg/m³ day-over-day)
    washout_parts: list[pd.DataFrame] = []
    for sn, grp in pairs[pairs['season'] == 'wet'].groupby('station_name'):
        grp_s = grp.sort_values('date').copy()
        grp_s['pm25_delta'] = grp_s['pm25_daily'].diff()
        washout_days = grp_s[grp_s['pm25_delta'] <= -30]['date']
        if not washout_days.empty:
            washout_parts.append(grp_s[grp_s['date'].isin(washout_days)])
    if washout_parts:
        e4 = pd.concat(washout_parts, ignore_index=True)
        for sn, grp in e4.groupby('station_name'):
            r = _ep_row(grp, 'precip_washout', str(sn))
            if r:
                rows.append(r)

    return pd.DataFrame(rows)
