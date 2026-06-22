"""Stage A validation — Section 8.1 of the thesis plan (v3.3.2).

§8.1.1  Held-out AERONET validation (the headline test)
        `extract_aeronet_pairs`, `stratified_metrics`.
        Matches `AOD_merged` at AERONET cells against AERONET ±30 min, then
        slices the metric panel (N, R, R², RMSE, MAE, Bias, %EE) by
        site × season × confidence_flag.

§8.1.2  Pre/post-correction comparison  → see `validate_bias_correction.ipynb`,
        which works off `bias_correction.CDFCorrection` directly.

§8.1.3  Inter-sensor consistency
        `inter_sensor_consistency(source='merged'|'gridded', …)` — per-region
        R² of daily means at Envisoft pixels, before (raw GRIDDED_DIR) and
        after (corrected MERGED_DIR) the §7.4 CDF correction.  Baselines from
        Nguyen 2025 §3.1.4 + §5.2 finding 5 are subtracted to report ΔR².

§8.1.4  Baseline comparison
        `baseline_comparison` — B1 (best single sensor) and B2
        (Gupta-2024-style equal-weight merge of the same bias-corrected
        per-sensor grids) at AERONET, on the same matched pairs.

§8.1.5  Precipitation-aware validation
        `precip_aware_validation` — three bins per the plan (dry > 24 h since
        rain, recovery 12–24 h, post-rain 0–12 h) from ERA5 Precip stored
        per slot.

§8.1.6  Case studies
        `pm25_case_studies` + `extract_aod_pm25_pairs` — Spearman r between
        merged AOD_phys and Envisoft PM2.5 over event windows defined in
        the plan (severe Hanoi haze, March–April biomass burning,
        precipitation washout).
"""

from __future__ import annotations
from datetime import date, datetime, timedelta
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
    GRIDDED_DIR, MERGED_DIR, TZ_OFFSET_HOURS,
    LATS, LONS, NLAT, NLON, GRID_RES, LAT_MAX, LON_MIN,
    DATA_ROOT,
    TRAIN_START, TRAIN_END, TEST_START, TEST_END,
)
from aeronet import load_all_aeronet

_TZ = pd.Timedelta(hours=TZ_OFFSET_HOURS)  # UTC → UTC+7 (AERONET datetimes are UTC+7)

# Expected-error envelope (MODIS Deep Blue / DT land standard)
EE_OFFSET   = 0.05
EE_SLOPE    = 0.15

# Nguyen 2025 inter-sensor R² baselines, keyed by (sensor_a, sensor_b, region).
# Source: Nguyen 2025 §3.1.4 and §5.2 finding 5.  v3.3.2 collapses the L2/L3
# Himawari grids into one merged channel, so 'himawari' is the key downstream
# code uses.  Pairs not separately reported in Nguyen 2025 (e.g. MODIS-Himawari
# north was reported, VIIRS-Himawari north was not) are simply absent.
NGUYEN2025_R2_BASELINES = {
    ('modis_maiac',  'himawari',    'north'):   0.621,
    ('modis_maiac',  'himawari',    'central'): 0.474,
    ('modis_maiac',  'himawari',    'south'):   0.756,
    ('viirs_noaa20', 'himawari',    'central'): 0.450,
    ('viirs_noaa20', 'himawari',    'south'):   0.756,
    ('modis_maiac',  'viirs_noaa20','north'):   0.837,
    ('modis_maiac',  'viirs_noaa20','central'): 0.638,
    ('modis_maiac',  'viirs_noaa20','south'):   0.545,
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
    """Parse UTC slot datetime from a Stage A slot NetCDF filename.

    Accepts either `merged_YYYYMMDD_HHMM` (MERGED_DIR) or
    `gridded_YYYYMMDD_HHMM` (GRIDDED_DIR) stems.
    """
    stem = Path(fpath).stem
    for prefix in ('merged_', 'gridded_'):
        if stem.startswith(prefix):
            stem = stem[len(prefix):]
            break
    try:
        return datetime.strptime(stem, '%Y%m%d_%H%M')
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


# ── §8.1.3 Inter-sensor consistency (internal uncertainty) ───────────────────

DEFAULT_SENSOR_PAIRS = (
    ('AOD_modis_maiac',  'AOD_himawari'),
    ('AOD_viirs_noaa20', 'AOD_himawari'),
    ('AOD_viirs_snpp',   'AOD_himawari'),
    ('AOD_viirs_noaa20', 'AOD_modis_maiac'),
    ('AOD_viirs_snpp',   'AOD_modis_maiac'),
    ('AOD_viirs_noaa20', 'AOD_viirs_snpp'),
)


def _slot_files_for_range(source: str, start: date, end: date) -> list[str]:
    """Discover slot NetCDFs for either gridded (raw) or merged (post-correction)."""
    base = GRIDDED_DIR if source == 'gridded' else MERGED_DIR
    stem = 'gridded_' if source == 'gridded' else 'merged_'
    files: list[str] = []
    d = start
    while d <= end:
        pattern = str(base / d.strftime('%Y') / d.strftime('%m')
                      / d.strftime('%d') / f'{stem}*.nc')
        files.extend(glob.glob(pattern))
        d += timedelta(days=1)
    return sorted(files)


def _baseline_lookup(sensor_a: str, sensor_b: str, region: str) -> float:
    """Look up Nguyen 2025 R² baseline; sensor pair is order-invariant."""
    a = sensor_a.replace('AOD_', '')
    b = sensor_b.replace('AOD_', '')
    v = NGUYEN2025_R2_BASELINES.get((a, b, region),
          NGUYEN2025_R2_BASELINES.get((b, a, region), np.nan))
    return float(v) if v is not None else np.nan


def inter_sensor_consistency(
    start: date,
    end:   date,
    sensor_pairs: list[tuple[str, str]] = DEFAULT_SENSOR_PAIRS,
    source: str = 'merged',
    stations_csv: Path = None,
) -> pd.DataFrame:
    """§8.1.3: inter-sensor R² by region using daily means at Envisoft pixels.

    Parameters
    ----------
    source : {'merged', 'gridded'}
        - 'merged'  → read post-correction `AOD_<sensor>` from MERGED_DIR.
        - 'gridded' → read raw `AOD_<sensor>` from GRIDDED_DIR (Stage A2 output,
          before §7.4 CDF correction).  In gridded mode Himawari is split into
          `AOD_himawari_l2` and `AOD_himawari_l3`; pass either explicitly via
          `sensor_pairs`.

    Methodology
    -----------
      1. Extract each sensor's AOD at every Envisoft station pixel, every slot.
      2. Aggregate to daily means per (station, date, sensor).
      3. For each (sensor_a, sensor_b, region), regress all (station, date)
         daily-mean pairs within that region.

    Station-level daily aggregation matches Nguyen 2025 §3.1.4; pooling raw
    pixel pairs would be dominated by QA-edge artefacts.

    Returns DataFrame: sensor_a, sensor_b, region, source, N, R2, R2_baseline,
                       R2_delta, RMSE, Bias.
    """
    if source not in ('merged', 'gridded'):
        raise ValueError(f"source must be 'merged' or 'gridded', got {source!r}")

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
    from collections import defaultdict
    daily: dict[tuple[str, date, str], list[float]] = defaultdict(list)

    sensor_vars = sorted({s for pair in sensor_pairs for s in pair})

    files = _slot_files_for_range(source, start, end)
    bar   = _make_bar(files, desc=f'Sensor overlap scan ({source})',
                     unit='file', ncols=80)

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
            baseline_r2 = _baseline_lookup(sen_a, sen_b, region)
            rows.append({
                'sensor_a':    sen_a,
                'sensor_b':    sen_b,
                'region':      region,
                'source':      source,
                'N':           int(n),
                'R2':          r2,
                'R2_baseline': baseline_r2,
                'R2_delta':    (r2 - baseline_r2)
                               if not np.isnan(baseline_r2) else np.nan,
                'RMSE':        rmse,
                'Bias':        bias,
            })

    return pd.DataFrame(rows)


# ── §8.1.5 Precipitation-aware validation ─────────────────────────────────────

def precip_label_pairs(pairs: pd.DataFrame) -> pd.DataFrame:
    """Annotate matched-pair rows with `hours_since_rain` and `precip_bin`.

    Reads ERA5_Precip from each slot's merged NetCDF at the station pixel,
    then for every matched pair walks back 24 h in 30-min steps to find the
    most recent slot with precipitation ≥ 0.1 mm/h.

    Adds two columns and returns a new DataFrame:
        hours_since_rain  : float, np.inf if no rain in lookback window
        precip_bin        : 'post-rain' (≤12 h), 'recovery' (12-24 h), 'dry' (>24 h)
    """
    station_rc = {site: _station_rc(site) for site in AERONET_SITES}
    slot_precip: dict[tuple[str, datetime], float] = {}

    unique_slots = pairs['slot_utc'].unique()
    for slot in unique_slots:
        s = pd.Timestamp(slot)
        fpath = str(MERGED_DIR / s.strftime('%Y') / s.strftime('%m') /
                    s.strftime('%d') / f'merged_{s.strftime("%Y%m%d_%H%M")}.nc')
        try:
            with nc.Dataset(fpath) as ds:
                if 'ERA5_Precip' not in ds.variables:
                    continue
                precip = np.ma.filled(ds.variables['ERA5_Precip'][:].astype(np.float32),
                                       np.nan)
                for site, (row, col) in station_rc.items():
                    if 0 <= row < NLAT and 0 <= col < NLON:
                        slot_precip[(site, slot)] = float(precip[row, col])
        except Exception:
            continue

    if not slot_precip:
        out = pairs.copy()
        out['hours_since_rain'] = np.nan
        out['precip_bin']       = 'unknown'
        return out

    rain_thresh = 0.1  # mm/h
    def _hours_since(site: str, slot) -> float:
        for lag in range(0, 49):                      # 0–24 h in 30-min steps
            t = pd.Timestamp(slot) - pd.Timedelta(minutes=30 * lag)
            prec = slot_precip.get((site, t.to_pydatetime()
                                    if hasattr(t, 'to_pydatetime') else t))
            if prec is not None and np.isfinite(prec) and prec >= rain_thresh:
                return lag / 2.0
        return np.inf

    def _bin(h: float) -> str:
        if not np.isfinite(h):
            return 'dry'
        if h <= 12.0:
            return 'post-rain'
        if h <= 24.0:
            return 'recovery'
        return 'dry'

    out = pairs.copy()
    out['hours_since_rain'] = out.apply(lambda r: _hours_since(r['site'], r['slot_utc']),
                                        axis=1)
    out['precip_bin']       = out['hours_since_rain'].apply(_bin)
    return out


def precip_aware_validation(pairs: pd.DataFrame) -> pd.DataFrame:
    """§8.1.5: metric panel per (site × precip_bin).

    See `precip_label_pairs` for the binning rule (dry > 24 h since rain,
    recovery 12-24 h, post-rain 0-12 h).  Bins with < 3 pairs are dropped.
    """
    labelled = precip_label_pairs(pairs)
    if 'precip_bin' not in labelled.columns or (labelled['precip_bin'] == 'unknown').all():
        return pd.DataFrame()

    rows = []
    for bin_lbl in ('dry', 'recovery', 'post-rain'):
        for site in AERONET_SITES:
            sub = labelled[(labelled['site'] == site) & (labelled['precip_bin'] == bin_lbl)]
            if len(sub) < 3:
                continue
            m = compute_metrics(sub['merged_aod'].values, sub['aeronet_aod'].values,
                                 label=f'{site} {bin_lbl}')
            m['site']       = site
            m['precip_bin'] = bin_lbl
            rows.append(m)
    return pd.DataFrame(rows)


# ── §8.1.4 Baseline comparison (B1 single-sensor, B2 equal-weight, B3 ICW) ───

def compute_per_sensor_aeronet_rmse(
    start: date,
    end:   date,
    sensor_keys: tuple[str, ...] = (
        'AOD_viirs_noaa20', 'AOD_viirs_snpp',
        'AOD_modis_maiac',  'AOD_himawari',
    ),
    window_min: int = 30,
) -> dict[str, float]:
    """Per-sensor RMSE vs AERONET on [start, end] — feeds B3 (Ahn 2021 ICW).

    Ahn 2021 §2.2.3 defines ICW weights as w_i = (1/RMSE_i) / Σ(1/RMSE_j) with
    one RMSE per sensor (no region/season stratification).  This helper
    reproduces that recipe: for every merged NetCDF in the window, sample
    `AOD_<sensor>` at each AERONET station pixel, pair with AERONET ±window_min
    min (same matching rule as `extract_aeronet_pairs`), then collapse to one
    global RMSE per sensor.

    Should be called on the *training* window — the held-out window is reserved
    for B3 evaluation.
    """
    aer_all     = load_all_aeronet()
    aer_by_site = {site: grp.copy() for site, grp in aer_all.groupby('site')}
    station_rc  = {site: _station_rc(site) for site in AERONET_SITES}

    sq: dict[str, list[float]] = {sk: [] for sk in sensor_keys}
    files = _merged_files_for_range(start, end)
    bar   = _make_bar(files, desc='B3 per-sensor RMSE', unit='file', ncols=80)

    for fpath in (bar if bar is not None else files):
        slot = _parse_slot_utc(fpath)
        if slot is None:
            continue
        try:
            with nc.Dataset(fpath) as ds:
                available = set(ds.variables.keys())
                grids: dict[str, np.ndarray] = {}
                for sk in sensor_keys:
                    if sk in available:
                        grids[sk] = np.ma.filled(
                            ds.variables[sk][:].astype(np.float32), np.nan)
        except Exception:
            continue
        if not grids:
            continue

        slot_local_ts = pd.Timestamp(slot) + _TZ
        delta         = pd.Timedelta(minutes=window_min)
        for site, (row, col) in station_rc.items():
            if not (0 <= row < NLAT and 0 <= col < NLON):
                continue
            site_df = aer_by_site.get(site)
            if site_df is None or site_df.empty:
                continue
            in_win = site_df[
                (site_df['datetime'] >= slot_local_ts - delta) &
                (site_df['datetime'] <= slot_local_ts + delta)
            ]
            if in_win.empty:
                continue
            aer = float(in_win['aod_550'].mean())
            for sk, g in grids.items():
                v = float(g[row, col])
                if np.isfinite(v) and v >= 0:
                    sq[sk].append((v - aer) ** 2)

    return {sk: float(np.sqrt(np.mean(d))) if d else float('nan')
            for sk, d in sq.items()}


def baseline_comparison(
    pairs: pd.DataFrame,
    sensor_keys: tuple[str, ...] = (
        'AOD_viirs_noaa20', 'AOD_viirs_snpp',
        'AOD_modis_maiac',  'AOD_himawari',
    ),
    merged_nc_dir: Path = MERGED_DIR,
    b3_weights: Optional[dict[str, float]] = None,
) -> pd.DataFrame:
    """§8.1.4 B1/B2/B3: merged vs per-sensor / equal-weight / Ahn-2021-ICW.

    For every (slot, site) pair already matched in `pairs` (see §8.1.1
    `extract_aeronet_pairs`), reopen the merged NetCDF and read each per-sensor
    bias-corrected grid at the station pixel.  Then:

      * B1 — single-sensor:  each `AOD_<sensor>` evaluated against AERONET.
        Best single sensor (typically VIIRS NOAA-20) is the §8.1.4 B1 baseline.
      * B2 — equal-weight (Gupta 2024 §4):  arithmetic mean of valid sensors
        at the cell.  Same inputs as the thesis fusion but uniform weights.
      * B3 — Ahn 2021 §2.2.3 ICW:  Σ wᵢ · AODᵢ with wᵢ ∝ 1/RMSEᵢ over sensors
        valid at the cell.  RMSEᵢ is the global per-sensor AERONET RMSE passed
        in `b3_weights` (compute once on the training window via
        `compute_per_sensor_aeronet_rmse`).  B3 rows are emitted only when
        `b3_weights` is provided.
      * Reference — `merged`:  the thesis TC-weighted AOD_merged from `pairs`.

    Returns rows per (site, season, product) and per (site, season='ALL',
    product).  Columns: standard `compute_metrics` output plus `product`,
    `site`, `season`.
    """
    station_rc = {site: _station_rc(site) for site in AERONET_SITES}
    records: list[dict] = []

    for _, row in pairs.iterrows():
        site   = row['site']
        season = row.get('season', 'ALL')
        slot   = pd.Timestamp(row['slot_utc'])
        fpath  = str(merged_nc_dir / slot.strftime('%Y') / slot.strftime('%m') /
                     slot.strftime('%d') / f"merged_{slot.strftime('%Y%m%d_%H%M')}.nc")

        per_sen: dict[str, float] = {}
        try:
            with nc.Dataset(fpath) as ds:
                available = set(ds.variables.keys())
                for sk in sensor_keys:
                    if sk in available:
                        grid = np.ma.filled(
                            ds.variables[sk][:].astype(np.float32), np.nan)
                        v = float(grid[station_rc[site]])
                        if np.isfinite(v) and v >= 0:
                            per_sen[sk] = v
        except Exception:
            continue

        base = {'site': site, 'season': season,
                'aer':  float(row['aeronet_aod'])}

        records.append({**base, 'product': 'merged',
                        'sat': float(row['merged_aod'])})
        for sk in sensor_keys:
            records.append({**base, 'product': sk,
                            'sat': per_sen.get(sk, np.nan)})
        records.append({**base, 'product': 'B2_equal_weight',
                        'sat': float(np.mean(list(per_sen.values())))
                               if per_sen else np.nan})

        if b3_weights is not None:
            valid = {sk: v for sk, v in per_sen.items()
                     if sk in b3_weights
                     and np.isfinite(b3_weights[sk])
                     and b3_weights[sk] > 0}
            if valid:
                inv = {sk: 1.0 / b3_weights[sk] for sk in valid}
                tot = sum(inv.values())
                icw = sum(inv[sk] / tot * valid[sk] for sk in valid)
            else:
                icw = np.nan
            records.append({**base, 'product': 'B3_ahn2021_icw', 'sat': icw})

    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)

    rows: list[dict] = []
    for (site, season, product), grp in df.groupby(['site', 'season', 'product']):
        mask = np.isfinite(grp['sat'].values) & np.isfinite(grp['aer'].values)
        if mask.sum() < 3:
            continue
        m = compute_metrics(grp['sat'].values[mask], grp['aer'].values[mask],
                             label=f'{site} {season} {product}')
        m.update({'site': site, 'season': season, 'product': product})
        rows.append(m)
    # Season-aggregated rows
    for (site, product), grp in df.groupby(['site', 'product']):
        mask = np.isfinite(grp['sat'].values) & np.isfinite(grp['aer'].values)
        if mask.sum() < 3:
            continue
        m = compute_metrics(grp['sat'].values[mask], grp['aer'].values[mask],
                             label=f'{site} ALL {product}')
        m.update({'site': site, 'season': 'ALL', 'product': product})
        rows.append(m)

    return pd.DataFrame(rows)


# ── §8.6 RANSAC diagnostic ────────────────────────────────────────────────────

def ransac_diagnostic(pairs: pd.DataFrame) -> pd.DataFrame:
    """§8.6: OLS vs RANSAC regression for (merged_aod, aeronet_aod) pairs.

    Quantifies outlier influence — the merged product is NOT filtered,
    this is a diagnostic only.

    Stratification (one row per stratum):
      • site   ∈ AERONET_SITES ∪ {ALL}
      • region ∈ {north, central, south, ALL}
      • season ∈ {dry, wet, ALL}
    The 'ALL' label means the stratum aggregates that axis.
    """
    def _fit(sub: pd.DataFrame, label: dict) -> Optional[dict]:
        sat = sub['merged_aod'].values.reshape(-1, 1)
        aer = sub['aeronet_aod'].values
        mask = np.isfinite(sat.ravel()) & np.isfinite(aer)
        sat  = sat[mask];  aer = aer[mask]
        if len(sat) < 10:
            return None

        ols    = LinearRegression().fit(sat, aer)
        ols_r2 = float(ols.score(sat, aer))

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

        out = {
            'N':                len(sat),
            'OLS_R2':           ols_r2,
            'RANSAC_R2':        ransac_r2,
            'RANSAC_R2_lift':   float(ransac_r2 - ols_r2) if not np.isnan(ransac_r2) else np.nan,
            'inlier_frac':      inlier_frac,
            'ransac_slope':     slope,
            'ransac_intercept': intercept,
        }
        out.update(label)
        return out

    rows: list[dict] = []

    # Per site (incl. ALL)
    for site in (list(AERONET_SITES.keys()) + ['ALL']):
        sub = pairs if site == 'ALL' else pairs[pairs['site'] == site]
        r = _fit(sub, {'site': site, 'region': 'ALL', 'season': 'ALL'})
        if r: rows.append(r)

    # Per site × season
    if 'season' in pairs.columns:
        for (site, season), sub in pairs.groupby(['site', 'season']):
            r = _fit(sub, {'site': str(site), 'region': 'ALL', 'season': str(season)})
            if r: rows.append(r)

    # Per region
    if 'region' in pairs.columns:
        for region, sub in pairs.groupby('region'):
            r = _fit(sub, {'site': 'ALL', 'region': str(region), 'season': 'ALL'})
            if r: rows.append(r)

    # Per region × season
    if {'region', 'season'}.issubset(pairs.columns):
        for (region, season), sub in pairs.groupby(['region', 'season']):
            r = _fit(sub, {'site': 'ALL', 'region': str(region), 'season': str(season)})
            if r: rows.append(r)

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

    Methodology (aligned with EDA_MODIS_AQI.ipynb):
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
