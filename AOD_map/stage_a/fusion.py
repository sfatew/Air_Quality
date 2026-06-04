"""Step A5: Inverse Composite Weighting (ICW) sensor fusion.

Following Ahn et al. 2021 (Eq. 3):

    w_i = 1 / RMSE_i²
    AOD_merged = Σ(w_i × AOD_i_corrected) / Σ(w_i)

Weights are per (sensor, region, season) stratum.  Initial RMSE priors come
from Nguyen et al. 2025 (config.SENSOR_RMSE_PRIOR); after bias correction,
updated post-correction RMSE values should replace these priors and be saved
to the same BIASC_DIR as the CDFCorrection objects.

Additional sensor inclusion rules (§7.5 of the thesis plan):
    • VIIRS is always included when present (highest accuracy anchor).
    • MODIS MAIAC is excluded from the south via MODIS_SOUTH_WEIGHT_FACTOR.
    • Himawari is included when SZA < 70° (enforced in himawari.py before fusion).

Output per grid cell:
    aod_merged      – ICW weighted mean AOD
    aod_std         – unweighted cross-sensor spread (sqrt of weighted variance)
    n_sensors       – number of sensors contributing
    dominant_sensor – integer code for the sensor with the highest weight
    confidence_flag – 0–4 (see config.CONFIDENCE_FLAG)
"""

from __future__ import annotations
from typing import Optional
import json
from pathlib import Path

import numpy as np

try:
    import cupy as cp
    cp.array([0])  # triggers CUDA init — raises if no GPU device
    _xp = cp
except Exception:
    cp = None
    _xp = np

from config import (
    SENSOR_RMSE_PRIOR,
    MODIS_SOUTH_WEIGHT_FACTOR,
    HIMAWARI_WET_WEIGHT_FACTOR,
    SENSOR_RMSE_FLOOR,
    CONFIDENCE_FLAG,
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
    NLAT, NLON, BIASC_DIR, DRY_MONTHS,
)

# Integer codes for dominant_sensor output variable
SENSOR_CODES = {
    'himawari_l2': 1,
    'himawari_l3': 2,
    'modis_maiac': 3,
    'viirs_snpp':  4,
    'viirs_noaa20':5,
}
SENSOR_KEYS = list(SENSOR_CODES.keys())

_RMSE_CACHE_FILE = BIASC_DIR / 'post_correction_rmse.json'


# ── RMSE loading ──────────────────────────────────────────────────────────────

def load_rmse(
    use_post_correction: bool = True,
) -> dict[tuple, float]:
    """Return RMSE dict keyed by (sensor, region) or (sensor, region, season).

    Tries to load post-correction RMSE from the JSON cache first; falls back
    to the prior values from config.py if the cache is missing.
    The JSON may contain season-stratified keys written by run_collocate.py.
    """
    if use_post_correction and _RMSE_CACHE_FILE.exists():
        try:
            raw = json.loads(_RMSE_CACHE_FILE.read_text())
            return {tuple(k.split('|')): v for k, v in raw.items()}
        except Exception:
            pass
    return dict(SENSOR_RMSE_PRIOR)


def save_rmse(rmse_dict: dict[tuple, float]) -> None:
    """Persist post-correction RMSE values to JSON.

    Accepts tuples of length 2 (sensor, region) or 3 (sensor, region, season).
    """
    _RMSE_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    raw = {'|'.join(k): v for k, v in rmse_dict.items()}
    _RMSE_CACHE_FILE.write_text(json.dumps(raw, indent=2))


# ── Confidence flag helpers ───────────────────────────────────────────────────

def _assign_confidence(
    sensor_has_data: dict[str, np.ndarray],
) -> np.ndarray:
    """Assign per-cell confidence flags based on which sensors contributed."""
    shape = (NLAT, NLON)
    flag  = np.zeros(shape, dtype=np.int8)

    leo_keys = ('modis_maiac', 'viirs_snpp', 'viirs_noaa20')
    hi_keys  = ('himawari_l2', 'himawari_l3')

    has_hi  = np.zeros(shape, dtype=bool)
    has_leo = np.zeros(shape, dtype=bool)
    leo_count = np.zeros(shape, dtype=np.int8)

    for k in hi_keys:
        if k in sensor_has_data:
            has_hi |= sensor_has_data[k]
    for k in leo_keys:
        if k in sensor_has_data:
            has_leo |= sensor_has_data[k]
            leo_count[sensor_has_data[k]] += 1

    flag[has_hi & ~has_leo]              = CONFIDENCE_FLAG['himawari_only']
    flag[~has_hi & has_leo]             = CONFIDENCE_FLAG['leo_only']
    flag[has_hi & has_leo & (leo_count < 2)] = CONFIDENCE_FLAG['himawari_plus_leo']
    flag[has_hi & has_leo & (leo_count >= 2)] = CONFIDENCE_FLAG['multi_leo_himawari']
    return flag


# ── Main fusion function ──────────────────────────────────────────────────────

def fuse(
    sensor_grids: dict[str, np.ndarray],
    month: int,
    lat_2d: np.ndarray,
    rmse_dict: Optional[dict[tuple[str, str], float]] = None,
) -> dict[str, np.ndarray]:
    """ICW fusion of bias-corrected sensor grids into a single merged AOD.

    Parameters
    ----------
    sensor_grids : dict mapping sensor_key → (NLAT, NLON) bias-corrected
                   float32 AOD array (NaN = no data for this sensor/cell).
    month        : calendar month (1–12) used to select season stratum.
    lat_2d       : (NLAT, NLON) latitude array for region selection.
    rmse_dict    : optional pre-loaded RMSE values; if None, loads from disk.

    Returns a dict with (NLAT, NLON) arrays:
        aod_merged      float32
        aod_std         float32
        n_sensors       int8
        dominant_sensor int8   (SENSOR_CODES values)
        confidence_flag int8
    """
    if rmse_dict is None:
        rmse_dict = load_rmse()

    xp     = _xp
    season = 'dry' if month in DRY_MONTHS else 'wet'
    shape  = (NLAT, NLON)

    # Region codes on GPU: 0=south, 1=central, 2=north (avoids string arrays)
    region_code = np.where(lat_2d >= NORTH_CENTRAL_LAT, 2,
                           np.where(lat_2d < CENTRAL_SOUTH_LAT, 0, 1)).astype(np.int8)
    region_code_d = xp.asarray(region_code)

    weighted_sum_d  = xp.zeros(shape, dtype=np.float64)
    weight_total_d  = xp.zeros(shape, dtype=np.float64)
    weighted_sq_d   = xp.zeros(shape, dtype=np.float64)
    dominant_wt_d   = xp.zeros(shape, dtype=np.float64)
    dominant_code_d = xp.zeros(shape, dtype=np.int8)
    n_sensors_d     = xp.zeros(shape, dtype=np.int8)
    sensor_has_data: dict[str, np.ndarray] = {}

    _reg_codes = ((0, 'south'), (1, 'central'), (2, 'north'))

    for sensor, aod in sensor_grids.items():
        if aod is None:
            continue
        has = np.isfinite(aod) & (aod >= 0)
        sensor_has_data[sensor] = has
        if not np.any(has):
            continue

        # Build per-cell RMSE on GPU using integer region codes.
        # Look up season-specific key first, fall back to region-only, then 'north'.
        rmse_arr_d = xp.full(shape, np.nan, dtype=np.float64)
        for code, reg in _reg_codes:
            rmse_val = rmse_dict.get(
                (sensor, reg, season),
                rmse_dict.get((sensor, reg),
                              rmse_dict.get((sensor, 'north'), np.nan)),
            )
            if not np.isnan(rmse_val):
                rmse_val = max(float(rmse_val), SENSOR_RMSE_FLOOR)
            rmse_arr_d[region_code_d == code] = rmse_val

        # ICW weight = 1 / RMSE²; apply per-sensor down-weight factors
        w_d = xp.where(xp.isfinite(rmse_arr_d) & (rmse_arr_d > 0),
                       1.0 / rmse_arr_d ** 2, 0.0)
        if sensor == 'modis_maiac':
            w_d = xp.where(region_code_d == 0, w_d * MODIS_SOUTH_WEIGHT_FACTOR, w_d)
        if sensor in ('himawari_l2', 'himawari_l3') and season == 'wet':
            w_d = w_d * HIMAWARI_WET_WEIGHT_FACTOR

        aod_d     = xp.asarray(aod)
        has_d     = xp.asarray(has)
        valid_w_d = has_d & (w_d > 0)
        s_code    = SENSOR_CODES.get(sensor, 0)

        weighted_sum_d[valid_w_d]  += w_d[valid_w_d] * aod_d[valid_w_d]
        weight_total_d[valid_w_d]  += w_d[valid_w_d]
        weighted_sq_d[valid_w_d]   += w_d[valid_w_d] * aod_d[valid_w_d] ** 2
        n_sensors_d[valid_w_d]     += 1

        update_d = valid_w_d & (w_d > dominant_wt_d)
        dominant_wt_d[update_d]   = w_d[update_d]
        dominant_code_d[update_d] = s_code

    has_any_d = weight_total_d > 0
    safe_w_d  = xp.where(has_any_d, weight_total_d, 1.0)
    aod_merged = np.asarray(
        xp.where(has_any_d, weighted_sum_d / safe_w_d, np.nan).astype(np.float32)
    )

    variance_d = xp.where(
        has_any_d & (n_sensors_d > 1),
        weighted_sq_d / safe_w_d - (weighted_sum_d / safe_w_d) ** 2,
        np.nan,
    )
    variance_d = xp.clip(variance_d, 0, None)
    aod_std = np.asarray(
        xp.where(xp.isfinite(variance_d), xp.sqrt(variance_d), np.nan).astype(np.float32)
    )

    conf_flag = _assign_confidence(sensor_has_data)

    return {
        'aod_merged':      aod_merged,
        'aod_std':         aod_std,
        'n_sensors':       np.asarray(n_sensors_d),
        'dominant_sensor': np.asarray(dominant_code_d),
        'confidence_flag': conf_flag,
    }
