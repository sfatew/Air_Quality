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

from config import (
    SENSOR_RMSE_PRIOR,
    MODIS_SOUTH_WEIGHT_FACTOR,
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
) -> dict[tuple[str, str], float]:
    """Return RMSE dict keyed by (sensor, region).

    Tries to load post-correction RMSE from the JSON cache first; falls back
    to the prior values from config.py if the cache is missing.
    """
    if use_post_correction and _RMSE_CACHE_FILE.exists():
        try:
            raw = json.loads(_RMSE_CACHE_FILE.read_text())
            return {tuple(k.split('|')): v for k, v in raw.items()}
        except Exception:
            pass
    return dict(SENSOR_RMSE_PRIOR)


def save_rmse(rmse_dict: dict[tuple[str, str], float]) -> None:
    """Persist updated post-correction RMSE values to JSON."""
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


# ── Region weight modifier ────────────────────────────────────────────────────

def _region_weight_modifier(
    sensor: str,
    lat_2d: np.ndarray,
) -> np.ndarray:
    """Return a (NLAT, NLON) multiplicative modifier for sensor–region interactions.

    Applies:
      • MODIS MAIAC in the south: multiply weight by MODIS_SOUTH_WEIGHT_FACTOR
        (R=0.41 at Bac Lieu makes it unreliable in the Mekong Delta).
    """
    modifier = np.ones((NLAT, NLON), dtype=np.float32)
    if sensor == 'modis_maiac':
        south = lat_2d < CENTRAL_SOUTH_LAT
        modifier[south] = MODIS_SOUTH_WEIGHT_FACTOR
    return modifier


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

    season = 'dry' if month in DRY_MONTHS else 'wet'
    shape  = (NLAT, NLON)

    # --- build per-sensor weight grids and track presence ---
    weighted_sum  = np.zeros(shape, dtype=np.float64)
    weight_total  = np.zeros(shape, dtype=np.float64)
    weighted_sq   = np.zeros(shape, dtype=np.float64)   # for std dev
    dominant_wt   = np.zeros(shape, dtype=np.float64)
    dominant_code = np.zeros(shape, dtype=np.int8)
    n_sensors     = np.zeros(shape, dtype=np.int8)
    sensor_has_data: dict[str, np.ndarray] = {}

    # Assign a coarse region label per grid cell for RMSE look-up
    region_grid = np.where(
        lat_2d >= NORTH_CENTRAL_LAT, 'north',
        np.where(lat_2d < CENTRAL_SOUTH_LAT, 'south', 'central')
    )

    for sensor, aod in sensor_grids.items():
        if aod is None:
            continue
        has = np.isfinite(aod) & (aod >= 0)
        sensor_has_data[sensor] = has

        if not np.any(has):
            continue

        # Look up RMSE per cell using vectorised region mapping
        # Majority of cells: look up from 3 possible region values
        rmse_arr = np.full(shape, np.nan, dtype=np.float64)
        for reg in ('north', 'central', 'south'):
            key = (sensor, reg)
            rmse_val = rmse_dict.get(key, rmse_dict.get((sensor, 'north'), np.nan))
            rmse_arr[region_grid == reg] = rmse_val

        # ICW weight = 1 / RMSE²
        w_base = np.where(np.isfinite(rmse_arr) & (rmse_arr > 0),
                          1.0 / rmse_arr**2, 0.0)
        # Region modifier (e.g. MAIAC downweighted in south)
        w = w_base * _region_weight_modifier(sensor, lat_2d)

        valid_w = has & (w > 0)
        code = SENSOR_CODES.get(sensor, 0)

        np.add.at(weighted_sum,  np.where(valid_w, 1, 0).astype(bool), 0)  # dummy
        # Vectorised update
        weighted_sum[valid_w]  += w[valid_w] * aod[valid_w]
        weight_total[valid_w]  += w[valid_w]
        weighted_sq[valid_w]   += w[valid_w] * aod[valid_w]**2
        n_sensors[valid_w]     += 1

        # Track dominant sensor (highest weight)
        update = valid_w & (w > dominant_wt)
        dominant_wt[update]   = w[update]
        dominant_code[update] = code

    # Merged AOD (safe division: avoid 0/0 warning from np.where eager evaluation)
    has_any = weight_total > 0
    safe_w  = np.where(has_any, weight_total, 1.0)   # replace 0 → 1 to avoid divide
    aod_merged = np.where(has_any, weighted_sum / safe_w, np.nan).astype(np.float32)

    # Cross-sensor spread (weighted std dev)
    # Var = E[w·x²]/E[w] - (E[w·x]/E[w])²
    variance = np.where(
        has_any & (n_sensors > 1),
        weighted_sq / safe_w - (weighted_sum / safe_w) ** 2,
        np.nan,
    )
    variance   = np.clip(variance, 0, None)
    aod_std    = np.where(np.isfinite(variance), np.sqrt(variance), np.nan).astype(np.float32)

    # Confidence flag
    conf_flag = _assign_confidence(sensor_has_data)

    return {
        'aod_merged':       aod_merged,
        'aod_std':          aod_std,
        'n_sensors':        n_sensors.astype(np.int8),
        'dominant_sensor':  dominant_code,
        'confidence_flag':  conf_flag,
    }
