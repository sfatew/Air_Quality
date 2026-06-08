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
    HIMAWARI_WET_WEIGHT_FACTOR,
    SENSOR_RMSE_FLOOR,
    CONFIDENCE_FLAG,
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
    NLAT, NLON, BIASC_DIR, DRY_MONTHS, WET_MONTHS,
)

# Integer codes for dominant_sensor output variable.
# a single 'himawari' contribution (L3-preferred, L2-fallback) replaces
# the separate L2/L3 sensors so ICW weights don't double-count Himawari.
SENSOR_CODES = {
    'himawari':    1,
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

    Always seeds from SENSOR_RMSE_PRIOR (2-tuple keys, covers all regions
    including central), then overlays post-correction 3-tuple entries from the
    JSON cache.  Without this merge, central-region cells would fall through
    every branch of the fuse() lookup and receive NaN RMSE → zero weight →
    no merged output for the entire central latitude band.
    """
    result = dict(SENSOR_RMSE_PRIOR)
    if use_post_correction and _RMSE_CACHE_FILE.exists():
        try:
            raw = json.loads(_RMSE_CACHE_FILE.read_text())
            result.update({tuple(k.split('|')): v for k, v in raw.items()})
        except Exception:
            pass
    return result


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
    hi_keys  = ('himawari',)

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

    season = 'dry' if month in DRY_MONTHS else 'wet'
    shape  = (NLAT, NLON)

    # Region codes: 0=south, 1=central, 2=north (avoids string arrays).
    region_code = np.where(lat_2d >= NORTH_CENTRAL_LAT, 2,
                           np.where(lat_2d < CENTRAL_SOUTH_LAT, 0, 1)).astype(np.int8)

    weighted_sum   = np.zeros(shape, dtype=np.float64)
    weight_total   = np.zeros(shape, dtype=np.float64)
    weighted_sq    = np.zeros(shape, dtype=np.float64)
    dominant_wt    = np.zeros(shape, dtype=np.float64)
    dominant_code  = np.zeros(shape, dtype=np.int8)
    n_sensors      = np.zeros(shape, dtype=np.int8)
    sensor_has_data: dict[str, np.ndarray] = {}

    _reg_codes = ((0, 'south'), (1, 'central'), (2, 'north'))

    for sensor, aod in sensor_grids.items():
        if aod is None:
            continue
        has = np.isfinite(aod) & (aod >= 0)
        sensor_has_data[sensor] = has
        if not np.any(has):
            continue

        # Per-cell RMSE: season-specific key first, fall back to region-only, then 'north'.
        rmse_arr = np.full(shape, np.nan, dtype=np.float64)
        for code, reg in _reg_codes:
            rmse_val = rmse_dict.get(
                (sensor, reg, season),
                rmse_dict.get((sensor, reg),
                              rmse_dict.get((sensor, 'north'), np.nan)),
            )
            if not np.isnan(rmse_val):
                rmse_val = max(float(rmse_val), SENSOR_RMSE_FLOOR)
            rmse_arr[region_code == code] = rmse_val

        # ICW weight = 1 / RMSE²; apply per-sensor down-weight factors
        w = np.where(np.isfinite(rmse_arr) & (rmse_arr > 0),
                     1.0 / rmse_arr ** 2, 0.0)
        if sensor == 'modis_maiac':
            w = np.where(region_code == 0, w * MODIS_SOUTH_WEIGHT_FACTOR, w)
        # Wet-season Himawari down-weight: suppress cloud-edge bias
        # without the v3.1 tight-QA filter that stripped 70% of monsoon
        # retrievals.
        if sensor == 'himawari' and month in WET_MONTHS:
            w = w * HIMAWARI_WET_WEIGHT_FACTOR

        valid_w = has & (w > 0)
        s_code  = SENSOR_CODES.get(sensor, 0)

        weighted_sum[valid_w] += w[valid_w] * aod[valid_w]
        weight_total[valid_w] += w[valid_w]
        weighted_sq[valid_w]  += w[valid_w] * aod[valid_w] ** 2
        n_sensors[valid_w]    += 1

        update = valid_w & (w > dominant_wt)
        dominant_wt[update]   = w[update]
        dominant_code[update] = s_code

    has_any = weight_total > 0
    safe_w  = np.where(has_any, weight_total, 1.0)
    aod_merged = np.where(has_any, weighted_sum / safe_w, np.nan).astype(np.float32)

    variance = np.where(
        has_any & (n_sensors > 1),
        weighted_sq / safe_w - (weighted_sum / safe_w) ** 2,
        np.nan,
    )
    variance = np.clip(variance, 0, None)
    aod_std  = np.where(np.isfinite(variance), np.sqrt(variance), np.nan).astype(np.float32)

    conf_flag = _assign_confidence(sensor_has_data)

    return {
        'aod_merged':      aod_merged,
        'aod_std':         aod_std,
        'n_sensors':       n_sensors,
        'dominant_sensor': dominant_code,
        'confidence_flag': conf_flag,
    }
