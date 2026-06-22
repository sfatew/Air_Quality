"""Step A5 — Triple-collocation-weighted fusion (§7.5, v3.4.0).

Ahn 2021 inverse-variance form, with the variance source moved from per-site
AERONET-RMSE to AERONET-independent σ²_TC per (sensor, region, season):

    w_i = 1 / σ²_TC,i(region, season, sensor)
    AOD_merged = Σ(w_i · AOD_i_corrected) / Σ(w_i)

σ²_TC is floored at the Sayer/Levy expected-error envelope
(EE = 0.05 + 0.15 · AOD_ref) so low-N strata cannot produce runaway weights
(§7.4.2 'Floor against the Sayer/Levy EE envelope').

Strata whose §7.4.1 soft calibration route to 'none' have their σ² multiplied
by NONE_PENALTY_FACTOR (default 2.0 → weight drops ~4×).  This is the only
sensor down-weight in the merge — no hand-set MODIS_SOUTH_WEIGHT_FACTOR or
HIMAWARI_WET_WEIGHT_FACTOR remains; every weight decision flows through one
table (§7.5 'No hand-set multipliers').

MERRA-2 is *not* in the production merge; its role ends at training time.
Slots with zero satellite sensors are left as gaps for Stage B (§7.6+).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import numpy as np

from config import (
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
    NLAT, NLON,
    REGIONS, SEASONS,
    DRY_MONTHS,
    SENSOR_CODES, CONFIDENCE_FLAG,
    SENSOR_RMSE_EE_OFFSET, SENSOR_RMSE_EE_SLOPE, SENSOR_RMSE_EE_REFAOD,
    SENSOR_EE_SLOPE,
    NONE_PENALTY_FACTOR,
    TC_VARIANCE_FILE, SOFT_CAL_FILE,
)


# Production-merge sensor keys.  Himawari L2/L3 are merged per-pixel into a
# single 'himawari' grid *before* fusion (run_stage_a.py), so fusion sees
# only one Himawari row.
_PRODUCTION_SENSORS = ('himawari', 'modis_maiac', 'viirs_snpp', 'viirs_noaa20')


# ── EE-envelope floor (Sayer/Levy 2013, 2020) ───────────────────────────────

def ee_floor_sigma2(sensor: str | None = None) -> float:
    """σ² fallback used when a stratum has no TC entry in the table.

    Per-sensor when `sensor` is given (uses SENSOR_EE_SLOPE); otherwise falls
    back to the flat SENSOR_RMSE_EE_SLOPE — used by status banners that aren't
    sensor-specific.  NOT used as a clamp on TC values that passed the gate.
    """
    slope = SENSOR_EE_SLOPE.get(sensor, SENSOR_RMSE_EE_SLOPE) if sensor else SENSOR_RMSE_EE_SLOPE
    ee = SENSOR_RMSE_EE_OFFSET + slope * SENSOR_RMSE_EE_REFAOD
    return float(ee * ee)


# ── σ²_TC table loading ─────────────────────────────────────────────────────

def load_tc_variance(path: Path = TC_VARIANCE_FILE) -> dict:
    """Load `tc_error_variance.json` into a (sensor, region, season) lookup.

    Expected schema produced by run_collocate.py tc_variance::

        {
          "himawari": {
              "north|dry":   {"sigma2": 0.21, "n_triplets": 87, "n_collocations": 412345},
              ...
          },
          "modis_maiac": { ... },
          ...
        }

    Returns ``{(sensor, region, season): sigma2_float}`` for downstream lookup.
    Missing file → empty dict; the caller falls back to the EE floor per cell.
    """
    if not path.exists():
        return {}
    raw = json.loads(path.read_text())
    out: dict[tuple[str, str, str], float] = {}
    for sensor, sensor_entries in raw.items():
        for stratum_key, fit_dict in sensor_entries.items():
            region, season = stratum_key.split('|')
            sig2 = fit_dict.get('sigma2')
            if sig2 is None or not np.isfinite(float(sig2)):
                continue
            out[(sensor, region, season)] = float(sig2)
    return out


def load_routes(path: Path = SOFT_CAL_FILE) -> dict[tuple[str, str, str], str]:
    """Load per-stratum soft-calibration routes ('apply'|'none') from soft_calibration.json.

    Needed so fusion can apply the NONE_PENALTY_FACTOR to 'none' strata at
    weight-construction time.  Returns ``{}`` if the JSON is missing.
    """
    if not path.exists():
        return {}
    raw = json.loads(path.read_text())
    out: dict[tuple[str, str, str], str] = {}
    for sensor, sensor_entries in raw.items():
        # Sensor-key folding: per-level Himawari L2/L3 soft cals are persisted
        # under their own keys but the production-merge 'himawari' channel
        # needs a single route lookup.  We propagate 'none' iff BOTH levels
        # are routed to 'none' (i.e. the merged Himawari pixel is genuinely
        # uncorrected; otherwise the merged grid carries at least one level
        # with an applied correction).
        for stratum_key, fit_dict in sensor_entries.items():
            region, season = stratum_key.split('|')
            out[(sensor, region, season)] = fit_dict.get('route', 'apply')

    # Derive merged-Himawari route per (region, season).
    for region in REGIONS:
        for season in SEASONS:
            r2 = out.get(('himawari_l2', region, season))
            r3 = out.get(('himawari_l3', region, season))
            if r2 is None and r3 is None:
                continue
            if (r2 == 'apply') or (r3 == 'apply'):
                out[('himawari', region, season)] = 'apply'
            else:
                out[('himawari', region, season)] = 'none'
    return out


# ── Per-cell σ² lookup ──────────────────────────────────────────────────────

def _sigma2_grid(
    sensor: str, season: str,
    region_code: np.ndarray,
    sigma2_table: dict,
    routes:       dict,
    floor: float,
) -> np.ndarray:
    """Per-cell σ² (NLAT, NLON) for one sensor at one slot.

    Region-by-region lookup of σ²_TC; missing entry → per-sensor EE floor.
    TC values that passed the gate are NOT re-clamped here — the gate is the
    trust check.  Strata routed to 'none' have σ² multiplied by
    NONE_PENALTY_FACTOR (weight ↓ by ~factor²).
    """
    shape = region_code.shape
    out = np.full(shape, floor, dtype=np.float64)
    for code, region in ((0, 'south'), (1, 'central'), (2, 'north')):
        sig2 = sigma2_table.get((sensor, region, season))
        if sig2 is None or not np.isfinite(sig2):
            continue
        sig2 = float(sig2)
        if routes.get((sensor, region, season), 'apply') == 'none':
            sig2 *= NONE_PENALTY_FACTOR
        out[region_code == code] = sig2
    return out


def _assign_confidence(
    sensor_has_data: dict[str, np.ndarray],
) -> np.ndarray:
    """Per-cell confidence flag (0–4) using the §7.5 / config.CONFIDENCE_FLAG rules."""
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

    flag[has_hi & ~has_leo]                  = CONFIDENCE_FLAG['himawari_only']
    flag[~has_hi & has_leo]                  = CONFIDENCE_FLAG['leo_only']
    flag[has_hi & has_leo & (leo_count < 2)] = CONFIDENCE_FLAG['himawari_plus_leo']
    flag[has_hi & has_leo & (leo_count >= 2)] = CONFIDENCE_FLAG['multi_leo_himawari']
    return flag


# ── Main fusion entry point ─────────────────────────────────────────────────

def fuse(
    sensor_grids: dict[str, np.ndarray],
    month: int,
    lat_2d: np.ndarray,
    sigma2_table: Optional[dict] = None,
    routes:       Optional[dict] = None,
) -> dict[str, np.ndarray]:
    """TC-weighted fusion of bias-corrected sensor grids → one merged AOD.

    Parameters
    ----------
    sensor_grids : dict mapping sensor_key → (NLAT, NLON) bias-corrected
                   float32 AOD array (NaN = no data for this sensor/cell).
                   Keys must be in _PRODUCTION_SENSORS.
    month        : calendar month (1–12) used to pick the season stratum.
    lat_2d       : (NLAT, NLON) latitude array (cell centres).
    sigma2_table : (sensor, region, season) → σ²_TC; if None, loads from disk.
    routes       : (sensor, region, season) → 'apply'|'none'; if None, loads from disk.

    Returns a dict with (NLAT, NLON) arrays:
        aod_merged      float32
        aod_std         float32   (cross-sensor weighted std)
        weight_sum      float32   (Σ w_i — provenance for Stage B daily aggregation)
        n_sensors       int8
        dominant_sensor int8      (SENSOR_CODES values)
        confidence_flag int8
    """
    if sigma2_table is None:
        sigma2_table = load_tc_variance()
    if routes is None:
        routes = load_routes()

    season = 'dry' if month in DRY_MONTHS else 'wet'
    shape  = (NLAT, NLON)

    region_code = np.where(
        lat_2d >= NORTH_CENTRAL_LAT, 2,
        np.where(lat_2d < CENTRAL_SOUTH_LAT, 0, 1),
    ).astype(np.int8)

    weighted_sum  = np.zeros(shape, dtype=np.float64)
    weight_total  = np.zeros(shape, dtype=np.float64)
    weighted_sq   = np.zeros(shape, dtype=np.float64)
    dominant_wt   = np.zeros(shape, dtype=np.float64)
    dominant_code = np.zeros(shape, dtype=np.int8)
    n_sensors     = np.zeros(shape, dtype=np.int8)
    sensor_has_data: dict[str, np.ndarray] = {}

    for sensor, aod in sensor_grids.items():
        if aod is None or sensor not in _PRODUCTION_SENSORS:
            continue
        has = np.isfinite(aod) & (aod >= 0)
        sensor_has_data[sensor] = has
        if not np.any(has):
            continue

        sigma2 = _sigma2_grid(sensor, season, region_code,
                              sigma2_table, routes,
                              ee_floor_sigma2(sensor))
        w = np.where((sigma2 > 0), 1.0 / sigma2, 0.0)

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

    weight_sum_out = np.where(has_any, weight_total, np.nan).astype(np.float32)
    conf_flag = _assign_confidence(sensor_has_data)

    return {
        'aod_merged':      aod_merged,
        'aod_std':         aod_std,
        'weight_sum':      weight_sum_out,
        'n_sensors':       n_sensors,
        'dominant_sensor': dominant_code,
        'confidence_flag': conf_flag,
    }


# ── Persistence helper used by run_collocate.py tc_variance ─────────────────

def save_tc_variance(
    table: dict[str, dict[str, dict]],
    path: Path = TC_VARIANCE_FILE,
) -> None:
    """Write the per-sensor / per-stratum σ²_TC dict to JSON.

    `table` schema mirrors what load_tc_variance reads back::

        { sensor: { 'region|season': {'sigma2': ..., 'n_triplets': ...,
                                      'n_collocations': ...},
                    ... },
          ... }
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(table, indent=2, sort_keys=True))
