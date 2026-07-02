"""Triple-collocation σ² estimation (§7.4.2).

Stoffelen (1998) Eq. 5 — given three datasets X, Y, Z observing the same truth
with mutually uncorrelated errors:

    σ²_X = Var(X) − Cov(X, Y) · Cov(X, Z) / Cov(Y, Z)
    σ²_Y = Var(Y) − Cov(X, Y) · Cov(Y, Z) / Cov(X, Z)
    σ²_Z = Var(Z) − Cov(X, Z) · Cov(Y, Z) / Cov(X, Y)

McColl et al. (2014) extended TC (their Eqs. 7–10): additionally recovers
the unknown additive bias α'_i and the correlation-with-truth ρ_i,t per sensor
from the same covariance matrix.  We expose both as diagnostics — §8.1.3
consumes ρ_i,t as a Vietnam-central inter-sensor consistency proxy.

Triplet construction rules (§7.4.2 independence assumption):

  • Reject triplets where two members share the retrieval algorithm:
    VIIRS-SNPP + VIIRS-NOAA20 both use Deep Blue (Sayer et al. 2020).
  • Allow MERRA-2 + MAIAC (MERRA-2 assimilates MODIS Dark Target NNR, not MAIAC).
  • Allow Himawari L2 + L3 (JAXA L3 V3 merged_aot composites land/ocean
    retrievals, not L2 pixels).

The strict-vs-permissive sensitivity check (§7.4.2 'Sensitivity check'
paragraph) is materialised here via `INDEPENDENCE_STRICT` — toggling it
forces every pair sharing the underlying instrument to be rejected.
"""

from __future__ import annotations
from collections import defaultdict
from itertools import combinations
import math
from typing import Iterable

import numpy as np

# Sensor → retrieval-algorithm family.  Triplets are rejected when two members
# share the same algorithm.
_ALGO = {
    'himawari_l2':  'AHI_JAXA_L2',
    'himawari_l3':  'AHI_JAXA_L3',
    'modis_maiac':  'MODIS_MAIAC',
    'viirs_snpp':   'VIIRS_DeepBlue',
    'viirs_noaa20': 'VIIRS_DeepBlue',
    'merra2':       'MERRA2_GOCART',
}

# Sensor → underlying instrument (used by the strict variant only).
_INSTR = {
    'himawari_l2':  'AHI',
    'himawari_l3':  'AHI',
    'modis_maiac':  'MODIS',
    'viirs_snpp':   'VIIRS_SNPP',
    'viirs_noaa20': 'VIIRS_NOAA20',
    # MERRA-2 ingests MODIS DT NNR — strict variant treats it as MODIS-tied.
    'merra2':       'MODIS',
}


def is_independent_triplet(
    a: str, b: str, c: str, strict: bool = False,
) -> bool:
    """Return True if the (a, b, c) triplet satisfies the independence rule."""
    members = (a, b, c)
    algos = [_ALGO.get(m, m) for m in members]
    if len(set(algos)) < 3:
        return False
    if strict:
        instrs = [_INSTR.get(m, m) for m in members]
        if len(set(instrs)) < 3:
            return False
    return True


def enumerate_triplets(
    sensors: Iterable[str], strict: bool = False,
) -> list[tuple[str, str, str]]:
    """All independence-valid (unordered) triplets from `sensors`."""
    return [
        triple
        for triple in combinations(sensors, 3)
        if is_independent_triplet(*triple, strict=strict)
    ]


# ── Core TC math ────────────────────────────────────────────────────────────

def triple_collocation_variance(
    x: np.ndarray, y: np.ndarray, z: np.ndarray,
    ddof: int = 1,
) -> dict[str, float]:
    """Stoffelen 1998 + McColl 2014 estimator on one matched (x, y, z) triplet.

    Inputs are 1-D arrays of the same length, already restricted to rows where
    all three datasets have a valid observation.  Each row is treated as one
    independent realisation; no spatial / temporal weighting.

    Returns a dict with σ² per sensor, plus McColl 2014 ρ_i,t (correlation
    with truth, defined when σ²_i ≥ 0):

        {'sigma2_x', 'sigma2_y', 'sigma2_z',
         'rho_x',    'rho_y',    'rho_z',
         'n', 'cxy', 'cxz', 'cyz'}

    NaN is returned for σ² entries when any covariance in the denominator
    falls below `_COV_FLOOR` (degenerate triplet with one near-constant
    member — division would explode).  The caller decides whether to drop
    the triplet or floor σ² at the EE envelope.
    """
    _COV_FLOOR = 1e-10

    if x.size < 30:
        # Below this sample size, σ² estimates are dominated by sampling noise.
        # §7.4.2 floors at TC_MIN_TRIPLETS at the stratum level, but a single
        # triplet with N < 30 collocations is meaningless.
        return {'sigma2_x': np.nan, 'sigma2_y': np.nan, 'sigma2_z': np.nan,
                'rho_x': np.nan, 'rho_y': np.nan, 'rho_z': np.nan,
                'n': int(x.size), 'cxy': np.nan, 'cxz': np.nan, 'cyz': np.nan}

    xm = x.astype(np.float64) - x.mean()
    ym = y.astype(np.float64) - y.mean()
    zm = z.astype(np.float64) - z.mean()

    n_denom = float(x.size - ddof)
    vx = float(np.sum(xm * xm) / n_denom)
    vy = float(np.sum(ym * ym) / n_denom)
    vz = float(np.sum(zm * zm) / n_denom)
    cxy = float(np.sum(xm * ym) / n_denom)
    cxz = float(np.sum(xm * zm) / n_denom)
    cyz = float(np.sum(ym * zm) / n_denom)

    def _safe(num, denom):
        if abs(denom) < _COV_FLOOR:
            return np.nan
        return num / denom

    # Stoffelen 1998 Eq. 5
    s2x = vx - _safe(cxy * cxz, cyz)
    s2y = vy - _safe(cxy * cyz, cxz)
    s2z = vz - _safe(cxz * cyz, cxy)

    # McColl 2014 Eq. 9 — correlation-with-truth.  Sign matches the
    # sign of the off-diagonal product in the numerator.
    def _rho(v_self, cov_a, cov_b, cov_other):
        denom = v_self * cov_other
        if abs(denom) < _COV_FLOOR:
            return np.nan
        val = (cov_a * cov_b) / denom
        if val <= 0:
            return np.nan
        return float(np.sqrt(val))

    rho_x = _rho(vx, cxy, cxz, cyz)
    rho_y = _rho(vy, cxy, cyz, cxz)
    rho_z = _rho(vz, cxz, cyz, cxy)

    return {
        'sigma2_x': s2x, 'sigma2_y': s2y, 'sigma2_z': s2z,
        'rho_x':    rho_x, 'rho_y': rho_y, 'rho_z': rho_z,
        'n': int(x.size),
        'cxy': cxy, 'cxz': cxz, 'cyz': cyz,
    }


# ── Per-stratum aggregation ──────────────────────────────────────────────────
#
# TODO (5–10 lines):  Aggregate per-sensor σ² across all valid triplets
# containing that sensor, per (region, season) stratum.  Pick the right
# central-tendency estimator:
#
#   • §7.4.2 specifies the MEDIAN across triplets (more robust to a single
#     pathological triplet — e.g. when an independence violation slips
#     through and inflates σ² for one pair).
#   • You also need a minimum-triplet count: per §7.4.2 + config.TC_MIN_TRIPLETS,
#     a stratum with fewer than that many valid triplets should fall back to
#     the EE envelope as σ² rather than a noisy median over <50 samples.
#   • Negative σ² values from a single triplet should NOT enter the aggregate
#     (Stoffelen 1998 §4 — a negative σ² means the independence assumption
#     was violated; treat it as the triplet being unusable, not as a real
#     small variance).
#
# Fill in `aggregate_sigma2_per_sensor` below.  Inputs and outputs are spelt
# out; the implementation is yours.
#
# This is the only design decision in this file where the §7.4.2 prose leaves
# real choices: the "median" wording in §7.4.2 doesn't pin down exactly how
# the per-triplet σ² for sensor i gets aggregated when sensor i appears in
# (e.g.) 7 triplets in a stratum.  Median per stratum is what §7.4.2 says;
# whether you also drop the top/bottom outliers is judgment.

def aggregate_sigma2_per_sensor(
    triplet_results: list[dict],
    min_triplets: int,
    min_collocations: int,
) -> dict[str, dict]:
    """Aggregate per-sensor σ² across triplets within one (region, season) stratum.

    Parameters
    ----------
    triplet_results : list of dicts, each from one valid triplet.  Each dict
        has the shape::

            {
                'members': ('himawari_l2', 'modis_maiac', 'merra2'),
                'sigma2':  {member_name: float, ...},   # per-sensor σ² (Stoffelen)
                'rho':     {member_name: float, ...},   # per-sensor ρ_{i,t} (McColl Eq. 9)
                'n':       int,                          # collocations used
            }

        Members are the sensor keys used as triplet positions.

    min_triplets : minimum number of distinct triplet combinations a sensor must
        appear in (§7.4.2 + config.TC_MIN_TRIPLETS).
    min_collocations : minimum total per-cell-per-slot samples summed across
        those triplets (§7.4.2 + config.TC_MIN_COLLOCATIONS).
        Both gates must pass; otherwise return NaN σ² so the caller falls back
        to the EE-envelope floor.

    Returns
    -------
    dict keyed by sensor name with::

        {
            'sigma2':         float (NaN if below min_triplets or all NaN),
            'rho_t':          float (McColl ρ_{i,t} median across valid triplets;
                                     NaN if below min_triplets or all Eq.-9 signs failed),
            'n_triplets':     int,   # triplets contributing a finite σ²
            'n_rho_triplets': int,   # triplets contributing a finite ρ (may differ:
                                     # McColl's radicand can fail even when σ² is fine)
            'n_collocations': int (total across triplets used),
        }
    """
    # Accumulators for valid data per sensor
    sensor_sigma_arrays = defaultdict(list)
    sensor_rho_arrays   = defaultdict(list)
    sensor_collocation_counts = defaultdict(int)

    # 1. Pivot and Filter
    for triplet in triplet_results:
        collocations = triplet.get('n', 0)
        rho_map = triplet.get('rho', {})

        for sensor, sig2 in triplet.get('sigma2', {}).items():
            # Drop negatives (independence violation) and explicitly check for NaNs
            if sig2 is not None and not math.isnan(sig2) and sig2 >= 0:
                sensor_sigma_arrays[sensor].append(sig2)
                sensor_collocation_counts[sensor] += collocations
            # ρ_{i,t} is NaN-guarded upstream (McColl Eq. 9 negative radicand).
            # Aggregated independently: a triplet with usable σ² may have NaN ρ
            # if the sign check failed, and vice versa.
            rho = rho_map.get(sensor)
            if rho is not None and math.isfinite(rho):
                sensor_rho_arrays[sensor].append(float(rho))

    # 2. Aggregate
    aggregated_results = {}
    all_sensors = set(sensor_sigma_arrays) | set(sensor_rho_arrays)
    for sensor in all_sensors:
        sig2_list = sensor_sigma_arrays.get(sensor, [])
        rho_list  = sensor_rho_arrays.get(sensor, [])
        n_triplets = len(sig2_list)
        n_colloc   = sensor_collocation_counts[sensor]

        if n_triplets >= min_triplets and n_colloc >= min_collocations:
            final_sigma2 = float(np.median(sig2_list))
        else:
            final_sigma2 = float('nan')

        # ρ shares the min_triplets / min_collocations gate — a median over
        # <TC_MIN_TRIPLETS values is not the honest §7.4.2 estimate.
        if len(rho_list) >= min_triplets and n_colloc >= min_collocations:
            final_rho = float(np.median(rho_list))
        else:
            final_rho = float('nan')

        aggregated_results[sensor] = {
            'sigma2': final_sigma2,
            'rho_t': final_rho,
            'n_triplets': n_triplets,
            'n_rho_triplets': len(rho_list),
            'n_collocations': n_colloc,
        }

    return aggregated_results


# ── EE envelope floor (§7.4.2 'Floor against the Sayer/Levy EE envelope') ───

def ee_floor_sigma2(
    ee_offset: float, ee_slope: float, aod_ref: float,
) -> float:
    """σ² floor = (EE_OFFSET + EE_SLOPE · AOD_ref)² (Levy 2013 / Sayer 2020)."""
    ee = ee_offset + ee_slope * aod_ref
    return float(ee ** 2)


# ── Per-stratum collocation collection (Stage A2 + MERRA-2 → triplet samples) ──
#
# Lives here rather than bias_correction.py because the collocation is fully
# TC-specific: we need per-cell per-slot stacks of every sensor's *soft-
# calibrated* AOD plus MERRA-2 at the same cell, partitioned by stratum.

def collect_stratum_triplets(
    train_start, train_end,
    soft_cals: dict,
    strict: bool = False,
    sample_every_n_slots: int = 1,
) -> dict[tuple[str, str], dict[tuple[str, str, str], dict[str, np.ndarray]]]:
    """Walk Stage A2 slots; collect collocated samples per triplet per stratum.

    For each slot:
      1. Read every sensor's gridded NetCDF (via grid.read_gridded_slot).
      2. Apply soft calibration to the satellite grids (so triplets are computed
         on bias-removed data — §7.4.2 'After soft calibration has been applied').
      3. Read the MERRA-2 hourly slice resampled to the 0.05° grid.
      4. For every valid triplet (under the §7.4.2 independence rule):
         for every cell where all three members are valid, append each
         member's value to its slot in the triplet's per-stratum buffer.

    Returns nested dict ``{(region, season): {triplet_tuple: {member: arr}}}``,
    where ``member`` keys match the triplet tuple's positions.

    Strict mode (`strict=True`) additionally rejects pairs sharing the
    underlying instrument (MERRA-2 + MAIAC, Himawari L2 + L3 — §7.4.2
    'Sensitivity check').  The caller can run both variants and report the
    one with the lower headline σ² as documented in §7.4.2.
    """
    # Local imports so this module stays optional for callers who only want
    # the math (e.g. notebooks).
    from datetime import datetime, timedelta
    from config import (
        SLOT_MINUTES, DRY_MONTHS,
        NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
        LATS, REGIONS, SEASONS,
        TC_INPUT_SPACE,
    )
    from grid import read_gridded_slot, ALL_SENSORS
    from bias_correction import apply_soft_calibration_grid
    import merra2

    # config.TC_INPUT_SPACE: 'calibrated' applies α·sat+β before computing σ²;
    # 'raw' feeds uncalibrated AOD into TC (Gruber 2016 §2.1) and lets fusion
    # rescale σ²_raw → α²·σ²_raw in calibrated space.
    use_calibrated_inputs = (TC_INPUT_SPACE == 'calibrated')

    try:
        from tqdm import tqdm as _tqdm
    except ImportError:
        _tqdm = None

    # All sensors eligible as triplet members.  Include MERRA-2 as a synthetic
    # 'sensor' so its containing triplets are enumerated alongside satellites.
    triplet_members = ('himawari_l2', 'himawari_l3',
                       'modis_maiac', 'viirs_snpp', 'viirs_noaa20',
                       'merra2')
    valid_triplets = enumerate_triplets(triplet_members, strict=strict)

    # Per-region code for fast partitioning.
    lat_col = LATS[:, None]
    region_code = np.where(lat_col >= NORTH_CENTRAL_LAT, 2,
                           np.where(lat_col < CENTRAL_SOUTH_LAT, 0, 1)).astype(np.int8)
    region_name = {0: 'south', 1: 'central', 2: 'north'}

    # Pre-shape buffers (regions × seasons × triplets × members).
    buffers: dict = {}
    for region in REGIONS:
        for season in SEASONS:
            buffers[(region, season)] = {
                triple: {m: [] for m in triple}
                for triple in valid_triplets
            }

    # Enumerate slots.
    slots: list = []
    d = train_start
    while d <= train_end:
        base = datetime(d.year, d.month, d.day)
        for i in range(48):
            slots.append(base + timedelta(minutes=i * SLOT_MINUTES))
        d += timedelta(days=1)
    if sample_every_n_slots > 1:
        slots = slots[::sample_every_n_slots]

    iter_obj = (_tqdm(slots, desc='  collect triplets', unit='slot',
                      ncols=80, leave=False)
                if _tqdm is not None else slots)

    for slot_utc in iter_obj:
        g = read_gridded_slot(slot_utc, sensors=ALL_SENSORS)
        if g is None:
            continue
        season = 'dry' if slot_utc.month in DRY_MONTHS else 'wet'

        # Build per-member grids for this slot.  Sensors missing from `g` stay
        # None so per-triplet validity masks reject them.  In 'calibrated' mode
        # (default) we soft-calibrate each satellite before TC; in 'raw' mode
        # we pass the gridded AOD through unmodified so σ² stays in raw space.
        grids: dict = {}
        for sensor in ('himawari_l2', 'himawari_l3',
                       'modis_maiac', 'viirs_snpp', 'viirs_noaa20'):
            if sensor in g:
                sat = g[sensor]['aod_mean']
                if use_calibrated_inputs:
                    sat = apply_soft_calibration_grid(
                        sat, sensor, slot_utc.month, soft_cals
                    )
                grids[sensor] = sat
            else:
                grids[sensor] = None
        grids['merra2'] = merra2.get_slot_grid(slot_utc)

        for triple in valid_triplets:
            mem_grids = [grids.get(m) for m in triple]
            if any(arr is None for arr in mem_grids):
                continue
            joint = np.isfinite(mem_grids[0]) & np.isfinite(mem_grids[1]) & np.isfinite(mem_grids[2])
            if not np.any(joint):
                continue
            for code, region in region_name.items():
                mask = joint & (region_code == code)
                if not np.any(mask):
                    continue
                buf = buffers[(region, season)][triple]
                for m, arr in zip(triple, mem_grids):
                    buf[m].append(arr[mask].astype(np.float32))

    # Concatenate per triplet.
    out: dict = {}
    for stratum, triples in buffers.items():
        out_stratum: dict = {}
        for triple, member_arrs in triples.items():
            sizes = [len(member_arrs[m]) for m in triple]
            if min(sizes) == 0:
                continue
            out_stratum[triple] = {
                m: np.concatenate(member_arrs[m]) for m in triple
            }
        if out_stratum:
            out[stratum] = out_stratum
    return out
