"""Step A4 — MERRA-2-anchored soft calibration with AERONET fallback (§7.4.1).

For each (sensor, region, season) stratum, fit a linear transfer function

    MERRA-2 = α · sat + β     ⇒     sat_corrected = α · sat + β

over the training window (Sep 2022 – Dec 2024), with MERRA-2 hourly AOD
bilinearly resampled from its native 0.5° × 0.625° grid onto the 0.05°
production grid and nearest-hour matched to the 30-min slot centre.  The
linear form (rather than a full PCHIP CDF) follows Ding et al. (2025) and
avoids over-flattening the satellite tails against MERRA-2's smoother field.

CV-time guard rail (MERRA-2 anchor)
-----------------------------------
5-fold cross-validated α and β are computed alongside the in-sample fit.
A stratum routes to 'none' from the MERRA-2 anchor when:

    * N < SOFT_CAL_MIN_PAIRS, or
    * CV α ∉ [SOFT_CAL_ALPHA_MIN, SOFT_CAL_ALPHA_MAX], or
    * |CV β| > SOFT_CAL_BETA_ABSMAX, or
    * §8.1.2 gate A — rmse_after_cv > rmse_before − GATE_A_RMSE_DROP_MIN.

Gate B (held-out RMSE inflation) is evaluated separately in
validate_bias_correction.ipynb and never feeds back into the JSON.

AERONET-anchored fallback (§7.4.1.1)
------------------------------------
When the MERRA-2 anchor routes a stratum to 'none' (and the stratum's region
has an AERONET station — i.e. north or south, never central), a one-shot
fallback fit is attempted on every AERONET pair in [TRAIN_START, TRAIN_END]
for that (region, season), gated on k-fold CV that mirrors the MERRA-2
anchor's CV gate.  The §8 held-out window (Jan 2025 – Apr 2026) is never
touched.

The fallback uses *widened* α/β bounds (AERONET_FALLBACK_ALPHA_MIN/MAX,
AERONET_FALLBACK_BETA_ABSMAX — currently α∈[0.3, 3.0], |β|≤0.4) to admit the
strong scale biases AERONET reveals in wet-season monsoon strata.  These are
coupled with a *stricter* RMSE-drop requirement (AERONET_FALLBACK_RMSE_DROP_MIN
= 0.05 vs 0.02 on the MERRA-2 anchor): gain a larger correction window in
exchange for proving more improvement on CV.

Gate (on k-fold CV mean):

    α_aer_cv ∈ [AERONET_FALLBACK_ALPHA_MIN, AERONET_FALLBACK_ALPHA_MAX]
    |β_aer_cv| ≤ AERONET_FALLBACK_BETA_ABSMAX
    rmse_aer_after_cv ≤ rmse_aer_before − AERONET_FALLBACK_RMSE_DROP_MIN

When the fallback passes, the persisted SoftCal swaps `alpha`/`beta` to the
AERONET-anchored in-sample fit and records `anchor='aeronet'`; route flips
to 'apply'.  When it fails, the stratum stays 'none' and `anchor` records
which side failed last ('merra2' if AERONET was never tried, 'aeronet' if
it was).

Persistence
-----------
All trained strata for a sensor are saved as one JSON file at
SOFT_CAL_FILE (= BIASC_DIR / 'soft_calibration.json'), keyed by sensor.
Schema is small and human-inspectable:

    {
      "himawari_l2": {
        "north|dry":    {"alpha": 0.92, "beta": 0.03, "alpha_cv": 0.91, ...,
                          "n_pairs": 14323, "route": "apply"},
        "central|wet":  {... "route": "none", ...},
        ...
      },
      "modis_maiac": { ... },
      ...
    }

run_stage_a.py loads the file once at startup and applies the per-sensor
per-stratum (α, β) to that sensor's gridded AOD in O(NLAT · NLON) per slot.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional, Iterable

import numpy as np
import pandas as pd

from config import (
    DRY_MONTHS, WET_MONTHS,
    REGIONS, SEASONS,
    AERONET_SITES,
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
    NLAT, NLON, LATS, LONS,
    SLOT_MINUTES,
    SOFT_CAL_FILE,
    SOFT_CAL_ALPHA_MIN, SOFT_CAL_ALPHA_MAX,
    SOFT_CAL_BETA_ABSMAX,
    SOFT_CAL_CV_FOLDS,
    SOFT_CAL_MIN_PAIRS,
    SOFT_CAL_OUTPUT_MIN, SOFT_CAL_OUTPUT_MAX,
    SOFTCAL_BLEND_HALF_WIDTH,
    GATE_A_RMSE_DROP_MIN,
    TRAIN_START, TRAIN_END,
    AERONET_FALLBACK_MIN_PAIRS,
    AERONET_FALLBACK_ALPHA_MIN, AERONET_FALLBACK_ALPHA_MAX,
    AERONET_FALLBACK_BETA_ABSMAX,
    AERONET_FALLBACK_RMSE_DROP_MIN,
    COLLOCATE_DIR,
)
from grid import read_gridded_slot, ALL_SENSORS as _ALL_GRID_SENSORS
import merra2

try:
    from tqdm import tqdm as _tqdm
    _HAS_TQDM = True
except ImportError:
    _tqdm = None
    _HAS_TQDM = False


_SENSORS_TO_CALIBRATE: tuple[str, ...] = (
    'himawari_l2', 'himawari_l3',
    'modis_maiac', 'viirs_snpp', 'viirs_noaa20',
)


# ── Stratum identification ──────────────────────────────────────────────────

def _season_of(month: int) -> str:
    return 'dry' if month in DRY_MONTHS else 'wet'


def _region_codes_2d() -> np.ndarray:
    """(NLAT, NLON) int8 region codes: 0=south, 1=central, 2=north."""
    lat_col = LATS[:, None]
    return np.where(lat_col >= NORTH_CENTRAL_LAT, 2,
                    np.where(lat_col < CENTRAL_SOUTH_LAT, 0, 1)).astype(np.int8)


_REGION_CODE_NAME = {0: 'south', 1: 'central', 2: 'north'}


# ── Dataclass for one stratum's fit ────────────────────────────────────────

@dataclass
class SoftCal:
    sensor:   str
    region:   str
    season:   str
    # Winning (production) fit — α, β that apply_soft_calibration_grid uses.
    alpha:    float
    beta:     float
    # MERRA-2 anchor diagnostics (the primary attempt; always populated).
    alpha_cv: float
    beta_cv:  float
    n_pairs:  int           # MERRA-2 pairs
    rmse_before: float      # MERRA-2 − sat (training)
    rmse_after:  float      # MERRA-2 − (α · sat + β) (training)
    rmse_after_cv: float    # 5-fold CV; what fusion downstream uses for diagnostics
    route:    str           # 'apply' | 'none'
    anchor:   str = 'merra2'  # 'merra2' | 'aeronet' | 'none'
    # AERONET fallback diagnostics — NaN/0 when fallback never ran (central
    # regions or MERRA-2 anchor already passed).  Schema mirrors the MERRA-2
    # anchor's `(alpha, beta, alpha_cv, beta_cv, rmse_before, rmse_after,
    # rmse_after_cv)` quartet so the two anchors can be compared field-by-field.
    n_aeronet:           int   = 0
    alpha_aer:           float = float('nan')   # in-sample fit on all pairs
    beta_aer:            float = float('nan')
    alpha_aer_cv:        float = float('nan')   # mean across k folds (the gate)
    beta_aer_cv:         float = float('nan')
    rmse_aer_before:     float = float('nan')   # uncorrected, all pairs
    rmse_aer_after:      float = float('nan')   # in-sample fit, all pairs
    rmse_aer_after_cv:   float = float('nan')   # mean fold-out RMSE (the gate)

    def to_dict(self) -> dict:
        return asdict(self)


# ── Training-pair collection from gridded slots ──────────────────────────────

def _enumerate_train_slots(train_start: date, train_end: date) -> list[datetime]:
    """All 30-min UTC slot centres in [train_start, train_end] inclusive."""
    out: list[datetime] = []
    d = train_start
    end = train_end
    while d <= end:
        base = datetime(d.year, d.month, d.day)
        for i in range(48):
            out.append(base + timedelta(minutes=i * SLOT_MINUTES))
        d += timedelta(days=1)
    return out


def collect_sat_merra2_pairs(
    sensor: str,
    train_start: date = TRAIN_START,
    train_end:   date = TRAIN_END,
    sample_every_n_slots: int = 1,
) -> dict[tuple[str, str], dict[str, np.ndarray]]:
    """Walk Stage A2 gridded slots; collect (sat, merra2) per (region, season).

    Each slot:
      1. Reads the Stage A2 gridded NetCDF for `sensor`; skip if absent.
      2. Reads the MERRA-2 hourly slice resampled to the 0.05° grid for the
         slot's nearest hour; skip if absent.
      3. For every cell with both values valid, appends (sat, merra2) to the
         stratum identified by (region, season).

    Returns a dict keyed by (region, season) with arrays {'sat': ..., 'merra2': ...}.
    Strata that the satellite never observed in this window are omitted.

    `sample_every_n_slots` lets a caller subsample for a faster training pass
    (the full 48 × ~850-day grid is ~40 000 slots; every cell is independent
    so a stride of 2–4 is harmless and saves I/O).
    """
    rc2d = _region_codes_2d()
    slots = _enumerate_train_slots(train_start, train_end)
    if sample_every_n_slots > 1:
        slots = slots[::sample_every_n_slots]

    pairs: dict[tuple[str, str], dict[str, list]] = {}
    iter_obj = _tqdm(slots, desc=f'  collect {sensor}', unit='slot',
                     ncols=80, leave=False) if _HAS_TQDM else slots

    for slot_utc in iter_obj:
        g = read_gridded_slot(slot_utc, sensors=(sensor,))
        if g is None or sensor not in g:
            continue
        sat = g[sensor]['aod_mean']
        m = merra2.get_slot_grid(slot_utc)
        if m is None:
            continue

        valid = np.isfinite(sat) & np.isfinite(m)
        if not np.any(valid):
            continue

        season = _season_of(slot_utc.month)
        for code, region in _REGION_CODE_NAME.items():
            mask = valid & (rc2d == code)
            if not np.any(mask):
                continue
            key = (region, season)
            d = pairs.setdefault(key, {'sat': [], 'merra2': []})
            d['sat'].append(sat[mask].astype(np.float32))
            d['merra2'].append(m[mask].astype(np.float32))

    # Concatenate per stratum.
    result: dict[tuple[str, str], dict[str, np.ndarray]] = {}
    for key, d in pairs.items():
        result[key] = {
            'sat':    np.concatenate(d['sat'])    if d['sat']    else np.empty(0, np.float32),
            'merra2': np.concatenate(d['merra2']) if d['merra2'] else np.empty(0, np.float32),
        }
    return result


# ── AERONET pair collection (fallback only; §7.4.1.1) ───────────────────────

def _aeronet_region_for_sensor() -> set[str]:
    """Regions that have at least one AERONET station configured in this build.

    Central currently has none → it can never receive the AERONET fallback.
    """
    return {meta['region'] for meta in AERONET_SITES.values()}


def collect_aeronet_pairs(
    sensor: str,
    train_start: date = TRAIN_START,
    train_end:   date = TRAIN_END,
) -> dict[tuple[str, str], dict[str, np.ndarray]]:
    """Read sat↔AERONET collocations for `sensor`; bucket by (region, season).

    Reads every `COLLOCATE_DIR/{sensor}_{site}.csv` produced by `match_site`,
    filters to [train_start, train_end], and returns all pairs per stratum::

        { (region, season): {'sat': ..., 'aer': ...}, ... }

    The train/val split is no longer here — `_try_aeronet_fallback` does its
    own k-fold CV.  Strata with no rows in the window are omitted.  Returns
    {} if no collocate CSVs exist for the sensor (e.g. AERONET match step
    never ran).
    """
    lo = pd.Timestamp(train_start)
    hi = pd.Timestamp(train_end)

    frames: list[pd.DataFrame] = []
    for site in AERONET_SITES:
        path = Path(COLLOCATE_DIR) / f'{sensor}_{site}.csv'
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        return {}

    df = pd.concat(frames, ignore_index=True)
    df['date'] = pd.to_datetime(df['date'])
    df = df.dropna(subset=['satellite_aod', 'aeronet_aod'])
    df = df[(df['date'] >= lo) & (df['date'] <= hi)]

    out: dict[tuple[str, str], dict[str, np.ndarray]] = {}
    for (region, season), grp in df.groupby(['region', 'season']):
        sat = grp['satellite_aod'].astype(np.float32).to_numpy()
        aer = grp['aeronet_aod'].astype(np.float32).to_numpy()
        if sat.size == 0:
            continue
        out[(str(region), str(season))] = {'sat': sat, 'aer': aer}
    return out


# ── Fitting ──────────────────────────────────────────────────────────────────

def _linear_fit(sat: np.ndarray, ref: np.ndarray) -> tuple[float, float]:
    """OLS fit ref ≈ α · sat + β.  Returns (α, β); NaN, NaN if degenerate."""
    if sat.size < 2:
        return float('nan'), float('nan')
    x = sat.astype(np.float64)
    y = ref.astype(np.float64)
    sx, sy = x.mean(), y.mean()
    sxx = float(np.sum((x - sx) * (x - sx)))
    if sxx < 1e-12:
        return float('nan'), float('nan')
    sxy = float(np.sum((x - sx) * (y - sy)))
    alpha = sxy / sxx
    beta = sy - alpha * sx
    return alpha, beta


def _rmse(pred: np.ndarray, ref: np.ndarray) -> float:
    if pred.size == 0:
        return float('nan')
    diff = pred.astype(np.float64) - ref.astype(np.float64)
    return float(np.sqrt(np.mean(diff * diff)))


def _kfold_cv(
    sat: np.ndarray, ref: np.ndarray, n_folds: int,
) -> tuple[float, float, float]:
    """K-fold CV.  Returns (mean α_CV, mean β_CV, mean RMSE_after_CV)."""
    n = sat.size
    if n < n_folds * 2:
        return float('nan'), float('nan'), float('nan')

    rng = np.random.default_rng(42)
    idx = rng.permutation(n)
    folds = np.array_split(idx, n_folds)

    alphas, betas, rmses = [], [], []
    for f in range(n_folds):
        val_idx = folds[f]
        tr_idx  = np.concatenate([folds[g] for g in range(n_folds) if g != f])
        a, b = _linear_fit(sat[tr_idx], ref[tr_idx])
        if not (np.isfinite(a) and np.isfinite(b)):
            continue
        pred = a * sat[val_idx] + b
        alphas.append(a)
        betas.append(b)
        rmses.append(_rmse(pred, ref[val_idx]))

    if not alphas:
        return float('nan'), float('nan'), float('nan')
    return (float(np.mean(alphas)),
            float(np.mean(betas)),
            float(np.mean(rmses)))


def _classify_route(
    alpha_cv:      float,
    beta_cv:       float,
    n_pairs:       int,
    rmse_before:   float,
    rmse_after_cv: float,
) -> str:
    """Decide 'apply' vs 'none' for one MERRA-2-anchored stratum.

    Two gates in sequence:
      1. §7.4.1 guard rail — N, α_cv, β_cv must be in range.
      2. §8.1.2 gate A     — CV RMSE drop must clear GATE_A_RMSE_DROP_MIN.

    NaN rmse_after_cv (e.g. _kfold_cv bailed) is treated as a gate-A failure,
    matching the conservative reading of §7.4.1 step 3.
    """
    if n_pairs < SOFT_CAL_MIN_PAIRS:
        return 'none'
    if not (np.isfinite(alpha_cv) and np.isfinite(beta_cv)):
        return 'none'
    if alpha_cv < SOFT_CAL_ALPHA_MIN or alpha_cv > SOFT_CAL_ALPHA_MAX:
        return 'none'
    if abs(beta_cv) > SOFT_CAL_BETA_ABSMAX:
        return 'none'
    if not (np.isfinite(rmse_before) and np.isfinite(rmse_after_cv)):
        return 'none'
    if rmse_after_cv > rmse_before - GATE_A_RMSE_DROP_MIN:
        return 'none'
    return 'apply'


def _try_aeronet_fallback(
    sat: np.ndarray,
    aer: np.ndarray,
    n_folds: int = SOFT_CAL_CV_FOLDS,
) -> dict:
    """Fit `AERONET = α · sat + β` on all pairs; gate on k-fold CV mean drop.

    Returns a dict with the in-sample α/β (what apply-time uses if the gate
    passes), the CV-mean α/β (what the gate evaluates), uncorrected and
    corrected RMSEs (in-sample and CV-mean), the pair count, and a `passed`
    flag.  Widened α/β bounds and stricter RMSE-drop gate vs the MERRA-2
    anchor — see config for the why.
    """
    result = {
        'n':                int(sat.size),
        'alpha':            float('nan'),
        'beta':             float('nan'),
        'alpha_cv':         float('nan'),
        'beta_cv':          float('nan'),
        'rmse_before':      float('nan'),
        'rmse_after':       float('nan'),
        'rmse_after_cv':    float('nan'),
        'passed':           False,
    }

    if sat.size < AERONET_FALLBACK_MIN_PAIRS:
        return result

    # In-sample fit — α/β that production will apply if the CV gate passes.
    alpha, beta = _linear_fit(sat, aer)
    if not (np.isfinite(alpha) and np.isfinite(beta)):
        return result
    pred_in = alpha * sat + beta
    result.update(
        alpha=float(alpha), beta=float(beta),
        rmse_before=_rmse(sat, aer),
        rmse_after =_rmse(pred_in, aer),
    )

    # K-fold CV — α/β and out-of-fold RMSE used for the gate.
    alpha_cv, beta_cv, rmse_after_cv = _kfold_cv(sat, aer, n_folds)
    result.update(
        alpha_cv=alpha_cv, beta_cv=beta_cv,
        rmse_after_cv=rmse_after_cv,
    )

    if not (np.isfinite(alpha_cv) and np.isfinite(beta_cv)):
        return result
    # Widened α/β bounds (AERONET-specific).
    if alpha_cv < AERONET_FALLBACK_ALPHA_MIN or alpha_cv > AERONET_FALLBACK_ALPHA_MAX:
        return result
    if abs(beta_cv) > AERONET_FALLBACK_BETA_ABSMAX:
        return result

    rb = result['rmse_before']
    ra = result['rmse_after_cv']
    if not (np.isfinite(rb) and np.isfinite(ra)):
        return result
    # Stricter drop than MERRA-2 anchor — couples to widened bounds.
    if ra > rb - AERONET_FALLBACK_RMSE_DROP_MIN:
        return result

    result['passed'] = True
    return result


def fit_stratum(
    sensor: str, region: str, season: str,
    sat: np.ndarray, merra2_aod: np.ndarray,
    aeronet_pairs: Optional[dict[str, np.ndarray]] = None,
    n_folds: int = SOFT_CAL_CV_FOLDS,
) -> SoftCal:
    """Fit one (sensor, region, season) stratum (MERRA-2 + AERONET fallback).

    `aeronet_pairs`, when supplied, is the dict returned by
    `collect_aeronet_pairs()` for this (region, season); the fallback fits
    `AERONET = α · sat + β` on all pairs in [TRAIN_START, TRAIN_END] and
    gates on k-fold CV mean RMSE drop.  If the MERRA-2 anchor passes, the
    fallback is skipped (and its diagnostic fields stay at their defaults).
    """
    n = sat.size
    rmse_before = _rmse(sat, merra2_aod)

    if n < SOFT_CAL_MIN_PAIRS:
        fit = SoftCal(
            sensor=sensor, region=region, season=season,
            alpha=float('nan'), beta=float('nan'),
            alpha_cv=float('nan'), beta_cv=float('nan'),
            n_pairs=int(n),
            rmse_before=rmse_before, rmse_after=float('nan'),
            rmse_after_cv=float('nan'),
            route='none', anchor='none',
        )
    else:
        alpha, beta = _linear_fit(sat, merra2_aod)
        pred_full = alpha * sat + beta
        rmse_after = _rmse(pred_full, merra2_aod)

        alpha_cv, beta_cv, rmse_cv = _kfold_cv(sat, merra2_aod, n_folds)
        route = _classify_route(alpha_cv, beta_cv, int(n), rmse_before, rmse_cv)

        fit = SoftCal(
            sensor=sensor, region=region, season=season,
            alpha=float(alpha), beta=float(beta),
            alpha_cv=float(alpha_cv), beta_cv=float(beta_cv),
            n_pairs=int(n),
            rmse_before=rmse_before, rmse_after=rmse_after,
            rmse_after_cv=rmse_cv,
            route=route,
            anchor=('merra2' if route == 'apply' else 'none'),
        )

    if fit.route == 'apply':
        return fit
    if aeronet_pairs is None:
        return fit

    aer = _try_aeronet_fallback(
        aeronet_pairs.get('sat', np.empty(0, np.float32)),
        aeronet_pairs.get('aer', np.empty(0, np.float32)),
    )
    fit.n_aeronet         = aer['n']
    fit.alpha_aer         = aer['alpha']
    fit.beta_aer          = aer['beta']
    fit.alpha_aer_cv      = aer['alpha_cv']
    fit.beta_aer_cv       = aer['beta_cv']
    fit.rmse_aer_before   = aer['rmse_before']
    fit.rmse_aer_after    = aer['rmse_after']
    fit.rmse_aer_after_cv = aer['rmse_after_cv']

    if aer['passed']:
        # Swap winning fit to the AERONET anchor; route flips to 'apply'.
        fit.alpha  = aer['alpha']
        fit.beta   = aer['beta']
        fit.route  = 'apply'
        fit.anchor = 'aeronet'
    # else: route stays 'none', anchor stays 'none'.  The diagnostic fields
    # above let you tell 'AERONET never tried' (n_aeronet=0) from
    # 'AERONET tried and failed' (n_aeronet > 0).

    return fit


# ── Driver: train all sensors / all strata ──────────────────────────────────

def train_all_sensors(
    sensors: Iterable[str] = _SENSORS_TO_CALIBRATE,
    train_start: date = TRAIN_START,
    train_end:   date = TRAIN_END,
    sample_every_n_slots: int = 1,
    output_file: Path = SOFT_CAL_FILE,
) -> dict[str, dict[str, SoftCal]]:
    """Collect pairs + fit + persist for every (sensor, region, season).

    Returns a nested dict::

        { sensor: { 'region|season': SoftCal, ... }, ... }

    The same structure is JSON-serialised to `output_file`.  Existing entries
    in the file are overwritten only for the sensors we actually trained on
    this run, so a partial re-train doesn't wipe other sensors' fits.
    """
    out: dict[str, dict[str, SoftCal]] = {}

    for sensor in sensors:
        print(f'\n[soft_cal] {sensor}: collecting (sat, MERRA-2) pairs '
              f'{train_start} → {train_end}'
              + (f' (stride {sample_every_n_slots})' if sample_every_n_slots > 1 else ''))
        pairs = collect_sat_merra2_pairs(
            sensor=sensor,
            train_start=train_start, train_end=train_end,
            sample_every_n_slots=sample_every_n_slots,
        )
        if not pairs:
            print(f'  [soft_cal] {sensor}: no (sat, MERRA-2) pairs collected '
                  '— is run_collocate.py grid up to date?')
            continue

        # AERONET fallback pairs are collected once per sensor.  Missing CSVs
        # (e.g. before `run_collocate.py match` has run) → empty dict → no
        # fallback attempted; that's fine, MERRA-2 anchor decisions stand.
        aer_pairs = collect_aeronet_pairs(sensor)
        if aer_pairs:
            print(f'  [soft_cal] {sensor}: AERONET fallback pairs available '
                  f'for {len(aer_pairs)} (region, season) stratum/strata')

        sensor_out: dict[str, SoftCal] = {}
        for region in REGIONS:
            for season in SEASONS:
                key = (region, season)
                d = pairs.get(key)
                if d is None or d['sat'].size == 0:
                    continue
                fit = fit_stratum(
                    sensor, region, season, d['sat'], d['merra2'],
                    aeronet_pairs=aer_pairs.get(key),
                )
                sensor_out[f'{region}|{season}'] = fit
                tag = (f'[{fit.route}/{fit.anchor}]' if fit.anchor != 'merra2'
                       else f'[{fit.route}]')
                print(f'  {sensor:>14}  {region:>7} {season:>3}  '
                      f'N={fit.n_pairs:>7d}  '
                      f'α={fit.alpha:6.3f} β={fit.beta:+6.3f}  '
                      f'α_CV={fit.alpha_cv:6.3f} β_CV={fit.beta_cv:+6.3f}  '
                      f'rmse {fit.rmse_before:.3f} → {fit.rmse_after_cv:.3f}  '
                      f'{tag}')
        out[sensor] = sensor_out

    _persist_soft_calibrations(out, output_file)
    return out


def _persist_soft_calibrations(
    fits: dict[str, dict[str, SoftCal]],
    output_file: Path,
) -> None:
    """Write or overlay the per-sensor SoftCal entries to JSON."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    existing: dict = {}
    if output_file.exists():
        try:
            existing = json.loads(output_file.read_text())
        except Exception:
            existing = {}

    for sensor, sensor_fits in fits.items():
        existing[sensor] = {k: fit.to_dict() for k, fit in sensor_fits.items()}

    output_file.write_text(json.dumps(existing, indent=2, sort_keys=True))
    print(f'\n[soft_cal] {len(fits)} sensor(s) persisted to {output_file}')


# ── Loading / applying at runtime ───────────────────────────────────────────

def load_soft_calibrations(
    path: Path = SOFT_CAL_FILE,
) -> dict[str, dict[tuple[str, str], dict]]:
    """Load `soft_calibration.json` into the lookup shape run_stage_a.py wants.

    Returns::

        { sensor: { (region, season): {'alpha': ..., 'beta': ..., 'route': ...},
                    ... },
          ... }

    Missing file → empty dict.  Strata routed to 'none' are kept in the dict
    so the caller can apply the §7.5 NONE_PENALTY_FACTOR.
    """
    if not path.exists():
        return {}
    raw = json.loads(path.read_text())
    out: dict[str, dict[tuple[str, str], dict]] = {}
    for sensor, sensor_entries in raw.items():
        out[sensor] = {}
        for stratum_key, fit_dict in sensor_entries.items():
            region, season = stratum_key.split('|')
            out[sensor][(region, season)] = fit_dict
    return out


# ── Cosine-tapered (α, β) across region boundaries ─────────────────────────
#
# Hard region masks at lat=16 and lat=11.5 stamp a step discontinuity into the
# merged AOD wherever the two adjacent strata's (α, β) differ.  We replace the
# step with a Hann-half cosine taper in calibration space over
# ±SOFTCAL_BLEND_HALF_WIDTH around each boundary.
#
# Blending happens on (α, β) directly, not on the corrected AOD.  Linearly
# interpolating the *parameters* of an affine map is the well-defined
# operation in calibration space; interpolating the *outputs*
# (w·(α_N·sat+β_N) + (1−w)·(α_C·sat+β_C)) is algebraically equivalent here,
# but the parameter form keeps a single (α(lat), β(lat)) field that
# downstream diagnostics can introspect.
#
# 'none'/missing strata contribute identity (α=1, β=0) to the blend, so a
# correction tapers smoothly to no-op across a route mismatch rather than
# extending a CV-rejected fit into a region whose data didn't validate it.

_REGION_BOUNDARIES_NS: tuple[tuple[float, str, str], ...] = (
    # (boundary_lat, upper_region, lower_region) — walked south → north
    (CENTRAL_SOUTH_LAT, 'central', 'south'),
    (NORTH_CENTRAL_LAT, 'north',   'central'),
)


def _upper_region_weight(
    lat: np.ndarray,
    boundary: float,
    half_width: float = SOFTCAL_BLEND_HALF_WIDTH,
) -> np.ndarray:
    """Cosine-taper weight for the region *above* `boundary`.

    Returns 1 for lat ≥ boundary+half_width, 0 for lat ≤ boundary−half_width,
    and ½(1 − cos(π·t)) in between — C¹ at both edges of the band.
    """
    t = np.clip((lat - (boundary - half_width)) / (2.0 * half_width), 0.0, 1.0)
    return 0.5 * (1.0 - np.cos(np.pi * t))


def _ab_for_region(
    sensor_fits: dict[tuple[str, str], dict],
    region: str,
    season: str,
) -> tuple[float, float]:
    """(α, β) for one (region, season), with identity fallback for 'none'/missing."""
    fit = sensor_fits.get((region, season))
    if fit is None or fit.get('route') != 'apply':
        return 1.0, 0.0
    return float(fit['alpha']), float(fit['beta'])


def apply_soft_calibration_grid(
    sat: np.ndarray,
    sensor: str,
    month: int,
    soft_cals: dict[str, dict[tuple[str, str], dict]],
) -> np.ndarray:
    """Apply `sat_corrected = α(lat)·sat + β(lat)` with cosine-tapered
    (α, β) across the region boundaries.

    Within ±SOFTCAL_BLEND_HALF_WIDTH of each boundary, (α, β) are blended in
    calibration space between the two adjacent strata's fits.  Strata routed
    to 'none' (or absent) contribute identity, so corrections taper to no-op
    across route mismatches.  Sensors absent from the JSON pass through raw.

    Output is clipped to [SOFT_CAL_OUTPUT_MIN, SOFT_CAL_OUTPUT_MAX] to bound
    extrapolation when α·sat+β projects below 0 or far above MERRA-2 cap.
    """
    if sat is None:
        return None
    sensor_fits = soft_cals.get(sensor)
    if not sensor_fits:
        return sat.astype(np.float32, copy=True)

    season = _season_of(month)
    ab = {r: _ab_for_region(sensor_fits, r, season) for r in REGIONS}

    # Sequential blend south → north.  The two bands ([11, 12] and [15.5, 16.5])
    # are non-overlapping, so each boundary's blend acts independently on the
    # already-resolved (α, β) below it.
    alpha_1d = np.full(NLAT, ab['south'][0], dtype=np.float64)
    beta_1d  = np.full(NLAT, ab['south'][1], dtype=np.float64)
    for boundary, upper, _lower in _REGION_BOUNDARIES_NS:
        w = _upper_region_weight(LATS, boundary)
        a_u, b_u = ab[upper]
        alpha_1d = (1.0 - w) * alpha_1d + w * a_u
        beta_1d  = (1.0 - w) * beta_1d  + w * b_u

    alpha_2d = alpha_1d[:, None]
    beta_2d  = beta_1d[:, None]

    # NaN propagates through the affine map, so invalid cells stay NaN.
    out = (alpha_2d * sat + beta_2d).astype(np.float32)
    valid = np.isfinite(out)
    np.clip(out, SOFT_CAL_OUTPUT_MIN, SOFT_CAL_OUTPUT_MAX, out=out, where=valid)
    return out
