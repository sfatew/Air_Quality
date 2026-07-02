"""Step B1 — spatiotemporal kriging at 30-min cadence (stage_b_fixes §7.7).

**Metric** variogram (single component over the space-time metric distance):

    γ_ST(h_s, h_t) = γ_metric( √(h_s² + k² · h_t²) )

with anisotropy `k` (km / hour).  Sum-metric was dropped after the §4
empirical-variogram diagnostic showed the data cannot constrain three
components (most (h_s, h_t) bins under 30 pairs and a structural day-night
hole on the temporal axis).  Family comes from `config.B1_VARIOGRAM_SPEC`.

The kriger predicts the **(AOD − CAMS) residual** by default
(`B1_VARIOGRAM_TARGET = 'aod_minus_cams'`); CAMS is added back at the target
cell.  Set `target_kind='aod'` to recover the legacy raw-AOD behaviour.

Outputs — per-slot NC files in **parallel product tree**
`ST_KRIGING_DIR / YYYY / MM / DD / aod_YYYYMMDD_HHMM.nc`:

    aod_550nm         (lat, lon)  filled AOD (passthrough or kriged)
    is_observed       (lat, lon)  True on Stage A pass-through cells
    uncertainty       (lat, lon)  ST kriging variance (NaN on observed)
    stage_a_weight_sum (lat, lon) Stage A ICW weight sum (NaN off-observed)

Global attrs: slot_utc, slot_index, method='st_kriging', method_version,
training_window, created_at.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from multiprocessing import Pool
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import netCDF4 as nc
import gstools as gs
from scipy.spatial import cKDTree

from config import (
    LATS, LONS, NLAT, NLON,
    ST_KRIGING_DIR, RF_PRED_DIR, RF_RK_DIR, MODELS_DIR,
    B1_W_SPACE_KM, B1_W_TIME_H,
    B1_MAX_NEIGHBOURS, B1_MIN_NEIGHBOURS,
    B1_VARIOGRAM_SUBSAMPLE,
    B1_VARIOGRAM_SPEC, B1_VARIOGRAM_INIT,
    B1_VARIOGRAM_TARGET,
    B1_VARIOGRAM_PAIRS_PER_CELL, B1_VARIOGRAM_PAIRS_PER_SLOT,
    B1_VARIOGRAM_MIN_BIN_PAIRS, B1_VARIOGRAM_T_BAND_H,
    B1_VARIOGRAM_N_BINS_S, B1_VARIOGRAM_N_BINS_T,
    EARTH_RADIUS_KM,
    TRAIN_START, TRAIN_END,
    SLOTS_PER_DAY,
)
from slots import (
    load_slot, iter_local_days, iter_window_slots, slot_index,
    window_slot_indices, observed_slot_indices,
)
from ancillary import cams_aod_slot


METHOD_TAG = 'st_kriging'
METHOD_VERSION = 'v2.0'   # metric variogram on (AOD − CAMS) residual

LAT_2D, LON_2D = np.meshgrid(LATS, LONS, indexing='ij')


# ── Drift / trend provider ───────────────────────────────────────────────────
# Kriging here is regression kriging: krige a residual (AOD − trend) and add the
# trend back at the target.  `target_kind` selects the trend field:
#   'aod'             — no trend (ordinary kriging of raw AOD).
#   'aod_minus_cams'  — trend = CAMS reanalysis (the existing B1 baseline).
#   'aod_minus_rf'    — trend = full-grid RF prediction ŷ_rf (regression kriging
#                       B3); reads the precomputed RF_PRED_DIR tree so the ±time
#                       window doesn't re-run the RF.  Better drift than CAMS →
#                       smaller, more stationary residual → refit the variogram.
_TREND_KINDS = ('aod_minus_cams', 'aod_minus_rf')


@lru_cache(maxsize=192)
def _rf_pred_slot(slot_utc: datetime) -> Optional[np.ndarray]:
    """Read the precomputed full-grid RF prediction ŷ_rf for one slot, or None
    if it hasn't been generated (run rf_gapfill.predict_range first)."""
    path = (RF_PRED_DIR / f'{slot_utc.year:04d}' / f'{slot_utc.month:02d}'
            / f'{slot_utc.day:02d}' / f'pred_{slot_utc:%Y%m%d_%H%M}.nc')
    if not path.exists():
        return None
    try:
        with nc.Dataset(path) as ds:
            arr = np.ma.filled(ds.variables['rf_pred'][:].astype(np.float32), np.nan)
    except (OSError, KeyError):
        return None
    arr = np.where(arr <= -9990.0, np.nan, arr)
    return arr


def _trend_field(slot_utc: datetime, target_kind: str) -> Optional[np.ndarray]:
    """Trend grid to subtract before kriging / add back after, or None to skip
    this slot (trend unavailable).  `target_kind='aod'` returns None by design
    (no trend), which callers treat as 'no subtraction'."""
    if target_kind == 'aod_minus_cams':
        return cams_aod_slot(slot_utc)
    if target_kind == 'aod_minus_rf':
        return _rf_pred_slot(slot_utc)
    return None


def _product_dir(target_kind: str) -> Path:
    """Output tree for a given trend kind — RK writes to its own tree so it sits
    alongside `st_kriging` and `rf` for the intercomparison notebook."""
    return RF_RK_DIR if target_kind == 'aod_minus_rf' else ST_KRIGING_DIR


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


# ── Metric variogram (single component over d = √(h_s² + (k·h_t)²)) ─────────

_COV_FAMILIES = {
    'Gaussian':    gs.Gaussian,
    'Exponential': gs.Exponential,
    'Spherical':   gs.Spherical,
    'Matern':      gs.Matern,
}


@dataclass
class MetricVariogram:
    metric: gs.CovModel          # γ as a function of metric distance d (km)
    k_km_per_hour: float         # space-time anisotropy (km of equivalent h_s per h_t)

    def gamma(self, h_s_km: np.ndarray, h_t_h: np.ndarray) -> np.ndarray:
        h_s = np.asarray(h_s_km, dtype=np.float64)
        h_t = np.asarray(h_t_h,  dtype=np.float64)
        d = np.sqrt(h_s ** 2 + (self.k_km_per_hour * h_t) ** 2)
        return self.metric.variogram(d)

    def total_sill(self) -> float:
        return float(self.metric.sill)


def _build_cov(family_name: str, var: float, len_scale: float,
               nugget: float = 0.0, dim: int = 1) -> gs.CovModel:
    cls = _COV_FAMILIES[family_name]
    return cls(dim=dim, var=var, len_scale=len_scale, nugget=nugget)


# ── Structured pair sampling ────────────────────────────────────────────────
# Random pairs from a (cell × slot × day) cube are dominated by combinatorially
# common combinations: far in space AND far in time.  Short-lag bins stay empty.
# Three samplers concatenated → dense coverage of both marginals + the joint.

def _group_pairs(keys: np.ndarray, *,
                 max_per_group: int,
                 rng: np.random.Generator) -> tuple[np.ndarray, np.ndarray]:
    """Form ≤ max_per_group random pairs from each group of identical keys."""
    order = np.argsort(keys, kind='stable')
    keys_sorted = keys[order]
    breaks = np.flatnonzero(np.diff(keys_sorted)) + 1
    starts = np.r_[0, breaks]
    ends   = np.r_[breaks, keys_sorted.size]
    is_list: list[np.ndarray] = []
    js_list: list[np.ndarray] = []
    for s, e in zip(starts, ends):
        m = e - s
        if m < 2:
            continue
        idx = order[s:e]
        n_pairs = min(max_per_group, m * (m - 1) // 2)
        ii = rng.integers(0, m, size=n_pairs)
        jj = rng.integers(0, m, size=n_pairs)
        keep = ii != jj
        is_list.append(idx[ii[keep]])
        js_list.append(idx[jj[keep]])
    if not is_list:
        return (np.array([], dtype=np.int64), np.array([], dtype=np.int64))
    return np.concatenate(is_list), np.concatenate(js_list)


def _structured_pair_indices(obs_lat: np.ndarray,
                             obs_lon: np.ndarray,
                             obs_time_h: np.ndarray,
                             *,
                             n_mixed: int,
                             rng_seed: int = 0) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Three concatenated samplers.

    Returns (i, j, src) where src is 0=same-cell, 1=same-slot, 2=mixed — useful
    for diagnostics.
    """
    rng = np.random.default_rng(rng_seed)
    n = obs_lat.size

    # Same-cell: encode (lat, lon) into an int64 key (production grid is fixed).
    keys_cell = ((np.round(obs_lat, 5) * 1_000_000).astype(np.int64) * 100_000_000
                 + (np.round(obs_lon, 5) * 1_000_000).astype(np.int64))
    cell_i, cell_j = _group_pairs(keys_cell,
                                  max_per_group=B1_VARIOGRAM_PAIRS_PER_CELL,
                                  rng=rng)

    # Same-slot: t_h is already in hours-from-epoch, equal slot ⇒ equal float.
    slot_i, slot_j = _group_pairs(obs_time_h,
                                  max_per_group=B1_VARIOGRAM_PAIRS_PER_SLOT,
                                  rng=rng)

    # Mixed: truly random pairs over the whole pool.
    n_target = max(0, min(n_mixed, n * (n - 1) // 2))
    mix_i = rng.integers(0, n, size=n_target)
    mix_j = rng.integers(0, n, size=n_target)
    keep  = mix_i != mix_j
    mix_i, mix_j = mix_i[keep], mix_j[keep]

    i = np.concatenate([cell_i, slot_i, mix_i])
    j = np.concatenate([cell_j, slot_j, mix_j])
    src = np.concatenate([
        np.zeros(cell_i.size, dtype=np.uint8),
        np.ones (slot_i.size, dtype=np.uint8),
        np.full (mix_i.size, 2, dtype=np.uint8),
    ])
    return i, j, src


def _empirical_st_variogram(obs_lat, obs_lon, obs_time_h, obs_val,
                            *,
                            n_mixed_pairs: int = B1_VARIOGRAM_SUBSAMPLE,
                            t_band_h: float = B1_VARIOGRAM_T_BAND_H,
                            s_max_km: float | None = None,
                            min_bin_pairs: int = B1_VARIOGRAM_MIN_BIN_PAIRS,
                            n_bins_s: int = B1_VARIOGRAM_N_BINS_S,
                            n_bins_t: int = B1_VARIOGRAM_N_BINS_T,
                            rng_seed: int = 0,
                            return_counts: bool = False):
    """Empirical (h_s, h_t) variogram from structured pair samples.

    * Pairs sampled by three concatenated samplers (same-cell, same-slot, mixed)
      so short-lag bins are densely populated.
    * Pairs with h_t > t_band_h are dropped (Stage A has a structural cross-day
      gap; including those pairs warps the temporal axis).
    * Bins with fewer than min_bin_pairs samples are returned as NaN — the
      fitter then ignores them.
    """
    n = obs_val.size
    if n < B1_MIN_NEIGHBOURS:
        raise ValueError(f'Too few obs ({n}) for variogram fit.')
    if s_max_km is None:
        s_max_km = B1_W_SPACE_KM * 1.5

    i, j, _src = _structured_pair_indices(
        obs_lat, obs_lon, obs_time_h,
        n_mixed=n_mixed_pairs, rng_seed=rng_seed,
    )
    if i.size == 0:
        raise RuntimeError('Structured samplers produced zero pairs.')

    h_s = haversine_km(obs_lat[i], obs_lon[i], obs_lat[j], obs_lon[j])
    h_t = np.abs(obs_time_h[i] - obs_time_h[j])
    keep = (h_t <= t_band_h) & (h_s <= s_max_km)
    h_s, h_t = h_s[keep], h_t[keep]
    gamma = 0.5 * (obs_val[i[keep]] - obs_val[j[keep]]) ** 2

    s_edges = np.linspace(0, s_max_km, n_bins_s + 1)
    t_edges = np.linspace(0, t_band_h, n_bins_t + 1)
    s_centres = 0.5 * (s_edges[:-1] + s_edges[1:])
    t_centres = 0.5 * (t_edges[:-1] + t_edges[1:])
    s_idx = np.clip(np.digitize(h_s, s_edges) - 1, 0, n_bins_s - 1)
    t_idx = np.clip(np.digitize(h_t, t_edges) - 1, 0, n_bins_t - 1)
    g_sum = np.zeros((n_bins_s, n_bins_t), dtype=np.float64)
    g_cnt = np.zeros_like(g_sum, dtype=np.int64)
    np.add.at(g_sum, (s_idx, t_idx), gamma)
    np.add.at(g_cnt, (s_idx, t_idx), 1)
    with np.errstate(invalid='ignore', divide='ignore'):
        g_mean = np.where(g_cnt >= min_bin_pairs,
                          g_sum / np.maximum(g_cnt, 1),
                          np.nan)
    if return_counts:
        return s_centres, t_centres, g_mean, g_cnt
    return s_centres, t_centres, g_mean


def fit_variogram(obs_lat, obs_lon, obs_time_h, obs_val) -> MetricVariogram:
    s_c, t_c, g_grid, g_cnt = _empirical_st_variogram(
        obs_lat, obs_lon, obs_time_h, obs_val, return_counts=True,
    )
    valid = np.isfinite(g_grid)
    if valid.sum() < 6:
        raise RuntimeError(
            f'Empirical variogram has {valid.sum()} populated bins (need ≥6). '
            f'Try lowering B1_VARIOGRAM_MIN_BIN_PAIRS '
            f'(current={B1_VARIOGRAM_MIN_BIN_PAIRS}) or sampling more pairs.'
        )
    S, T = np.meshgrid(s_c, t_c, indexing='ij')
    h_s_flat = S[valid]
    h_t_flat = T[valid]
    g_flat   = g_grid[valid]
    # Inverse-variance-style weighting: bins with more pairs are more trusted.
    w_flat   = np.sqrt(g_cnt[valid].astype(np.float64))

    init = B1_VARIOGRAM_INIT['metric']

    def _unpack(params):
        var, len_scale, nugget, k = params
        return MetricVariogram(
            metric=_build_cov(B1_VARIOGRAM_SPEC['metric'], var, len_scale, nugget),
            k_km_per_hour=float(k),
        )

    def resid(params):
        try:
            model = _unpack(params)
            pred = model.gamma(h_s_flat, h_t_flat)
        except Exception:
            return np.full_like(g_flat, 1e6)
        return (pred - g_flat) * w_flat

    x0 = np.array([
        init['var'], init['len_scale_km'], init['nugget'] + 1e-5,
        B1_VARIOGRAM_INIT['k_km_per_hour'],
    ], dtype=np.float64)
    lb = np.array([1e-4, 1.0,                1e-5,                  1e-3])
    ub = np.array([10.0, B1_W_SPACE_KM * 20, 1.0,                   B1_W_SPACE_KM])

    from scipy.optimize import least_squares
    res = least_squares(resid, x0=x0, bounds=(lb, ub), max_nfev=500)
    return _unpack(res.x)


# ── Variogram persistence ────────────────────────────────────────────────────

_VGM_PATH = MODELS_DIR / 'st_variogram.json'


def _vgm_path(target_kind: str = B1_VARIOGRAM_TARGET) -> Path:
    """Variogram file for a trend kind.  RK keeps its own so the RF-residual
    variogram never clobbers the CAMS-residual baseline."""
    if target_kind == 'aod_minus_rf':
        return MODELS_DIR / 'st_variogram_rf.json'
    return _VGM_PATH


def save_variogram(vgm: MetricVariogram, path: Optional[Path] = None,
                   target_kind: str = B1_VARIOGRAM_TARGET) -> Path:
    path = path or _vgm_path(target_kind)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'kind':   'metric',
        'spec':   B1_VARIOGRAM_SPEC,
        'target': target_kind,
        'metric': {'var': vgm.metric.var,
                   'len_scale': vgm.metric.len_scale,
                   'nugget': vgm.metric.nugget},
        'k_km_per_hour': vgm.k_km_per_hour,
    }
    path.write_text(json.dumps(payload, indent=2, default=float))
    return path


def load_variogram(path: Optional[Path] = None) -> MetricVariogram:
    path = path or _VGM_PATH
    payload = json.loads(path.read_text())
    if payload.get('kind') != 'metric':
        raise ValueError(
            f'{path} is not a metric variogram (kind={payload.get("kind")!r}). '
            f'Re-fit with the current code: kriging.fit_and_save_variogram(...).'
        )
    return MetricVariogram(
        metric = _build_cov(payload['spec']['metric'],
                            **{k: payload['metric'][k]
                               for k in ('var', 'len_scale', 'nugget')}),
        k_km_per_hour = payload['k_km_per_hour'],
    )


# ── Training-pair collection ─────────────────────────────────────────────────

def collect_training_pairs(
    start: date = TRAIN_START,
    end:   date = TRAIN_END,
    target_kind: str = B1_VARIOGRAM_TARGET,
    max_pairs: int = B1_VARIOGRAM_SUBSAMPLE,
    rng_seed: int = 0,
    progress: Optional[Callable] = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Flat pool of obs samples over [start, end].

    target_kind:
      'aod'             — returns Stage A AOD directly.
      'aod_minus_cams'  — returns (Stage A − CAMS) residual; slots without
                          CAMS coverage are skipped entirely.
      'aod_minus_rf'    — returns (Stage A − ŷ_rf) residual; slots without a
                          precomputed RF prediction are skipped entirely.
    """
    if target_kind not in ('aod', *_TREND_KINDS):
        raise ValueError(f'Unknown target_kind={target_kind!r}')
    rng = np.random.default_rng(rng_seed)
    epoch = datetime(start.year, start.month, start.day)
    lats, lons, ts, vals = [], [], [], []
    n_days = max((end - start).days, 1)
    target_per_slot = max(50, max_pairs // (SLOTS_PER_DAY * n_days))

    days = list(iter_local_days(start, end))
    iterator = progress(days, desc='vario pairs') if progress is not None else days
    for d in iterator:
        for slot_utc in iter_window_slots(d):
            payload = load_slot(slot_utc)
            if payload is None:
                continue
            valid = np.isfinite(payload['aod']) & (payload['aod'] >= 0)

            trend_field = None
            if target_kind in _TREND_KINDS:
                trend_field = _trend_field(slot_utc, target_kind)
                if trend_field is None:
                    continue
                valid &= np.isfinite(trend_field)

            rows, cols = np.where(valid)
            if rows.size == 0:
                continue
            n_take = min(target_per_slot, rows.size)
            pick = rng.choice(rows.size, size=n_take, replace=False)
            r = rows[pick]; c = cols[pick]
            aod = payload['aod'][r, c].astype(np.float64)
            if target_kind in _TREND_KINDS:
                aod = aod - trend_field[r, c].astype(np.float64)
            lats.append(LATS[r])
            lons.append(LONS[c])
            t_hours = (slot_utc - epoch).total_seconds() / 3600.0
            ts.append(np.full(n_take, t_hours, dtype=np.float64))
            vals.append(aod)

    if not vals:
        raise RuntimeError(f'No valid Stage A slots in [{start}, {end}]')
    return (np.concatenate(lats), np.concatenate(lons),
            np.concatenate(ts),   np.concatenate(vals))


def fit_and_save_variogram(start: date = TRAIN_START,
                            end:   date = TRAIN_END,
                            target_kind: str = B1_VARIOGRAM_TARGET,
                            progress: Optional[Callable] = None) -> MetricVariogram:
    lat, lon, t_h, val = collect_training_pairs(
        start, end, target_kind=target_kind, progress=progress,
    )
    vgm = fit_variogram(lat, lon, t_h, val)
    save_variogram(vgm, target_kind=target_kind)
    return vgm


def fit_and_save_rk_variogram_oof(
    bundle_name: str = 'rf_default_residual',
    holdout_fraction: float = 0.20,
    progress: Optional[Callable] = None,
) -> MetricVariogram:
    """Fit the regression-kriging variogram on OUT-OF-FOLD RF residuals.

    This is the correct RK variogram: in-sample train residuals are degenerate
    (memorised bias), and the test window is off-limits.  A single temporal
    holdout inside train yields honest out-of-sample residual structure.  Uses
    the production bundle's hyperparams so the holdout model matches the drift
    model actually shipped.  Saves to the RK variogram slot (`aod_minus_rf`).
    """
    from rf_gapfill import collect_oof_residual_pairs, load_bundle  # lazy: heavy deps
    bundle = load_bundle(bundle_name)
    lat, lon, t_h, resid = collect_oof_residual_pairs(
        holdout_fraction=holdout_fraction,
        hp=bundle.hyperparams,
        target_kind=bundle.target_kind,
        progress=progress,
    )
    vgm = fit_variogram(lat, lon, t_h, resid)
    save_variogram(vgm, path=_vgm_path('aod_minus_rf'), target_kind='aod_minus_rf')
    return vgm


# ── ST kriging — per-target solve ────────────────────────────────────────────

@dataclass
class _SlotObservations:
    lat:    np.ndarray
    lon:    np.ndarray
    t_hour: np.ndarray
    val:    np.ndarray


def _gather_window_observations(target_utc: datetime,
                                w_t_hours: float = B1_W_TIME_H,
                                target_kind: str = B1_VARIOGRAM_TARGET) -> _SlotObservations:
    pool_lat, pool_lon, pool_t, pool_v = [], [], [], []
    for day_offset in (-1, 0, 1):
        d = target_utc.date() + timedelta(days=day_offset)
        for s_utc in iter_window_slots(d):
            dt_h = (s_utc - target_utc).total_seconds() / 3600.0
            if abs(dt_h) > w_t_hours:
                continue
            payload = load_slot(s_utc)
            if payload is None:
                continue
            valid = np.isfinite(payload['aod']) & (payload['aod'] >= 0)

            trend_field = None
            if target_kind in _TREND_KINDS:
                trend_field = _trend_field(s_utc, target_kind)
                if trend_field is None:
                    continue
                valid &= np.isfinite(trend_field)

            r, c = np.where(valid)
            if r.size == 0:
                continue
            aod = payload['aod'][r, c].astype(np.float64)
            if target_kind in _TREND_KINDS:
                aod = aod - trend_field[r, c].astype(np.float64)
            pool_lat.append(LATS[r])
            pool_lon.append(LONS[c])
            pool_t.append(np.full(r.size, dt_h, dtype=np.float64))
            pool_v.append(aod)

    if not pool_v:
        return _SlotObservations(np.array([]), np.array([]),
                                  np.array([]), np.array([]))
    return _SlotObservations(
        lat=np.concatenate(pool_lat), lon=np.concatenate(pool_lon),
        t_hour=np.concatenate(pool_t), val=np.concatenate(pool_v),
    )


def _latlon_to_unit_xyz(lat_deg: np.ndarray, lon_deg: np.ndarray) -> np.ndarray:
    """Project (lat°, lon°) onto the unit sphere — chord distance ≈ great-circle
    distance / EARTH_RADIUS_KM for short ranges.  Returns (..., 3) array."""
    lat = np.radians(lat_deg)
    lon = np.radians(lon_deg)
    cos_lat = np.cos(lat)
    return np.stack([cos_lat * np.cos(lon),
                     cos_lat * np.sin(lon),
                     np.sin(lat)], axis=-1)


def _build_pool_index(pool: _SlotObservations) -> Optional[cKDTree]:
    """Build a cKDTree over the pool's unit-sphere coords.  Returns None if
    the pool is empty."""
    if pool.val.size == 0:
        return None
    xyz = _latlon_to_unit_xyz(pool.lat, pool.lon)
    return cKDTree(xyz)


def _krige_target(tgt_lat: float, tgt_lon: float,
                  pool: _SlotObservations,
                  tree: Optional[cKDTree],
                  vgm: MetricVariogram,
                  w_s_km: float = B1_W_SPACE_KM,
                  k_max: int = B1_MAX_NEIGHBOURS) -> tuple[float, float]:
    if pool.val.size == 0 or tree is None:
        return np.nan, np.nan
    # Chord distance on the unit sphere corresponding to w_s_km on the great circle.
    r_chord = 2.0 * np.sin(0.5 * w_s_km / EARTH_RADIUS_KM)
    tgt_xyz = _latlon_to_unit_xyz(np.array([tgt_lat]), np.array([tgt_lon]))[0]
    idx_list = tree.query_ball_point(tgt_xyz, r=r_chord)
    if len(idx_list) < B1_MIN_NEIGHBOURS:
        return np.nan, np.nan
    idx = np.asarray(idx_list, dtype=np.int64)
    h_s = haversine_km(tgt_lat, tgt_lon, pool.lat[idx], pool.lon[idx])
    h_t = np.abs(pool.t_hour[idx])
    val = pool.val[idx]
    nn_lat = pool.lat[idx]
    nn_lon = pool.lon[idx]
    nn_t   = pool.t_hour[idx]

    if h_s.size > k_max:
        combined = np.sqrt(h_s ** 2 + (vgm.k_km_per_hour * h_t) ** 2)
        pick = np.argpartition(combined, k_max)[:k_max]
        h_s = h_s[pick]; h_t = h_t[pick]; val = val[pick]
        nn_lat = nn_lat[pick]; nn_lon = nn_lon[pick]; nn_t = nn_t[pick]

    K = h_s.size
    pair_s = haversine_km(nn_lat[:, None], nn_lon[:, None],
                          nn_lat[None, :], nn_lon[None, :])
    pair_t = np.abs(nn_t[:, None] - nn_t[None, :])

    A = np.empty((K + 1, K + 1), dtype=np.float64)
    A[:K, :K] = vgm.gamma(pair_s, pair_t)
    A[-1, :K] = 1.0; A[:K, -1] = 1.0; A[-1, -1] = 0.0

    b = np.empty(K + 1, dtype=np.float64)
    b[:K] = vgm.gamma(h_s, h_t)
    b[-1] = 1.0

    try:
        w = np.linalg.solve(A, b)
    except np.linalg.LinAlgError:
        return np.nan, np.nan
    estimate = float(np.dot(w[:K], val))
    variance = float(np.dot(w[:K], b[:K]) + w[-1])
    if not np.isfinite(estimate):
        return np.nan, np.nan
    # NOTE: do not clip here — for residual kriging the estimate is (AOD − CAMS)
    # and is allowed to be negative.  Final [0, 5] clamp is applied in krige_slot
    # after CAMS has been added back.
    return estimate, max(variance, 0.0)


# ── Output file path + writer ────────────────────────────────────────────────

def kriged_path(slot_utc: datetime, out_dir: Path = ST_KRIGING_DIR) -> Path:
    return (out_dir
            / f'{slot_utc.year:04d}'
            / f'{slot_utc.month:02d}'
            / f'{slot_utc.day:02d}'
            / f'aod_{slot_utc:%Y%m%d_%H%M}.nc')


def _write_slot(slot_utc: datetime,
                 aod: np.ndarray,
                 is_observed: np.ndarray,
                 uncertainty: np.ndarray,
                 weight_sum: np.ndarray,
                 training_window: str = '',
                 out_dir: Path = ST_KRIGING_DIR) -> Path:
    out = kriged_path(slot_utc, out_dir=out_dir)
    out.parent.mkdir(parents=True, exist_ok=True)
    s_idx = slot_index(slot_utc)
    with nc.Dataset(out, 'w', format='NETCDF4') as ds:
        ds.title              = 'Vietnam Stage B 30-min AOD — ST kriging product (Yang & Hu 2018)'
        ds.method             = METHOD_TAG
        ds.method_version     = METHOD_VERSION
        ds.slot_utc           = slot_utc.replace(tzinfo=timezone.utc).isoformat()
        ds.slot_index         = s_idx
        ds.training_window    = training_window
        ds.created_at         = datetime.now(timezone.utc).isoformat()
        ds.uncertainty_units  = 'AOD^2 (kriging variance)'
        ds.Conventions        = 'CF-1.8'
        ds.createDimension('lat', NLAT)
        ds.createDimension('lon', NLON)
        ds.createVariable('lat', 'f4', ('lat',))[:] = LATS.astype(np.float32)
        ds.createVariable('lon', 'f4', ('lon',))[:] = LONS.astype(np.float32)

        def _add(name, arr, long_name, dtype='f4', fill=-9999.0, units='1'):
            v = ds.createVariable(name, dtype, ('lat', 'lon'),
                                  fill_value=fill, zlib=True, complevel=4)
            v.long_name = long_name; v.units = units
            data = arr if dtype != 'f4' else np.where(np.isfinite(arr), arr, fill)
            v[:] = data

        _add('aod_550nm',          aod,          'Filled AOD at 550 nm (observed or ST-kriged)')
        _add('is_observed',        is_observed.astype(np.uint8),
             'True where this cell was a Stage A observation pass-through',
             dtype='u1', fill=0)
        _add('uncertainty',        uncertainty,
             'ST kriging variance (AOD²) at filled cells; NaN on observed')
        _add('stage_a_weight_sum', weight_sum,
             'Stage A ICW weight sum at observed cells; NaN off-observed', units='count')
    return out


# ── Slot-level driver ────────────────────────────────────────────────────────

def krige_slot(slot_utc: datetime, vgm: MetricVariogram,
                training_window: str = '',
                target_kind: str = B1_VARIOGRAM_TARGET) -> Optional[Path]:
    """Krige one 30-min slot and write the NC file.

    Per Yang & Hu (2018): cells where the spatiotemporal pool is empty or
    where the kriging solve fails are left as NaN (no climatology backstop).

    target_kind='aod_minus_cams' (default): the kriger predicts a residual and
    CAMS is added back at the target cell.  Cells where CAMS is unavailable at
    the target slot are left as NaN.

    target_kind='aod_minus_rf' (regression kriging B3): identical machinery, but
    the drift added back at the target is the precomputed RF prediction ŷ_rf.
    Cells where ŷ_rf is unavailable stay NaN.  Falls back to ŷ_rf wherever the
    residual pool is empty (kriged residual → not written, cell left NaN); to
    keep the graceful RF fallback, unfilled gap cells are backfilled with ŷ_rf
    below.
    """
    payload = load_slot(slot_utc)
    if payload is not None:
        obs = payload['aod'].astype(np.float32)
        is_observed = np.isfinite(obs) & (obs >= 0)
        weight_sum  = np.where(is_observed, payload['weight_sum'], np.nan).astype(np.float32)
    else:
        obs = np.full((NLAT, NLON), np.nan, dtype=np.float32)
        is_observed = np.zeros((NLAT, NLON), dtype=bool)
        weight_sum  = np.full((NLAT, NLON), np.nan, dtype=np.float32)

    final       = np.where(is_observed, obs, np.nan).astype(np.float32)
    uncertainty = np.full((NLAT, NLON), np.nan, dtype=np.float32)

    missing = np.argwhere(~is_observed)
    if missing.size > 0:
        trend_target = None
        if target_kind in _TREND_KINDS:
            trend_target = _trend_field(slot_utc, target_kind)
            # Cells with no trend coverage stay NaN — handled per-pixel below.

        pool = _gather_window_observations(slot_utc, B1_W_TIME_H,
                                            target_kind=target_kind)
        if pool.val.size >= B1_MIN_NEIGHBOURS:
            tree = _build_pool_index(pool)
            for (r, c) in missing:
                est, var = _krige_target(float(LATS[r]), float(LONS[c]),
                                          pool, tree, vgm)
                if not np.isfinite(est):
                    continue
                if target_kind in _TREND_KINDS:
                    if trend_target is None or not np.isfinite(trend_target[r, c]):
                        continue
                    est = est + float(trend_target[r, c])
                final[r, c]       = np.float32(np.clip(est, 0.0, 5.0))
                uncertainty[r, c] = np.float32(var)

        # Regression-kriging graceful fallback: any gap cell the residual pool
        # couldn't reach (no neighbours in the ±time window) falls back to the
        # pure RF drift ŷ_rf — i.e. kriged residual r̂ → 0, ŷ = ŷ_rf.  This is
        # the whole point of RK: never worse than the RF baseline in a gap.
        if target_kind == 'aod_minus_rf' and trend_target is not None:
            gap = ~is_observed & ~np.isfinite(final) & np.isfinite(trend_target)
            final[gap] = np.clip(trend_target[gap], 0.0, 5.0).astype(np.float32)

    out_dir = _product_dir(target_kind)
    return _write_slot(slot_utc, final, is_observed, uncertainty, weight_sum,
                        training_window=training_window, out_dir=out_dir)


# ── End-to-end driver ───────────────────────────────────────────────────────

# Worker-side state: each worker process loads the variogram once from disk
# in its initializer, then re-uses it across all slots it processes.  This is
# more robust than pickling gstools CovModel instances across the pipe.
_WORKER_VGM: Optional[MetricVariogram] = None
_WORKER_TARGET_KIND: str = B1_VARIOGRAM_TARGET


def _init_worker(vgm_path_str: str, target_kind: str):
    global _WORKER_VGM, _WORKER_TARGET_KIND
    os.environ.setdefault('HDF5_USE_FILE_LOCKING', 'FALSE')
    _WORKER_VGM = load_variogram(Path(vgm_path_str))
    _WORKER_TARGET_KIND = target_kind


def _worker_krige_slot(args: tuple[datetime, str]) -> bool:
    slot_utc, training_window = args
    try:
        krige_slot(slot_utc, _WORKER_VGM, training_window=training_window,
                   target_kind=_WORKER_TARGET_KIND)
        return True
    except Exception:
        return False


def run(start: date,
        end:   date,
        vgm:   Optional[MetricVariogram] = None,
        training_window: Optional[str] = None,
        overwrite: bool = False,
        refit_variogram: bool = False,
        target_kind: str = B1_VARIOGRAM_TARGET,
        progress: Optional[Callable] = None,
        n_workers: Optional[int] = None) -> int:
    """Kriging over [start, end].  Optionally fits the variogram first.

    `target_kind` selects the drift (see the module-level trend-provider note):
    the default CAMS-residual baseline writes to `ST_KRIGING_DIR`; 'aod_minus_rf'
    is regression kriging (B3) and writes to `RF_RK_DIR` — run
    `rf_gapfill.predict_range` first so the RF drift tree exists.
    Cells the kriger cannot estimate are left as NaN (Yang & Hu 2018), except RK
    gap cells with no neighbours, which fall back to ŷ_rf (see krige_slot).

    Parallelism: by default uses `os.cpu_count() - 1` worker processes.  Pass
    `n_workers=1` for a serial (debuggable) run, or any positive int to cap.
    """
    vgm_path = _vgm_path(target_kind)
    if vgm is None:
        if vgm_path.exists() and not refit_variogram:
            vgm = load_variogram(vgm_path)
        else:
            vgm = fit_and_save_variogram(target_kind=target_kind, progress=progress)
    # Workers load the variogram from disk; make sure it's there.
    if not vgm_path.exists():
        save_variogram(vgm, path=vgm_path, target_kind=target_kind)

    if training_window is None:
        training_window = f'{TRAIN_START.isoformat()} to {TRAIN_END.isoformat()}'

    out_dir = _product_dir(target_kind)
    slots: list[datetime] = []
    for d in iter_local_days(start, end):
        for slot_utc in iter_window_slots(d):
            if kriged_path(slot_utc, out_dir=out_dir).exists() and not overwrite:
                continue
            slots.append(slot_utc)
    if not slots:
        return 0

    if n_workers is None:
        n_workers = max(1, (os.cpu_count() or 2) - 1)

    if n_workers <= 1:
        iterator = progress(slots, desc='ST-krige') if progress is not None else slots
        n_written = 0
        for slot_utc in iterator:
            krige_slot(slot_utc, vgm, training_window=training_window,
                       target_kind=target_kind)
            n_written += 1
        return n_written

    args_iter = [(s, training_window) for s in slots]
    n_written = 0
    with Pool(processes=n_workers, initializer=_init_worker,
              initargs=(str(vgm_path), target_kind)) as pool:
        result_iter = pool.imap_unordered(_worker_krige_slot, args_iter, chunksize=1)
        if progress is not None:
            result_iter = progress(result_iter, total=len(slots), desc='ST-krige')
        for ok in result_iter:
            if ok:
                n_written += 1
    return n_written
