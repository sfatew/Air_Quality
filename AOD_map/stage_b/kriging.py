"""Step B1 — spatiotemporal kriging at 30-min cadence (stage_b_fixes §7.7).

Yang & Hu (2018) sum-metric variogram:

    γ_ST(h_s, h_t) = γ_S(h_s) + γ_T(h_t) + γ_J( √(h_s² + k² · h_t²) )

with spatial, temporal, and joint sub-components plus anisotropy `k`
(km / hour).  Sub-component families come from `config.B1_VARIOGRAM_SPEC`.

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
from multiprocessing import Pool
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import netCDF4 as nc
import gstools as gs
from scipy.spatial import cKDTree

from config import (
    LATS, LONS, NLAT, NLON,
    ST_KRIGING_DIR, MODELS_DIR,
    B1_W_SPACE_KM, B1_W_TIME_H,
    B1_MAX_NEIGHBOURS, B1_MIN_NEIGHBOURS,
    B1_VARIOGRAM_SUBSAMPLE,
    B1_VARIOGRAM_SPEC, B1_VARIOGRAM_INIT,
    EARTH_RADIUS_KM,
    TRAIN_START, TRAIN_END,
    SLOTS_PER_DAY,
)
from slots import (
    load_slot, iter_local_days, iter_window_slots, slot_index,
    window_slot_indices, observed_slot_indices,
)


METHOD_TAG = 'st_kriging'
METHOD_VERSION = 'v1.0'

LAT_2D, LON_2D = np.meshgrid(LATS, LONS, indexing='ij')


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


# ── Sum-metric variogram ─────────────────────────────────────────────────────

_COV_FAMILIES = {
    'Gaussian':    gs.Gaussian,
    'Exponential': gs.Exponential,
    'Spherical':   gs.Spherical,
    'Matern':      gs.Matern,
}


@dataclass
class SumMetricVariogram:
    spatial:  gs.CovModel
    temporal: gs.CovModel
    joint:    gs.CovModel
    k_km_per_hour: float

    def gamma(self, h_s_km: np.ndarray, h_t_h: np.ndarray) -> np.ndarray:
        h_s = np.asarray(h_s_km, dtype=np.float64)
        h_t = np.asarray(h_t_h,  dtype=np.float64)
        g_s = self.spatial.variogram(h_s)
        g_t = self.temporal.variogram(h_t)
        joint_h = np.sqrt(h_s ** 2 + (self.k_km_per_hour * h_t) ** 2)
        g_j = self.joint.variogram(joint_h)
        return g_s + g_t + g_j

    def total_sill(self) -> float:
        return float(self.spatial.sill + self.temporal.sill + self.joint.sill)


def _build_cov(family_name: str, var: float, len_scale: float,
               nugget: float = 0.0, dim: int = 1) -> gs.CovModel:
    cls = _COV_FAMILIES[family_name]
    return cls(dim=dim, var=var, len_scale=len_scale, nugget=nugget)


def _empirical_st_variogram(obs_lat, obs_lon, obs_time_h, obs_val,
                            n_pairs: int = B1_VARIOGRAM_SUBSAMPLE,
                            rng_seed: int = 0):
    n = obs_val.size
    if n < B1_MIN_NEIGHBOURS:
        raise ValueError(f'Too few obs ({n}) for variogram fit.')
    rng = np.random.default_rng(rng_seed)
    n_target = min(n_pairs, n * (n - 1) // 2)
    i = rng.integers(0, n, size=n_target)
    j = rng.integers(0, n, size=n_target)
    mask = i != j
    i, j = i[mask], j[mask]
    h_s = haversine_km(obs_lat[i], obs_lon[i], obs_lat[j], obs_lon[j])
    h_t = np.abs(obs_time_h[i] - obs_time_h[j])
    gamma = 0.5 * (obs_val[i] - obs_val[j]) ** 2

    s_edges = np.linspace(0, B1_W_SPACE_KM * 1.5, 12)
    t_edges = np.linspace(0, B1_W_TIME_H  * 1.5, 12)
    s_centres = 0.5 * (s_edges[:-1] + s_edges[1:])
    t_centres = 0.5 * (t_edges[:-1] + t_edges[1:])
    s_idx = np.clip(np.digitize(h_s, s_edges) - 1, 0, len(s_centres) - 1)
    t_idx = np.clip(np.digitize(h_t, t_edges) - 1, 0, len(t_centres) - 1)
    g_sum   = np.zeros((len(s_centres), len(t_centres)), dtype=np.float64)
    g_count = np.zeros_like(g_sum, dtype=np.int64)
    np.add.at(g_sum,   (s_idx, t_idx), gamma)
    np.add.at(g_count, (s_idx, t_idx), 1)
    with np.errstate(invalid='ignore', divide='ignore'):
        g_mean = np.where(g_count > 0, g_sum / g_count, np.nan)
    return s_centres, t_centres, g_mean


def fit_variogram(obs_lat, obs_lon, obs_time_h, obs_val) -> SumMetricVariogram:
    s_c, t_c, g_grid = _empirical_st_variogram(obs_lat, obs_lon, obs_time_h, obs_val)
    valid = np.isfinite(g_grid)
    if valid.sum() < 6:
        raise RuntimeError('Empirical ST variogram has <6 populated bins.')
    S, T = np.meshgrid(s_c, t_c, indexing='ij')
    h_s_flat = S[valid]
    h_t_flat = T[valid]
    g_flat   = g_grid[valid]

    init = B1_VARIOGRAM_INIT

    def _unpack(params):
        var_s, len_s, nug_s, var_t, len_t, nug_t, var_j, len_j, nug_j, k = params
        return SumMetricVariogram(
            spatial  = _build_cov(B1_VARIOGRAM_SPEC['spatial'],  var_s, len_s, nug_s),
            temporal = _build_cov(B1_VARIOGRAM_SPEC['temporal'], var_t, len_t, nug_t),
            joint    = _build_cov(B1_VARIOGRAM_SPEC['joint'],    var_j, len_j, nug_j),
            k_km_per_hour = k,
        )

    def resid(params):
        try:
            model = _unpack(np.clip(params, 1e-6, None))
            pred = model.gamma(h_s_flat, h_t_flat)
        except Exception:
            return np.full_like(g_flat, 1e6)
        return pred - g_flat

    x0 = np.array([
        init['spatial']['var'],  init['spatial']['len_scale_km'],     init['spatial']['nugget'] + 1e-6,
        init['temporal']['var'], init['temporal']['len_scale_hours'], init['temporal']['nugget'] + 1e-6,
        init['joint']['var'],    init['joint']['len_scale_km'],       init['joint']['nugget'] + 1e-6,
        init['k_km_per_hour'],
    ], dtype=np.float64)

    from scipy.optimize import least_squares
    res = least_squares(
        resid, x0=x0,
        bounds=(1e-6, [
            10.0, B1_W_SPACE_KM * 4, 1.0,
            10.0, B1_W_TIME_H  * 4, 1.0,
            10.0, B1_W_SPACE_KM * 20, 1.0,
            B1_W_SPACE_KM,
        ]),
        max_nfev=300,
    )
    return _unpack(res.x)


# ── Variogram persistence ────────────────────────────────────────────────────

_VGM_PATH = MODELS_DIR / 'st_variogram.json'


def save_variogram(vgm: SumMetricVariogram, path: Optional[Path] = None) -> Path:
    path = path or _VGM_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'spec': B1_VARIOGRAM_SPEC,
        'spatial':  {'var': vgm.spatial.var,  'len_scale': vgm.spatial.len_scale,  'nugget': vgm.spatial.nugget},
        'temporal': {'var': vgm.temporal.var, 'len_scale': vgm.temporal.len_scale, 'nugget': vgm.temporal.nugget},
        'joint':    {'var': vgm.joint.var,    'len_scale': vgm.joint.len_scale,    'nugget': vgm.joint.nugget},
        'k_km_per_hour': vgm.k_km_per_hour,
    }
    path.write_text(json.dumps(payload, indent=2, default=float))
    return path


def load_variogram(path: Optional[Path] = None) -> SumMetricVariogram:
    path = path or _VGM_PATH
    payload = json.loads(path.read_text())
    return SumMetricVariogram(
        spatial  = _build_cov(payload['spec']['spatial'],  **{k: payload['spatial'] [k] for k in ('var', 'len_scale', 'nugget')}),
        temporal = _build_cov(payload['spec']['temporal'], **{k: payload['temporal'][k] for k in ('var', 'len_scale', 'nugget')}),
        joint    = _build_cov(payload['spec']['joint'],    **{k: payload['joint']   [k] for k in ('var', 'len_scale', 'nugget')}),
        k_km_per_hour = payload['k_km_per_hour'],
    )


# ── Training-pair collection ─────────────────────────────────────────────────

def collect_training_pairs(
    start: date = TRAIN_START,
    end:   date = TRAIN_END,
    max_pairs: int = B1_VARIOGRAM_SUBSAMPLE,
    rng_seed: int = 0,
    progress: Optional[Callable] = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
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
            rows, cols = np.where(valid)
            if rows.size == 0:
                continue
            n_take = min(target_per_slot, rows.size)
            pick = rng.choice(rows.size, size=n_take, replace=False)
            r = rows[pick]; c = cols[pick]
            lats.append(LATS[r])
            lons.append(LONS[c])
            t_hours = (slot_utc - epoch).total_seconds() / 3600.0
            ts.append(np.full(n_take, t_hours, dtype=np.float64))
            vals.append(payload['aod'][r, c].astype(np.float64))

    if not vals:
        raise RuntimeError(f'No valid Stage A slots in [{start}, {end}]')
    return (np.concatenate(lats), np.concatenate(lons),
            np.concatenate(ts),   np.concatenate(vals))


def fit_and_save_variogram(start: date = TRAIN_START,
                            end:   date = TRAIN_END,
                            progress: Optional[Callable] = None) -> SumMetricVariogram:
    lat, lon, t_h, val = collect_training_pairs(start, end, progress=progress)
    vgm = fit_variogram(lat, lon, t_h, val)
    save_variogram(vgm)
    return vgm


# ── ST kriging — per-target solve ────────────────────────────────────────────

@dataclass
class _SlotObservations:
    lat:    np.ndarray
    lon:    np.ndarray
    t_hour: np.ndarray
    val:    np.ndarray


def _gather_window_observations(target_utc: datetime,
                                w_t_hours: float = B1_W_TIME_H) -> _SlotObservations:
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
            r, c = np.where(valid)
            if r.size == 0:
                continue
            pool_lat.append(LATS[r])
            pool_lon.append(LONS[c])
            pool_t.append(np.full(r.size, dt_h, dtype=np.float64))
            pool_v.append(payload['aod'][r, c].astype(np.float64))

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
                  vgm: SumMetricVariogram,
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
    return float(np.clip(estimate, 0.0, 5.0)), max(variance, 0.0)


# ── Output file path + writer ────────────────────────────────────────────────

def kriged_path(slot_utc: datetime) -> Path:
    return (ST_KRIGING_DIR
            / f'{slot_utc.year:04d}'
            / f'{slot_utc.month:02d}'
            / f'{slot_utc.day:02d}'
            / f'aod_{slot_utc:%Y%m%d_%H%M}.nc')


def _write_slot(slot_utc: datetime,
                 aod: np.ndarray,
                 is_observed: np.ndarray,
                 uncertainty: np.ndarray,
                 weight_sum: np.ndarray,
                 training_window: str = '') -> Path:
    out = kriged_path(slot_utc)
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

def krige_slot(slot_utc: datetime, vgm: SumMetricVariogram,
                training_window: str = '') -> Optional[Path]:
    """Krige one 30-min slot and write the NC file.

    Per Yang & Hu (2018): cells where the spatiotemporal pool is empty or
    where the kriging solve fails are left as NaN (no climatology backstop).
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
        pool = _gather_window_observations(slot_utc, B1_W_TIME_H)
        if pool.val.size >= B1_MIN_NEIGHBOURS:
            tree = _build_pool_index(pool)
            for (r, c) in missing:
                est, var = _krige_target(float(LATS[r]), float(LONS[c]),
                                          pool, tree, vgm)
                if np.isfinite(est):
                    final[r, c]       = np.float32(est)
                    uncertainty[r, c] = np.float32(var)

    return _write_slot(slot_utc, final, is_observed, uncertainty, weight_sum,
                        training_window=training_window)


# ── End-to-end driver ───────────────────────────────────────────────────────

# Worker-side state: each worker process loads the variogram once from disk
# in its initializer, then re-uses it across all slots it processes.  This is
# more robust than pickling gstools CovModel instances across the pipe.
_WORKER_VGM: Optional[SumMetricVariogram] = None


def _init_worker():
    global _WORKER_VGM
    os.environ.setdefault('HDF5_USE_FILE_LOCKING', 'FALSE')
    _WORKER_VGM = load_variogram()


def _worker_krige_slot(args: tuple[datetime, str]) -> bool:
    slot_utc, training_window = args
    try:
        krige_slot(slot_utc, _WORKER_VGM, training_window=training_window)
        return True
    except Exception:
        return False


def run(start: date,
        end:   date,
        vgm:   Optional[SumMetricVariogram] = None,
        training_window: Optional[str] = None,
        overwrite: bool = False,
        refit_variogram: bool = False,
        progress: Optional[Callable] = None,
        n_workers: Optional[int] = None) -> int:
    """B1 over [start, end].  Optionally fits the variogram first.

    Writes one NC per slot under `ST_KRIGING_DIR/YYYY/MM/DD/aod_*.nc`.
    Cells the kriger cannot estimate are left as NaN (Yang & Hu 2018).

    Parallelism: by default uses `os.cpu_count() - 1` worker processes.  Pass
    `n_workers=1` for a serial (debuggable) run, or any positive int to cap.
    """
    if vgm is None:
        if _VGM_PATH.exists() and not refit_variogram:
            vgm = load_variogram()
        else:
            vgm = fit_and_save_variogram(progress=progress)
    # Workers load the variogram from disk; make sure it's there.
    if not _VGM_PATH.exists():
        save_variogram(vgm)

    if training_window is None:
        training_window = f'{TRAIN_START.isoformat()} to {TRAIN_END.isoformat()}'

    slots: list[datetime] = []
    for d in iter_local_days(start, end):
        for slot_utc in iter_window_slots(d):
            if kriged_path(slot_utc).exists() and not overwrite:
                continue
            slots.append(slot_utc)
    if not slots:
        return 0

    if n_workers is None:
        n_workers = max(1, (os.cpu_count() or 2) - 1)

    if n_workers <= 1:
        iterator = progress(slots, desc='B1 ST-krige') if progress is not None else slots
        n_written = 0
        for slot_utc in iterator:
            krige_slot(slot_utc, vgm, training_window=training_window)
            n_written += 1
        return n_written

    args_iter = [(s, training_window) for s in slots]
    n_written = 0
    with Pool(processes=n_workers, initializer=_init_worker) as pool:
        result_iter = pool.imap_unordered(_worker_krige_slot, args_iter, chunksize=1)
        if progress is not None:
            result_iter = progress(result_iter, total=len(slots), desc='B1 ST-krige')
        for ok in result_iter:
            if ok:
                n_written += 1
    return n_written
