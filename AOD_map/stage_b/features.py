"""Feature-vector construction for Stage B per-slot RF (stage_b_fixes §7.8.1).

Public API
----------
- `build_feature_grid(slot_utc)`      per-slot (NLAT, NLON) grids keyed by
                                      `RF_FEATURES`.
- `stack_features(grids)`             flatten to (NCELL, NFEAT) — column order
                                      matches `RF_FEATURES`.
- `build_training_table(start, end)`  walk every valid (cell, slot, day) inside
                                      each day's discovered window and return
                                      (X, y, meta) with stratified subsampling
                                      over (month, slot_idx, aod_decile).
                                      The aod_decile axis (§7.8.1 fix-doc)
                                      stops high-AOD events from getting
                                      drowned out ~50:1 by low-AOD slots.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Callable, Optional

import numpy as np
import pandas as pd
from scipy.signal import convolve2d

from config import (
    LATS, LONS, NLAT, NLON,
    RF_FEATURES,
    RF_TRAIN_TARGET_ROWS,
    RF_AOD_DECILES,
    SLOT_MINUTES,
    TZ_OFFSET_HOURS,
    ANTECEDENT_RAIN_THRESHOLD_MM,
    ANTECEDENT_RAIN_LOOKBACK_H,
    CAMS_LAG_HOURS,
    OBS_NEIGHBORHOOD_BOX,
    PERSISTENCE_TAU_SLOTS,
    PERSISTENCE_LOOKBACK_SLOTS,
)
import ancillary as anc
from slots import (
    load_slot, iter_local_days, iter_window_slots, slot_index,
)


# ── Slot LRU ─────────────────────────────────────────────────────────────────
# slots.load_slot does the NC read on every call.  Backward-walking
# PERSISTENCE_LOOKBACK_SLOTS prior slots per build_feature_grid call would
# re-read the same files ~LOOKBACK times during build_training_table.  A small
# LRU here keeps prior-slot reads warm without touching slots.py.  Sized to
# cover ~1 daytime window (≈20 slots) × headroom for cross-slot reuse.

@lru_cache(maxsize=128)
def _load_slot_cached(slot_utc: datetime) -> Optional[tuple[np.ndarray, ...]]:
    payload = load_slot(slot_utc)
    if payload is None:
        return None
    # Return only what the ST-context helpers need; keep the tuple immutable so
    # the LRU stores a stable object (callers must not mutate the array).
    return (payload['aod'],)


def _nan_grid() -> np.ndarray:
    return np.full((NLAT, NLON), np.nan, dtype=np.float32)


def _scalar_grid(value: float) -> np.ndarray:
    return np.full((NLAT, NLON), np.float32(value), dtype=np.float32)


# Legacy lat/lon grids — pre-§7.8.1 bundles (rf_tuned.joblib) declare
# lat_rad / lon_rad in their feature_columns.  Compute them once at module
# load so backward-compat prediction still works without rebuilding the
# whole feature grid in a separate code path.
LAT_2D, LON_2D = np.meshgrid(LATS, LONS, indexing='ij')
_LEGACY_LAT_RAD = np.deg2rad(LAT_2D).astype(np.float32)
_LEGACY_LON_RAD = np.deg2rad(LON_2D).astype(np.float32)


# ── Per-slot feature grid ────────────────────────────────────────────────────

def _cyclical_temporal_grids(slot_utc: datetime) -> dict[str, np.ndarray]:
    """Cyclical hour-of-day (local) and month encoders.

    Hour is computed in LOCAL time (UTC + TZ_OFFSET_HOURS) because the diurnal
    cycle the RF needs to learn — sunrise, traffic peaks, boundary-layer
    growth — is anchored to local time, not UTC.
    """
    local_hour_frac = (slot_utc.hour + slot_utc.minute / 60.0 + TZ_OFFSET_HOURS) % 24.0
    hour_rad = 2.0 * np.pi * local_hour_frac / 24.0
    month_rad = 2.0 * np.pi * (slot_utc.month - 1) / 12.0
    return {
        'hour_sin':  _scalar_grid(np.sin(hour_rad)),
        'hour_cos':  _scalar_grid(np.cos(hour_rad)),
        'month_sin': _scalar_grid(np.sin(month_rad)),
        'month_cos': _scalar_grid(np.cos(month_rad)),
    }


# Pre-§7.8.1 features kept available for backward compat with rf_tuned.
# Gated on `columns` rather than built unconditionally — `tp` in particular
# costs a full IMERG read+reproject per slot that's thrown away when the
# production bundle (rf_tuned_residual) doesn't request it.
_LEGACY_FEATURES = frozenset({'lat_rad', 'lon_rad', 'tp'})


# ── ST-AOD-context helpers ───────────────────────────────────────────────────
# These three grids are how Stage B's RF gets to see the Stage A AOD field
# (which the kriging baseline uses directly).  At training time the centre
# cell of the 5x5 IS the target y, so the spatial mean must exclude it; the
# temporal walk is strictly prior so no future leakage either way.

def _obs_neighborhood_grids(
    aod_field: np.ndarray,
    box: int = OBS_NEIGHBORHOOD_BOX,
) -> tuple[np.ndarray, np.ndarray]:
    """Per-cell mean + count of valid AOD in a `box`×`box` window, EXCLUDING
    the centre cell.

    The mean is NaN where count == 0 (no valid neighbours in the window) so
    the RF median imputer fills it deliberately and `obs_aod_count_5x5` flags
    the same cells.

    Returns
    -------
    mean  : (NLAT, NLON) float32 — neighbour AOD mean; NaN if count == 0
    count : (NLAT, NLON) float32 — valid neighbour count, range 0..box²-1
    """
    valid = np.isfinite(aod_field) & (aod_field >= 0)
    aod_z = np.where(valid, aod_field, 0.0).astype(np.float64)
    kernel = np.ones((box, box), dtype=np.float64)
    kernel[box // 2, box // 2] = 0.0  # exclude centre — see module docstring
    sum_nb = convolve2d(aod_z, kernel, mode='same',
                        boundary='fill', fillvalue=0.0)
    cnt_nb = convolve2d(valid.astype(np.float64), kernel, mode='same',
                        boundary='fill', fillvalue=0.0)
    with np.errstate(invalid='ignore', divide='ignore'):
        mean = np.where(cnt_nb > 0, sum_nb / cnt_nb, np.nan)
    return mean.astype(np.float32), cnt_nb.astype(np.float32)


def _persistence_lookback_grids(
    slot_utc: datetime,
    lookback: int = PERSISTENCE_LOOKBACK_SLOTS,
) -> tuple[np.ndarray, np.ndarray]:
    """For every cell, the most recent STRICTLY PRIOR Stage A observation and
    how many 30-min slots back it was.

    The walk stops once every cell is filled or `lookback` slots have been
    searched.  Cells still NaN at the end had no observation in the window.

    Returns
    -------
    last_obs    : (NLAT, NLON) float32 — most-recent prior AOD; NaN if none
    slots_since : (NLAT, NLON) float32 — backward distance in slots; NaN if none
    """
    last_obs    = np.full((NLAT, NLON), np.nan, dtype=np.float32)
    slots_since = np.full((NLAT, NLON), np.nan, dtype=np.float32)
    found       = np.zeros((NLAT, NLON), dtype=bool)
    for k in range(1, lookback + 1):
        prior_utc = slot_utc - timedelta(minutes=SLOT_MINUTES * k)
        cached = _load_slot_cached(prior_utc)
        if cached is None:
            continue   # slot outside daytime window or NC missing
        aod = cached[0]
        fresh = np.isfinite(aod) & (aod >= 0) & ~found
        if not fresh.any():
            continue
        last_obs[fresh]    = aod[fresh].astype(np.float32)
        slots_since[fresh] = np.float32(k)
        found |= fresh
        if found.all():
            break
    return last_obs, slots_since


def _persistence_decay_grid(
    last_obs:    np.ndarray,
    slots_since: np.ndarray,
    tau:         float = PERSISTENCE_TAU_SLOTS,
) -> np.ndarray:
    """Recency-weighted persistence prior (option A from the design discussion):

        persistence_decay = last_obs * exp(-slots_since / tau)

    NaN inputs (no observation in the lookback window) propagate to NaN
    outputs — the RF's median imputer will fill them, and the co-NaN
    `slots_since_obs` tells the tree to distrust the imputed value.

    Tune `tau` via config.PERSISTENCE_TAU_SLOTS.  Smaller τ = forget faster;
    larger τ = trust older observations more.
    """
    return (last_obs * np.exp(-slots_since / np.float32(tau))).astype(np.float32)


def build_feature_grid(
    slot_utc: datetime,
    columns: Optional[list[str]] = None,
) -> dict[str, np.ndarray]:
    """Per-cell predictor stack for one 30-min slot.

    `columns` is the list of feature names that will be requested downstream
    (typically `bundle.feature_columns` at inference, or `RF_FEATURES` at
    training).  Legacy fields (`lat_rad`, `lon_rad`, `tp`) are filled only
    when `columns` actually asks for them, so production runs skip the
    redundant IMERG read.
    """
    if columns is None:
        columns = RF_FEATURES
    needed = set(columns)
    legacy_needed = _LEGACY_FEATURES & needed

    grids: dict[str, np.ndarray] = {}

    cams = anc.cams_aod_slot(slot_utc)
    grids['cams_aod'] = cams if cams is not None else _nan_grid()
    cams_lag = anc.cams_aod_slot_lagged(slot_utc, lag_hours=CAMS_LAG_HOURS)
    grids['cams_aod_lag_3h'] = cams_lag if cams_lag is not None else _nan_grid()

    era5 = anc.era5_slot(slot_utc)
    for key in ('t2m', 'dpt', 'rh', 'sp', 'u10', 'v10',
                'blh', 'tcc', 'tcwv', 'ssrd', 'fal'):
        grids[key] = era5.get(key, _nan_grid())

    # Antecedent precipitation replaces the single-slot `tp` feature.  The
    # rolling sums carry memory of rain that already washed out aerosols, and
    # `hours_since_rain` is the dry-spell clock — both correlate with PM
    # build-up far better than a 30-min snapshot.
    grids['precip_6h']  = anc.imerg_accum_hours(slot_utc, hours=6.0)
    grids['precip_24h'] = anc.imerg_accum_hours(slot_utc, hours=24.0)
    grids['hours_since_rain_0p1mm'] = anc.hours_since_rain(
        slot_utc,
        threshold_mm=ANTECEDENT_RAIN_THRESHOLD_MM,
        max_lookback_h=ANTECEDENT_RAIN_LOOKBACK_H,
    )

    grids.update(_cyclical_temporal_grids(slot_utc))

    # §8.2.4c follow-up: smoke proxy.  Skipped when the requested columns
    # don't include it (e.g. legacy bundles), so old joblibs don't pay the
    # FIRMS load cost.
    if 'fire_frp_log_24h' in needed:
        grids['fire_frp_log_24h'] = anc.firms_frp_grid(slot_utc)

    day = slot_utc.date()
    grids['elevation']  = anc.dem_grid()
    lc   = anc.landcover_grid(day)
    grids['land_cover'] = lc if lc is not None else _nan_grid()
    grids['population'] = anc.landscan_grid(day.year)
    # §8.2.4c follow-up: 3-level region.  Static, no slot dependence.
    if 'region_idx' in needed:
        grids['region_idx'] = anc.region_idx_grid()
    ndvi = anc.ndvi_grid(day)
    grids['ndvi'] = ndvi if ndvi is not None else _nan_grid()

    # ST-AOD-context features.  At training time the current cell IS observed,
    # but the spatial mean excludes the centre and the temporal walk is
    # strictly prior — neither path leaks the target y.  At inference on
    # AERONET-blind cells the current cell is unobserved, so the same code
    # path runs naturally without any branching.
    payload_now = _load_slot_cached(slot_utc)
    if payload_now is not None:
        mean_nb, cnt_nb = _obs_neighborhood_grids(payload_now[0])
    else:
        mean_nb, cnt_nb = _nan_grid(), _scalar_grid(0.0)
    grids['obs_aod_mean_5x5']  = mean_nb
    grids['obs_aod_count_5x5'] = cnt_nb

    last_obs, slots_since = _persistence_lookback_grids(slot_utc)
    grids['persistence_decay'] = _persistence_decay_grid(last_obs, slots_since)
    grids['slots_since_obs']   = slots_since

    if 'lat_rad' in legacy_needed:
        grids['lat_rad'] = _LEGACY_LAT_RAD
    if 'lon_rad' in legacy_needed:
        grids['lon_rad'] = _LEGACY_LON_RAD
    if 'tp' in legacy_needed:
        # Hit the IMERG LRU rather than the uncached public reader — the
        # 6 h / 24 h / hours_since_rain calls above just populated it for
        # this slot, so this is a cache hit.
        imerg = anc._imerg_slot_cached(slot_utc)
        grids['tp'] = imerg if imerg is not None else _nan_grid()

    for feat in columns:
        if feat not in grids:
            grids[feat] = _nan_grid()
        elif grids[feat].shape != (NLAT, NLON):
            raise ValueError(
                f'Feature {feat!r} has shape {grids[feat].shape}, '
                f'expected {(NLAT, NLON)}'
            )
    return grids


def stack_features(grids: dict[str, np.ndarray],
                   columns: Optional[list[str]] = None) -> np.ndarray:
    """Flatten requested feature grids to (NCELL, NFEAT).

    `columns` defaults to RF_FEATURES (current training-time order).  Pass
    `bundle.feature_columns` explicitly when predicting with a bundle
    that was trained on a different feature set (e.g. pre-§7.8.1 rf_tuned).
    """
    names = columns if columns is not None else RF_FEATURES
    cols = []
    for name in names:
        if name not in grids:
            raise KeyError(
                f'stack_features: requested feature {name!r} is not in the '
                f'feature grid (available: {sorted(grids)})'
            )
        cols.append(grids[name].ravel().astype(np.float32))
    return np.column_stack(cols)


# ── Training-table builder ──────────────────────────────────────────────────
# §7.8.1 fix-doc revision: stratification axis is now
# (month, slot_idx, aod_decile) instead of (month, slot_idx).  Decile edges
# are learned from a histogram pass over the training window — no hard-coded
# AOD thresholds, so the policy travels intact across seasons / sensors.

# Per-slot rows/cols stored as compact uint16 arrays.  Same layout as before,
# but a single slot may now contribute up to RF_AOD_DECILES entries (one per
# decile bin it has pixels in).
SlotEntry = tuple[date, int, np.ndarray, np.ndarray]

_HIST_BINS = 200  # AOD histogram bins for decile-edge estimation, [0, 5]
_HIST_RANGE = (0.0, 5.0)


def _aod_decile_edges(
    start: date, end: date,
    progress: Optional[Callable] = None,
) -> np.ndarray:
    """Walk the training window once, accumulate a coarse AOD histogram, then
    return decile edges from its cumulative distribution.

    Returns an array of length RF_AOD_DECILES - 1 — the interior edges that
    np.digitize uses to label every pixel with its decile in [0, n-1].
    """
    hist = np.zeros(_HIST_BINS, dtype=np.int64)
    days = list(iter_local_days(start, end))
    iterator = progress(days, desc='aod hist') if progress is not None else days
    for d in iterator:
        for slot_utc in iter_window_slots(d):
            payload = load_slot(slot_utc)
            if payload is None:
                continue
            aod = payload['aod']
            valid = np.isfinite(aod) & (aod >= 0)
            if not valid.any():
                continue
            h, _ = np.histogram(aod[valid], bins=_HIST_BINS, range=_HIST_RANGE)
            hist += h

    total = int(hist.sum())
    if total == 0:
        return np.linspace(0.05, 1.0, RF_AOD_DECILES - 1, dtype=np.float32)

    cum = np.cumsum(hist) / total
    bin_edges = np.linspace(_HIST_RANGE[0], _HIST_RANGE[1], _HIST_BINS + 1)
    bin_centres = 0.5 * (bin_edges[:-1] + bin_edges[1:])
    qs = np.linspace(0, 1, RF_AOD_DECILES + 1)[1:-1]  # 0.1 .. 0.9
    edges = np.interp(qs, cum, bin_centres).astype(np.float32)
    return edges


def _sample_stratum(
    items: list[SlotEntry],
    n_take: int,
    rng: np.random.Generator,
) -> dict[tuple[date, int], tuple[np.ndarray, np.ndarray]]:
    """Pick `n_take` cells uniformly at random from one stratum's slot list.

    `items` is the per-slot contributions for a single stratum: each element
    is (date, slot_idx, rows_uint16, cols_uint16). Sampling is *across cells*,
    not slots — a slot with more valid pixels in this stratum contributes
    proportionally more rows on average.

    Returns a dict keyed by (date, slot_idx) → (chosen_rows, chosen_cols).
    If a single (date, slot_idx) appears in multiple SlotEntries within
    `items` (different decile slices of the same slot), the caller merges
    them downstream — this function only reports per-entry picks.

    Edge case: if `n_take >= total`, return every cell of every slot.
    """
    counts = np.fromiter((len(e[2]) for e in items), dtype=np.int64, count=len(items))
    cum = np.cumsum(counts)
    total = int(cum[-1]) if len(cum) else 0
    if total == 0:
        return {}
    if n_take >= total:
        out: dict[tuple[date, int], tuple[np.ndarray, np.ndarray]] = {}
        for d, s, r, c in items:
            _accumulate_slot(out, d, s, r, c)
        return out

    picks = rng.choice(total, size=n_take, replace=False)
    slot_of_pick = np.searchsorted(cum, picks, side='right')
    offsets = np.concatenate(([0], cum[:-1]))
    local_of_pick = picks - offsets[slot_of_pick]

    order = np.argsort(slot_of_pick, kind='stable')
    slot_of_pick = slot_of_pick[order]
    local_of_pick = local_of_pick[order]
    split_at = np.searchsorted(slot_of_pick, np.arange(1, len(items)))
    locals_per_slot = np.split(local_of_pick, split_at)

    out: dict[tuple[date, int], tuple[np.ndarray, np.ndarray]] = {}
    for (d, s, r_arr, c_arr), locs in zip(items, locals_per_slot):
        if locs.size == 0:
            continue
        _accumulate_slot(out, d, s, r_arr[locs], c_arr[locs])
    return out


def _accumulate_slot(
    out: dict[tuple[date, int], tuple[np.ndarray, np.ndarray]],
    d: date, s: int, rows: np.ndarray, cols: np.ndarray,
) -> None:
    """Append picks for (d, s) to the running dict — merging with prior
    picks if another decile of this same slot already contributed."""
    key = (d, s)
    if key in out:
        prev_r, prev_c = out[key]
        out[key] = (np.concatenate([prev_r, rows]),
                    np.concatenate([prev_c, cols]))
    else:
        out[key] = (rows, cols)


def build_training_table(
    start: date,
    end:   date,
    target_rows: int = RF_TRAIN_TARGET_ROWS,
    rng_seed:    int = 42,
    progress:    Optional[Callable] = None,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    """Walk every (cell, slot, day) inside each day's window where Stage A AOD
    is valid → stratified (X, y, meta).

    Stratification bins: (month × slot_idx × aod_decile).  Decile edges are
    learned from a first-pass histogram of the training window's AOD values,
    so the top decile (~AOD > p90) gets the same per-stratum budget as the
    bottom decile rather than being out-voted ~50:1.

    Returns
    -------
    X    : DataFrame of feature columns (RF_FEATURES order)
    y    : Series of Stage A AOD at the sampled cells
    meta : DataFrame with date, slot_idx, row, col, aod_decile
    """
    rng = np.random.default_rng(rng_seed)

    decile_edges = _aod_decile_edges(start, end, progress=progress)

    days = list(iter_local_days(start, end))
    iterator = progress(days, desc='index slots') if progress is not None else days

    # Strata keyed by (month, slot_idx, decile).  One SlotEntry per (slot,
    # decile) pair so the stratum sampler can subsample independently.
    strata: dict[tuple[int, int, int], list[SlotEntry]] = {}
    for d in iterator:
        for slot_utc in iter_window_slots(d):
            payload = load_slot(slot_utc)
            if payload is None:
                continue
            aod = payload['aod']
            valid = np.isfinite(aod) & (aod >= 0)
            if not valid.any():
                continue
            rows, cols = np.where(valid)
            vals = aod[rows, cols]
            deciles = np.digitize(vals, decile_edges)  # 0..RF_AOD_DECILES-1
            s_idx = slot_index(slot_utc)
            for dec_idx in range(RF_AOD_DECILES):
                mask = deciles == dec_idx
                if not mask.any():
                    continue
                key = (d.month, s_idx, dec_idx)
                strata.setdefault(key, []).append(
                    (d, s_idx,
                     rows[mask].astype(np.uint16),
                     cols[mask].astype(np.uint16))
                )

    stratum_counts = {k: sum(len(e[2]) for e in v) for k, v in strata.items()}
    total_available = sum(stratum_counts.values())
    if total_available == 0:
        return (pd.DataFrame(columns=RF_FEATURES),
                pd.Series(dtype=np.float32, name='aod_slot'),
                pd.DataFrame({'date': [], 'slot_idx': [], 'row': [], 'col': [],
                              'aod_decile': []}))

    # Equal target per *stratum* — i.e. equal across deciles within a given
    # (month, slot_idx).  This is the §10 #21 mechanism for de-suppressing
    # the top AOD decile: it goes from ~10 % of the budget (proportional) to
    # ~10/n_strata of it (equal), with availability cap.
    n_strata = max(1, len(strata))
    per_stratum_target = max(50, target_rows // n_strata)

    by_slot: dict[tuple[date, int], tuple[np.ndarray, np.ndarray]] = {}
    decile_by_slot: dict[tuple[date, int], list[tuple[int, int]]] = {}
    for key, items in strata.items():
        n_have = stratum_counts[key]
        n_take = min(n_have, per_stratum_target)
        for slot_key, arrs in _sample_stratum(items, n_take, rng).items():
            r_new, c_new = arrs
            if slot_key in by_slot:
                prev_r, prev_c = by_slot[slot_key]
                by_slot[slot_key] = (np.concatenate([prev_r, r_new]),
                                     np.concatenate([prev_c, c_new]))
            else:
                by_slot[slot_key] = (r_new, c_new)
            decile_by_slot.setdefault(slot_key, []).append((key[2], r_new.size))

    del strata

    # Sort chronologically so the IMERG LRU (320 entries ≈ 160 h history) and
    # the CAMS LRU stay warm across consecutive slots.  by_slot is filled in
    # stratum-iteration order ((month, slot_idx, decile)) which is effectively
    # random across dates — without this sort, the 72 h hours_since_rain
    # back-walk re-reads every IMERG TIF for almost every slot.
    keys_sorted = sorted(by_slot.keys())
    iterator2 = progress(keys_sorted, desc='build features') \
                if progress is not None else keys_sorted

    X_rows, y_vals = [], []
    meta = {'date': [], 'slot_idx': [], 'row': [], 'col': [], 'aod_decile': []}
    for d, s_idx in iterator2:
        slot_utc = datetime(d.year, d.month, d.day,
                            (s_idx // 2), (s_idx % 2) * 30)
        payload = load_slot(slot_utc)
        if payload is None:
            continue
        grids = build_feature_grid(slot_utc)
        X_slot = stack_features(grids)
        rows_u16, cols_u16 = by_slot[(d, s_idx)]
        rows = rows_u16.astype(np.int32)
        cols = cols_u16.astype(np.int32)
        flat = rows * NLON + cols
        X_rows.append(X_slot[flat])
        y_vals.append(payload['aod'][rows, cols].astype(np.float32))
        meta['date'].extend([d] * len(rows))
        meta['slot_idx'].extend([s_idx] * len(rows))
        meta['row'].extend(rows.tolist())
        meta['col'].extend(cols.tolist())
        # Decile label per row — reconstruct from `decile_by_slot` (each entry
        # is (decile, count) in the order picks were merged).
        dec_labels = []
        for dec_idx, n in decile_by_slot[(d, s_idx)]:
            dec_labels.extend([dec_idx] * n)
        # Order in by_slot matches the merge order, so dec_labels lines up
        # with rows/cols 1:1.
        meta['aod_decile'].extend(dec_labels)

    if not X_rows:
        return (pd.DataFrame(columns=RF_FEATURES),
                pd.Series(dtype=np.float32, name='aod_slot'),
                pd.DataFrame(meta))

    X = pd.DataFrame(np.concatenate(X_rows, axis=0), columns=RF_FEATURES)
    y = pd.Series(np.concatenate(y_vals, axis=0), name='aod_slot')
    meta_df = pd.DataFrame(meta)

    # §8.2.4c audit — the 30-second check from the bias-decomposition story.
    # Equal-per-stratum sampling distorts the unconditional residual (because
    # high-AOD deciles are over-represented relative to their natural 0.1
    # prevalence).  This print makes the distortion legible without any
    # downstream re-analysis.
    print('  build_training_table audit:', flush=True)
    print(f'    n_rows                = {len(y):,}', flush=True)
    if 'cams_aod' in X.columns:
        cams = X['cams_aod'].to_numpy(dtype=np.float64)
        valid = np.isfinite(cams)
        resid = y.to_numpy(dtype=np.float64)[valid] - cams[valid]
        print(f'    mean(y_train)         = {float(np.nanmean(y)):+.5f}', flush=True)
        print(f'    mean(y_train - cams)  = {float(np.mean(resid)):+.5f}  '
              f'(unconditional residual prior — target ≈ 0)', flush=True)
        share = (meta_df['aod_decile'].value_counts(normalize=True)
                                       .sort_index().to_dict())
        share_str = ' '.join(f'd{k}:{v:.3f}' for k, v in share.items())
        print(f'    decile share          = {share_str}', flush=True)
        print(f'    decile share natural  ≈ 0.100 each (by decile-edge construction)',
              flush=True)
    return X, y, meta_df
