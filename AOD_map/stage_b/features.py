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
                                      over (month, slot_idx).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Callable, Optional

import numpy as np
import pandas as pd

from config import (
    LATS, LONS, NLAT, NLON,
    RF_FEATURES,
    RF_TRAIN_TARGET_ROWS,
)
import ancillary as anc
from slots import (
    load_slot, iter_local_days, iter_window_slots, slot_index,
)


LAT_2D, LON_2D = np.meshgrid(LATS, LONS, indexing='ij')
LAT_RAD = np.deg2rad(LAT_2D).astype(np.float32)
LON_RAD = np.deg2rad(LON_2D).astype(np.float32)


def _nan_grid() -> np.ndarray:
    return np.full((NLAT, NLON), np.nan, dtype=np.float32)


# ── Per-slot feature grid ────────────────────────────────────────────────────

def build_feature_grid(slot_utc: datetime) -> dict[str, np.ndarray]:
    """Per-cell predictor stack for one 30-min slot."""
    grids: dict[str, np.ndarray] = {}

    cams = anc.cams_aod_slot(slot_utc)
    grids['cams_aod'] = cams if cams is not None else _nan_grid()

    era5 = anc.era5_slot(slot_utc)
    for key in ('t2m', 'dpt', 'rh', 'sp', 'u10', 'v10',
                'blh', 'tcc', 'tcwv', 'ssrd', 'fal'):
        grids[key] = era5.get(key, _nan_grid())

    imerg = anc.imerg_slot(slot_utc)
    grids['tp'] = imerg if imerg is not None else _nan_grid()

    day = slot_utc.date()
    grids['elevation']  = anc.dem_grid()
    lc   = anc.landcover_grid(day)
    grids['land_cover'] = lc if lc is not None else _nan_grid()
    grids['population'] = anc.landscan_grid(day.year)
    grids['lat_rad']    = LAT_RAD
    grids['lon_rad']    = LON_RAD
    ndvi = anc.ndvi_grid(day)
    grids['ndvi'] = ndvi if ndvi is not None else _nan_grid()

    for feat in RF_FEATURES:
        if feat not in grids:
            grids[feat] = _nan_grid()
        elif grids[feat].shape != (NLAT, NLON):
            raise ValueError(
                f'Feature {feat!r} has shape {grids[feat].shape}, '
                f'expected {(NLAT, NLON)}'
            )
    return grids


def stack_features(grids: dict[str, np.ndarray]) -> np.ndarray:
    cols = [grids[name].ravel().astype(np.float32) for name in RF_FEATURES]
    return np.column_stack(cols)


# ── Training-table builder (stratified by month × slot_idx) ─────────────────

# Per-slot contribution to a stratum: (date, slot_idx, rows_uint16, cols_uint16).
SlotEntry = tuple[date, int, np.ndarray, np.ndarray]


def _sample_stratum(
    items: list[SlotEntry],
    n_take: int,
    rng: np.random.Generator,
) -> dict[tuple[date, int], tuple[np.ndarray, np.ndarray]]:
    """Pick `n_take` cells uniformly at random from one stratum's slot list.

    `items` is the per-slot contributions for a single (month, slot_idx) stratum:
    each element is (date, slot_idx, rows_uint16, cols_uint16). Sampling is
    *across cells*, not slots — a slot with more valid pixels should contribute
    proportionally more rows on average, matching the original behaviour where
    every (cell, slot, day) tuple was an independent draw target.

    Return a dict keyed by (date, slot_idx) → (chosen_rows, chosen_cols) so the
    downstream build-features loop can index into each slot only once.

    Implementation guidance (5-10 lines):
      1. Build a per-slot count vector and its cumulative offsets.
      2. Draw `n_take` distinct global indices in [0, total) via
         `rng.choice(total, size=n_take, replace=False)`.
      3. Use `np.searchsorted` on the cumulative offsets to bucket each global
         index back to its owning slot, then subtract the slot's offset to get
         the local index inside that slot's rows/cols arrays.
      4. Group by slot and emit `{(d, s_idx): (rows[local_idx], cols[local_idx])}`.

    Edge case: if `n_take >= total`, return every cell of every slot — this is
    a no-op subsample but keeps the caller's logic uniform.
    """
    counts = np.fromiter((len(e[2]) for e in items), dtype=np.int64, count=len(items))
    cum = np.cumsum(counts)
    total = int(cum[-1]) if len(cum) else 0
    if n_take >= total:
        return {(d, s): (r, c) for d, s, r, c in items}

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
        out[(d, s)] = (r_arr[locs], c_arr[locs])
    return out


def build_training_table(
    start: date,
    end:   date,
    target_rows: int = RF_TRAIN_TARGET_ROWS,
    rng_seed:    int = 42,
    progress:    Optional[Callable] = None,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    """Walk every (cell, slot, day) inside each day's window where Stage A AOD is
    valid → stratified (X, y, meta).

    Stratification bins: (month × slot_idx).  Up to 12 × 48 = 576 strata in
    principle but only the observed ones get populated (slot_idx is bounded
    by Vietnam's actual daytime range, ~14-42).
    """
    rng = np.random.default_rng(rng_seed)

    days = list(iter_local_days(start, end))
    iterator = progress(days, desc='index slots') if progress is not None else days

    # Per-slot rows/cols stored as compact uint16 arrays (NLAT=310, NLON=160 fit
    # comfortably). One list entry per slot, NOT per cell — kills the ~20×
    # Python-tuple overhead that used to OOM this builder on long training windows.
    strata: dict[tuple[int, int], list[SlotEntry]] = {}
    for d in iterator:
        for slot_utc in iter_window_slots(d):
            payload = load_slot(slot_utc)
            if payload is None:
                continue
            valid = np.isfinite(payload['aod']) & (payload['aod'] >= 0)
            if not valid.any():
                continue
            rows, cols = np.where(valid)
            s_idx = slot_index(slot_utc)
            key = (d.month, s_idx)
            strata.setdefault(key, []).append(
                (d, s_idx, rows.astype(np.uint16), cols.astype(np.uint16))
            )

    stratum_counts = {k: sum(len(e[2]) for e in v) for k, v in strata.items()}
    total_available = sum(stratum_counts.values())
    if total_available == 0:
        return (pd.DataFrame(columns=RF_FEATURES),
                pd.Series(dtype=np.float32, name='aod_slot'),
                pd.DataFrame({'date': [], 'slot_idx': [], 'row': [], 'col': []}))

    by_slot: dict[tuple[date, int], tuple[np.ndarray, np.ndarray]] = {}
    if total_available <= target_rows:
        for items in strata.values():
            for d_e, s_e, r_arr, c_arr in items:
                by_slot[(d_e, s_e)] = (r_arr, c_arr)
    else:
        floor = max(50, target_rows // (len(strata) * 4))
        for key, items in strata.items():
            n_have = stratum_counts[key]
            n_take = min(n_have, max(floor, int(target_rows * n_have / total_available)))
            for slot_key, arrs in _sample_stratum(items, n_take, rng).items():
                by_slot[slot_key] = arrs

    del strata  # free the index before the heavy feature-build phase

    iterator2 = progress(list(by_slot.keys()), desc='build features') \
                if progress is not None else by_slot.keys()

    X_rows, y_vals = [], []
    meta = {'date': [], 'slot_idx': [], 'row': [], 'col': []}
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

    if not X_rows:
        return (pd.DataFrame(columns=RF_FEATURES),
                pd.Series(dtype=np.float32, name='aod_slot'),
                pd.DataFrame(meta))

    X = pd.DataFrame(np.concatenate(X_rows, axis=0), columns=RF_FEATURES)
    y = pd.Series(np.concatenate(y_vals, axis=0), name='aod_slot')
    return X, y, pd.DataFrame(meta)
