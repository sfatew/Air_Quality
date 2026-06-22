"""Stage A merged-slot reader + data-driven daytime-window discovery.

Window discovery follows stage_b_fixes §0:

  1. List Stage A's NC files for one UTC day.
  2. Parse HHMM from each filename → observed-slot set.
  3. If ≥ WINDOW_MIN_SLOTS_PRESENT: window = [min(HHMM), max(HHMM)].
  4. Else: 7-day median fallback over ±WINDOW_MEDIAN_HALF_DAYS.

All slot indexing uses **slot_idx ∈ [0, 47]** (every 30-min slot in a day),
not a hard-coded daytime range — Vietnam's daytime varies with latitude and
season and the early-draft 00:00–09:30 UTC cap was too narrow.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Iterator, Optional

import numpy as np
import netCDF4 as nc

from config import (
    NLAT, NLON, MERGED_DIR, SLOTS_PER_DAY,
    WINDOW_MEDIAN_HALF_DAYS, WINDOW_MIN_SLOTS_PRESENT,
)


_FNAME_RE = re.compile(r'merged_\d{8}_(\d{4})\.nc$')


def slot_idx_from_hhmm(hhmm: int) -> int:
    """Map HHMM int (e.g. 0730) → slot_idx in [0, 47]."""
    h, m = divmod(hhmm, 100)
    return h * 2 + (m // 30)


def hhmm_from_slot_idx(slot_idx: int) -> int:
    h, half = divmod(slot_idx, 2)
    return h * 100 + half * 30


def slot_utc_from_idx(day: date, slot_idx: int) -> datetime:
    hhmm = hhmm_from_slot_idx(slot_idx)
    h, m = divmod(hhmm, 100)
    return datetime(day.year, day.month, day.day, h, m)


def slot_path(slot_utc: datetime) -> Path:
    return (MERGED_DIR
            / f'{slot_utc.year:04d}'
            / f'{slot_utc.month:02d}'
            / f'{slot_utc.day:02d}'
            / f'merged_{slot_utc:%Y%m%d_%H%M}.nc')


# ── Window discovery ────────────────────────────────────────────────────────

@lru_cache(maxsize=4096)
def observed_slot_indices(day: date) -> tuple[int, ...]:
    """Sorted slot indices for which a Stage A file exists on `day`."""
    folder = MERGED_DIR / f'{day.year:04d}' / f'{day.month:02d}' / f'{day.day:02d}'
    if not folder.exists():
        return ()
    out: set[int] = set()
    for f in folder.iterdir():
        m = _FNAME_RE.search(f.name)
        if m:
            out.add(slot_idx_from_hhmm(int(m.group(1))))
    return tuple(sorted(out))


def _median_window_fallback(day: date) -> Optional[tuple[int, int]]:
    """7-day median of (slot_first, slot_last) over days within ±WINDOW_MEDIAN_HALF_DAYS
    that themselves have ≥ WINDOW_MIN_SLOTS_PRESENT observed slots.
    """
    firsts, lasts = [], []
    for k in range(-WINDOW_MEDIAN_HALF_DAYS, WINDOW_MEDIAN_HALF_DAYS + 1):
        if k == 0:
            continue
        d2 = day + timedelta(days=k)
        idxs = observed_slot_indices(d2)
        if len(idxs) >= WINDOW_MIN_SLOTS_PRESENT:
            firsts.append(idxs[0])
            lasts.append(idxs[-1])
    if not firsts:
        return None
    return int(np.median(firsts)), int(np.median(lasts))


def discover_day_window(day: date) -> Optional[tuple[int, int]]:
    """Return `(slot_first, slot_last)` in slot_idx for `day`, or None if undefined.

    Per fix doc §0:
      - ≥10 observed slots: use [min(HHMM), max(HHMM)].
      - <10 observed slots: 7-day median fallback.
      - Dataset edges: whichever side of the median window is available.
    """
    idxs = observed_slot_indices(day)
    if len(idxs) >= WINDOW_MIN_SLOTS_PRESENT:
        return idxs[0], idxs[-1]
    fb = _median_window_fallback(day)
    if fb is not None:
        # If the day has any observations, widen the median window to include them.
        if idxs:
            return min(fb[0], idxs[0]), max(fb[1], idxs[-1])
        return fb
    if idxs:
        # Single isolated observation, no neighbour days — just emit that slot.
        return idxs[0], idxs[-1]
    return None


def iter_window_slots(day: date) -> Iterator[datetime]:
    """Every slot_utc in the day's observation window (inclusive endpoints)."""
    window = discover_day_window(day)
    if window is None:
        return
    s_first, s_last = window
    for slot_idx in range(s_first, s_last + 1):
        yield slot_utc_from_idx(day, slot_idx)


def window_slot_indices(day: date) -> list[int]:
    window = discover_day_window(day)
    if window is None:
        return []
    return list(range(window[0], window[1] + 1))


# ── Slot reader ──────────────────────────────────────────────────────────────

def load_slot(slot_utc: datetime) -> Optional[dict]:
    """Read one Stage A merged slot.  Returns `aod`, `weight_sum`, `n_sensors`,
    `dominant_sensor` arrays — or None if the file isn't on disk.
    """
    path = slot_path(slot_utc)
    if not path.exists():
        return None
    try:
        with nc.Dataset(path) as ds:
            aod  = np.ma.filled(ds.variables['AOD_merged'][:].astype(np.float32), np.nan)
            ws   = (np.ma.filled(ds.variables['weight_sum'][:].astype(np.float32), 0.0)
                    if 'weight_sum' in ds.variables else None)
            nsen = np.ma.filled(ds.variables['n_sensors'][:].astype(np.int8), 0)
            dom  = (np.ma.filled(ds.variables['dominant_sensor'][:].astype(np.int8), 0)
                    if 'dominant_sensor' in ds.variables else np.zeros((NLAT, NLON), np.int8))
    except (OSError, KeyError):
        return None
    if ws is None:
        ws = nsen.astype(np.float32)
    return {'aod': aod, 'weight_sum': ws, 'n_sensors': nsen, 'dominant_sensor': dom}


def slot_index(slot_utc: datetime) -> int:
    """Slot index 0..47 for a slot timestamp (no daytime restriction)."""
    return slot_utc.hour * 2 + (slot_utc.minute // 30)


# ── Iteration helpers ───────────────────────────────────────────────────────

def iter_local_days(start: date, end: date) -> Iterator[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def iter_daytime_slots(start: date, end: date) -> Iterator[datetime]:
    """All daytime slots in [start, end] inclusive, using each day's window."""
    for d in iter_local_days(start, end):
        yield from iter_window_slots(d)
