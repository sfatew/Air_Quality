"""Extract slot-cadence gridded satellite AOD at AERONET station coordinates.

Train-apply consistent design: training values come from the same Stage A2
gridded intermediate that production consumes.  Per slot, ``grid.read_gridded_slot``
returns the raw per-sensor 0.05° grids, and each sensor is sampled at the
station's grid cell with a tiered neighbourhood fallback:

    exact cell (1×1)  →  3×3 cells (≈ 15 km)  →  5×5 cells (≈ 25 km)

The bias-correction CDF is therefore trained on the same spatial
representation it is applied to in production (0.05° cell scale).  QA stays
at native pixel level inside the read functions called by Stage A2.

Prerequisite: ``run_collocate.py grid --start … --end …`` must have run
for the same date range, populating ``GRIDDED_DIR``.

Output CSV schema (one row per (sensor, slot) with a valid station sample):
    timestamp_sat  — UTC ISO-8601 slot timestamp
    sat_aod        — neighbourhood-mean of gridded AOD at the station cell
    box_std        — within-neighbourhood AOD std (grid-cell heterogeneity)
    spatial_flag   — 0 = exact cell, 1 = 3×3 cells, 2 = 5×5 cells
    dist_km        — 0.0 (kept for schema compatibility)
    vza            — mean VZA in the sampled neighbourhood (NaN where N/A)
    sza            — mean SZA in the sampled neighbourhood (NaN where N/A)
    is_fallback    — always False (MODIS no-timestamp days are skipped at Stage A2)
"""

from __future__ import annotations
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

try:
    from tqdm import tqdm as _tqdm_cls
    _HAS_TQDM = True
except ImportError:
    _tqdm_cls = None  # type: ignore[assignment]
    _HAS_TQDM = False


def _make_bar(iterable, **kwargs):
    if _HAS_TQDM and _tqdm_cls is not None:
        return _tqdm_cls(iterable, **kwargs)
    return None


from config import (
    AERONET_SITES,
    EXTRACT_DIR,
    NLAT, NLON, SLOT_MINUTES,
    COLLOCATE_SPATIAL_FLAG_EXACT, COLLOCATE_SPATIAL_FLAG_3X3, COLLOCATE_SPATIAL_FLAG_5X5,
    BOX_STD_ABS_FLOOR, BOX_STD_SLOPE,
)
from gridder import station_cell
from grid    import read_gridded_slot, ALL_SENSORS


_SLOTS_PER_DAY = (24 * 60) // SLOT_MINUTES


# ── Grid-cell neighbourhood sampling ─────────────────────────────────────────

_CELL_TIERS = (
    (0, COLLOCATE_SPATIAL_FLAG_EXACT),   # 1×1 cell
    (1, COLLOCATE_SPATIAL_FLAG_3X3),     # 3×3 cells (≈ 15 km)
    (2, COLLOCATE_SPATIAL_FLAG_5X5),     # 5×5 cells (≈ 25 km)
)


def _box_std_max(mean_aod: float, sensor: str) -> float:
    return max(BOX_STD_ABS_FLOOR, BOX_STD_SLOPE.get(sensor, 0.15) * mean_aod)


def _sample_cell(
    grid_2d: np.ndarray,
    row: int,
    col: int,
    sensor: str,
) -> 'tuple[float, float, int, int] | None':
    """Sample the (NLAT, NLON) config grid at (row, col) with tiered fallback.

    Returns ``(mean, std, n_valid_cells, spatial_flag)`` for the first tier
    whose neighbourhood passes the heterogeneity gate, or ``None`` when every
    tier is either empty or fails the std threshold.
    """
    for hw, flag in _CELL_TIERS:
        r0 = max(0, row - hw); r1 = min(NLAT, row + hw + 1)
        c0 = max(0, col - hw); c1 = min(NLON, col + hw + 1)
        vals = grid_2d[r0:r1, c0:c1].ravel()
        vals = vals[np.isfinite(vals) & (vals >= 0) & (vals <= 5.0)]
        if len(vals) == 0:
            continue
        mean_v = float(np.mean(vals))
        std_v  = float(np.std(vals, ddof=0))
        if std_v > _box_std_max(mean_v, sensor):
            return None
        return mean_v, std_v, int(len(vals)), flag
    return None


def _mean_in_cell(
    grid_2d: Optional[np.ndarray],
    row: int,
    col: int,
    spatial_flag: int,
) -> float:
    """Mean of an auxiliary 2-D grid (vza/sza) over the same neighbourhood."""
    if grid_2d is None:
        return float('nan')
    hw = (0, 1, 2)[spatial_flag]
    r0 = max(0, row - hw); r1 = min(NLAT, row + hw + 1)
    c0 = max(0, col - hw); c1 = min(NLON, col + hw + 1)
    vals = grid_2d[r0:r1, c0:c1]
    vals = vals[np.isfinite(vals)]
    return float(vals.mean()) if len(vals) else float('nan')


def _make_record(
    slot_utc: datetime,
    g: dict[str, np.ndarray],
    sensor: str,
    row: int,
    col: int,
) -> Optional[dict]:
    sample = _sample_cell(g['aod_mean'], row, col, sensor=sensor)
    if sample is None:
        return None
    mean_v, std_v, _n, flag = sample
    return {
        'timestamp_sat': slot_utc.isoformat(),
        'sat_aod':       mean_v,
        'box_std':       std_v,
        'spatial_flag':  flag,
        'dist_km':       0.0,
        'vza':           _mean_in_cell(g.get('vza_mean'), row, col, flag),
        'sza':           _mean_in_cell(g.get('sza_mean'), row, col, flag),
        'is_fallback':   False,
    }


# ── Date / slot iteration ────────────────────────────────────────────────────

def _date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _slot_iter(day: date):
    base = datetime(day.year, day.month, day.day)
    for i in range(_SLOTS_PER_DAY):
        yield base + timedelta(minutes=i * SLOT_MINUTES)


# ── Main extraction entry point ──────────────────────────────────────────────

def extract_site(
    site: str,
    start_date: date,
    end_date: date,
    sensors: tuple[str, ...] = ALL_SENSORS,
    output_dir: Path | str = EXTRACT_DIR,
) -> dict[str, pd.DataFrame]:
    """Extract slot-cadence gridded satellite AOD at one AERONET station.

    Reads each slot's gridded NetCDF from ``GRIDDED_DIR`` (produced by
    ``run_collocate.py grid``) and samples the station's 0.05° cell with the
    tiered cell-neighbourhood fallback.  Writes one CSV per sensor at
    ``output_dir/{sensor}_{site}_raw.csv``.

    Re-runs on overlapping date ranges are safe: new rows are appended and
    de-duplicated on ``timestamp_sat``.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    meta        = AERONET_SITES[site]
    station_lat = meta['lat']
    station_lon = meta['lon']
    row, col, in_domain = station_cell(station_lat, station_lon)
    if not in_domain:
        raise ValueError(
            f'Site {site} at ({station_lat}, {station_lon}) is outside the '
            f'config grid domain.'
        )

    records_by_sensor: dict[str, list[dict]] = {s: [] for s in sensors}

    days = list(_date_range(start_date, end_date))
    day_bar = _make_bar(days, desc=f'  {site} extract', unit='day',
                        ncols=72, leave=False)
    for d in (day_bar if day_bar is not None else days):
        for slot in _slot_iter(d):
            grids = read_gridded_slot(slot, sensors=tuple(sensors))
            if grids is None:
                continue
            for sensor in sensors:
                g = grids.get(sensor)
                if g is None:
                    continue
                rec = _make_record(slot, g, sensor, row, col)
                if rec is not None:
                    records_by_sensor[sensor].append(rec)
    if day_bar is not None:
        day_bar.close()

    results: dict[str, pd.DataFrame] = {}
    for sensor, records in records_by_sensor.items():
        if not records:
            continue
        df = pd.DataFrame(records)
        csv_path = output_dir / f'{sensor}_{site}_raw.csv'
        if csv_path.exists():
            existing = pd.read_csv(csv_path)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=['timestamp_sat'], keep='last')
        df.to_csv(csv_path, index=False)
        results[sensor] = df
        print(f'  [{site}] {sensor}: {len(records)} snapshots  (total {len(df)})')

    return results
