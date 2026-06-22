"""Main Stage A pipeline driver (v3.4): A2-read → A4 → A5 → A3 per 30-min slot.

A1 (QA) + A2 (bin to 0.05°) are produced once by `run_collocate.py grid` and
persisted to GRIDDED_DIR — this driver reads that intermediate instead of
re-gridding from raw files.

For each calendar day the pipeline:
  1. Iterates over 48 UTC half-hour slots (00:00, 00:30, …, 23:30).
  2. Reads the pre-gridded per-sensor AOD from GRIDDED_DIR.
  3. Applies the §7.4.1 linear MERRA-2 soft calibration per (sensor, region,
     season) stratum.
  4. Merges Himawari L2 + L3 into a single Himawari channel using the stratum-
     aware per-pixel preference (§7.5: the level with the lower σ²_TC supplies
     the primary pixel value; the other fills its gaps).
  5. TC-weighted fusion (§7.5) of the soft-calibrated sensors into AOD_merged.
  6. Step A3 physics normalization on the fused field (§7.3, AFTER fusion).
  7. Writes one NetCDF per slot to MERGED_DIR / YYYY / MM / DD.

Prerequisites:
  * `run_collocate.py grid` for the same date range (GRIDDED_DIR populated).
  * `run_collocate.py soft_cal` → SOFT_CAL_FILE present (otherwise corrections
    are skipped and sensors pass through uncorrected — fusion still works).
  * `run_collocate.py tc_variance` → TC_VARIANCE_FILE present (otherwise every
    sensor's σ² falls back to the Sayer/Levy EE floor — fusion still works
    but weights become uniform).

Usage
-----
python run_stage_a.py --start 2022-09-01 --end 2022-09-30
python run_stage_a.py --start 2022-09-01 --end 2026-04-30 --workers 4
python run_stage_a.py --start 2023-01-01 --end 2023-01-07 --sensors himawari viirs
python run_stage_a.py --start 2023-06-01 --end 2023-06-01 --dry-run
python run_stage_a.py --start 2023-01-01 --end 2023-01-31 --no-physics
"""

from __future__ import annotations
import os
# Must be set before xarray / netCDF4 / HDF5 are imported anywhere — disables
# HDF5 read locks so concurrent open of the 44 ERA5 monthly files doesn't race.
os.environ.setdefault('HDF5_USE_FILE_LOCKING', 'FALSE')

import argparse
import sys
import time
import traceback
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional
import concurrent.futures
import multiprocessing as mp

import numpy as np

try:
    from tqdm import tqdm as _tqdm
    _HAS_TQDM = True
except ImportError:
    _tqdm = None
    _HAS_TQDM = False

from config import (
    LATS, LONS, NLAT, NLON,
    MERGED_DIR,
    SLOT_MINUTES,
    DRY_MONTHS,
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
)
from grid            import read_gridded_slot
from physics         import apply_physics_correction, close_era5
from bias_correction import apply_soft_calibration_grid, load_soft_calibrations
from fusion          import fuse, load_tc_variance, load_routes, ee_floor_sigma2


_ALL_SENSOR_GROUPS = ('himawari', 'viirs', 'modis')
# Each Himawari level is soft-calibrated independently against MERRA-2; the
# corrected per-level grids are then merged into one 'himawari' grid using the
# σ²_TC table (the level with the lower σ²_TC per (region, season) is the
# per-pixel primary; the other fills its gaps).  Fusion sees only the merged
# 'himawari' grid (§7.5 'Himawari L2/L3 stratum-aware per-pixel merge').
_SENSOR_KEYS = {
    'himawari': ['himawari_l2', 'himawari_l3'],
    'viirs':    ['viirs_snpp', 'viirs_noaa20'],
    'modis':    ['modis_maiac'],
}

SLOTS_PER_DAY = 48


def _himawari_prefer_l2_mask(
    lat_2d: np.ndarray,
    month: int,
    sigma2_table: dict,
) -> np.ndarray:
    """Per-pixel boolean mask: True where L2's σ²_TC beats L3's for (region, season).

    §7.5: for each stratum the level with the lower σ²_TC supplies the primary
    pixel value; the other fills its gaps.  Ties and missing entries default to
    False (L3-first); when only one level has a TC entry the mask is irrelevant
    because the fallback path is the only available source.
    """
    season = 'dry' if month in DRY_MONTHS else 'wet'
    region_code = np.where(
        lat_2d >= NORTH_CENTRAL_LAT, 2,
        np.where(lat_2d < CENTRAL_SOUTH_LAT, 0, 1),
    )
    mask = np.zeros(lat_2d.shape, dtype=bool)
    for code, reg in ((0, 'south'), (1, 'central'), (2, 'north')):
        l2 = sigma2_table.get(('himawari_l2', reg, season))
        l3 = sigma2_table.get(('himawari_l3', reg, season))
        if l2 is not None and l3 is not None and l2 < l3:
            mask[region_code == code] = True
    return mask


# ── Environment banner ────────────────────────────────────────────────────────

def _print_env_banner() -> None:
    W = 64
    print("=" * W)
    print("Stage A Pipeline (v3.4) — Runtime Environment")
    print("-" * W)
    print(f"  {'Python':<12} {sys.version.split()[0]}")
    for mod_name, label in [
        ("numpy",    "NumPy"),
        ("scipy",    "SciPy"),
        ("xarray",   "xarray"),
        ("netCDF4",  "netCDF4"),
        ("rasterio", "rasterio"),
        ("tqdm",     "tqdm"),
    ]:
        try:
            mod = __import__(mod_name)
            print(f"  {label:<12} {getattr(mod, '__version__', '?')}")
        except ImportError:
            print(f"  {label:<12} not installed")
    print("=" * W)


def _fmt_duration(seconds: float) -> str:
    if seconds >= 60:
        m = int(seconds // 60)
        return f"{m}m {seconds - m * 60:.1f}s"
    return f"{seconds:.1f}s"


# ── NetCDF writer ─────────────────────────────────────────────────────────────

def _write_netcdf(
    out_path: Path,
    merged: dict[str, np.ndarray],
    sensor_grids_corrected: dict[str, np.ndarray],
    slot_utc: datetime,
    physics_fields: Optional[dict[str, np.ndarray]] = None,
) -> None:
    """Write one 30-min merged AOD slot to NetCDF."""
    import netCDF4 as nc

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with nc.Dataset(str(out_path), 'w', format='NETCDF4') as ds:
        ds.title       = 'Vietnam merged AOD — Stage A v3.4 30-min product'
        ds.institution = 'Hanoi University of Science and Technology'
        ds.slot_utc    = slot_utc.isoformat()
        ds.Conventions = 'CF-1.8'

        ds.createDimension('lat', NLAT)
        ds.createDimension('lon', NLON)

        vl = ds.createVariable('lat', 'f4', ('lat',))
        vl.units = 'degrees_north'; vl.long_name = 'Latitude (cell center)'
        vl[:] = LATS.astype(np.float32)

        vlo = ds.createVariable('lon', 'f4', ('lon',))
        vlo.units = 'degrees_east'; vlo.long_name = 'Longitude (cell center)'
        vlo[:] = LONS.astype(np.float32)

        def _add_var(name, data, long_name, units='1', dtype='f4', fill=-9999.0):
            v = ds.createVariable(name, dtype, ('lat', 'lon'),
                                  fill_value=fill, zlib=True, complevel=4)
            v.long_name = long_name
            v.units     = units
            arr = np.where(np.isnan(data), fill, data) if dtype == 'f4' else data
            v[:] = arr

        _add_var('AOD_merged',      merged['aod_merged'],
                 'TC-weighted merged AOD at 550 nm (MERRA-2-soft-calibrated)')
        _add_var('AOD_std',         merged['aod_std'],
                 'Cross-sensor AOD spread (weighted std dev)')
        _add_var('weight_sum',      merged['weight_sum'],
                 'Sum of TC weights (Σ 1/σ²_TC); provenance for Stage B aggregation')
        _add_var('n_sensors',       merged['n_sensors'],
                 'Number of sensors contributing', dtype='i1', fill=-1)
        _add_var('dominant_sensor', merged['dominant_sensor'],
                 'Sensor with highest TC weight (1=Himawari merged,3=MAIAC,4=SNPP,5=N20)',
                 dtype='i1', fill=0)
        _add_var('confidence_flag', merged['confidence_flag'],
                 'Confidence: 0=none,1=H-only,2=LEO-only,3=H+LEO,4=multi-LEO+H',
                 dtype='i1', fill=0)

        # Step A3 physics fields + physics-corrected AOD.
        if physics_fields is not None:
            if physics_fields.get('aod_phys') is not None:
                _add_var('AOD_phys_corrected', physics_fields['aod_phys'],
                         'Physics-normalized AOD: AOD_merged × (1−RH/100)^0.6 / PBLH '
                         '(PM2.5-coupling proxy; downstream-only)',
                         units='m-1')
            if physics_fields.get('RH') is not None:
                _add_var('ERA5_RH',   physics_fields['RH'],
                         'ERA5 2-m relative humidity (bilinear interp to 0.05°)', units='%')
            if physics_fields.get('PBLH') is not None:
                _add_var('ERA5_PBLH', physics_fields['PBLH'],
                         'ERA5 planetary boundary layer height (bilinear interp to 0.05°)',
                         units='m')

        for sensor, grid in sensor_grids_corrected.items():
            if grid is not None and np.any(np.isfinite(grid)):
                _add_var(f'AOD_{sensor}', grid,
                         f'Soft-calibrated AOD from {sensor}')


# ── Per-slot processing ───────────────────────────────────────────────────────

def _process_slot(
    slot_utc: datetime,
    soft_cals: dict,
    sigma2_table: dict,
    routes: dict,
    lat_2d: np.ndarray,
    sensor_groups: list[str],
    use_physics: bool,
    dry_run: bool,
) -> Optional[Path]:
    """Run A2-read → A4 (soft-cal) → A5 (TC fuse) → A3 (physics) for one slot."""
    month = slot_utc.month

    wanted_keys = tuple(k for grp in sensor_groups for k in _SENSOR_KEYS.get(grp, []))
    grids = read_gridded_slot(slot_utc, sensors=wanted_keys)
    if grids is None:
        return None

    himawari_l2_raw = grids.get('himawari_l2', {}).get('aod_mean')
    himawari_l3_raw = grids.get('himawari_l3', {}).get('aod_mean')
    raw_grids: dict[str, Optional[np.ndarray]] = {}
    for sensor in ('viirs_snpp', 'viirs_noaa20', 'modis_maiac'):
        g = grids.get(sensor)
        raw_grids[sensor] = g['aod_mean'] if g is not None else None

    if dry_run:
        return Path('dry_run')

    # ── Step A4: per-level Himawari soft calibration, then per-pixel merge ──
    corrected: dict[str, Optional[np.ndarray]] = {}

    himawari_l2_corrected = (
        apply_soft_calibration_grid(himawari_l2_raw, 'himawari_l2', month, soft_cals)
        if himawari_l2_raw is not None else None
    )
    himawari_l3_corrected = (
        apply_soft_calibration_grid(himawari_l3_raw, 'himawari_l3', month, soft_cals)
        if himawari_l3_raw is not None else None
    )

    # Per-pixel L2/L3 merge governed by σ²_TC per (region, season) — §7.5.
    if himawari_l2_corrected is not None and himawari_l3_corrected is not None:
        prefer_l2 = _himawari_prefer_l2_mask(lat_2d, month, sigma2_table)
        primary  = np.where(prefer_l2, himawari_l2_corrected, himawari_l3_corrected)
        fallback = np.where(prefer_l2, himawari_l3_corrected, himawari_l2_corrected)
        himawari_corrected = np.where(
            np.isfinite(primary), primary, fallback
        ).astype(np.float32)
    elif himawari_l2_corrected is not None:
        himawari_corrected = himawari_l2_corrected
    elif himawari_l3_corrected is not None:
        himawari_corrected = himawari_l3_corrected
    else:
        himawari_corrected = None

    corrected['himawari'] = himawari_corrected

    # LEO soft calibration.
    for sensor, aod in raw_grids.items():
        corrected[sensor] = (
            apply_soft_calibration_grid(aod, sensor, month, soft_cals)
            if aod is not None else None
        )

    # ── Step A5: TC-weighted fusion ────────────────────────────────────────
    valid_corrected = {k: v for k, v in corrected.items() if v is not None}
    merged = fuse(valid_corrected, month, lat_2d, sigma2_table, routes)

    # ── Step A3: physics normalization — SEPARATE output, not fed back ─────
    physics_fields: Optional[dict] = None
    if use_physics:
        aod_phys, rh_grid, pblh_grid = apply_physics_correction(
            merged['aod_merged'], slot_utc
        )
        if np.any(np.isfinite(rh_grid)):
            physics_fields = {
                'RH':       rh_grid,
                'PBLH':     pblh_grid,
                'aod_phys': aod_phys,
            }

    out_path = (MERGED_DIR
                / slot_utc.strftime('%Y') / slot_utc.strftime('%m')
                / slot_utc.strftime('%d')
                / f'merged_{slot_utc.strftime("%Y%m%d_%H%M")}.nc')

    _write_netcdf(out_path, merged, corrected, slot_utc, physics_fields)
    return out_path


def _make_lat_lon_grids() -> tuple[np.ndarray, np.ndarray]:
    return np.meshgrid(LATS, LONS, indexing='ij')


# ── Per-day driver ────────────────────────────────────────────────────────────

def run_day(
    day: date,
    sensor_groups: list[str],
    soft_cals: dict,
    sigma2_table: dict,
    routes: dict,
    lat_2d: np.ndarray,
    use_physics: bool = True,
    dry_run: bool = False,
    show_slot_bar: bool = False,
) -> tuple[int, int]:
    """Process all 48 slots for one calendar day.  Returns (n_written, n_errors)."""
    written = 0
    errors  = 0
    skipped = 0

    slot_range = range(SLOTS_PER_DAY)
    if show_slot_bar and _HAS_TQDM:
        slot_iter = _tqdm(
            slot_range, desc=f"  {day}", leave=False, unit="slot", ncols=80,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}  [{elapsed}<{remaining}]  {postfix}",
        )
    else:
        slot_iter = slot_range

    for slot_idx in slot_iter:
        slot_utc = (datetime(day.year, day.month, day.day)
                    + timedelta(minutes=slot_idx * SLOT_MINUTES))
        try:
            out = _process_slot(
                slot_utc, soft_cals, sigma2_table, routes, lat_2d,
                sensor_groups, use_physics, dry_run,
            )
            if out is not None:
                written += 1
            else:
                skipped += 1
        except Exception as exc:
            errors += 1
            msg = f"    [ERROR] {slot_utc.strftime('%H:%M')} UTC: {exc}"
            if show_slot_bar and _HAS_TQDM:
                slot_iter.write(msg)   # type: ignore[union-attr]
            else:
                print(msg)

        if show_slot_bar and _HAS_TQDM:
            slot_iter.set_postfix(ok=written, skip=skipped, err=errors)  # type: ignore[union-attr]

    return written, errors


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description='Run Stage A pipeline (v3.4)')
    p.add_argument('--start',      required=True, help='Start date YYYY-MM-DD')
    p.add_argument('--end',        required=True, help='End date YYYY-MM-DD')
    p.add_argument('--sensors',    nargs='+',     default=list(_ALL_SENSOR_GROUPS),
                   choices=_ALL_SENSOR_GROUPS,
                   help='Sensor groups to include (default: all)')
    p.add_argument('--workers',    type=int,      default=1,
                   help='Parallel worker processes (default: 1)')
    p.add_argument('--no-physics', action='store_true',
                   help='Skip Step A3 physics normalization (ERA5 RH/PBLH)')
    p.add_argument('--dry-run',    action='store_true',
                   help='Parse inputs only; do not write output files')
    return p.parse_args()


def main():
    args    = parse_args()
    start_d = date.fromisoformat(args.start)
    end_d   = date.fromisoformat(args.end)
    sensors = args.sensors

    use_physics = not args.no_physics

    _print_env_banner()

    print(f'Stage A pipeline : {start_d} → {end_d}')
    print(f'Sensors          : {sensors}')
    print(f'Physics (A3)     : {"enabled (ERA5 RH/PBLH)" if use_physics else "disabled (--no-physics)"}')
    print(f'Workers          : {args.workers}')
    print(f'Dry-run          : {args.dry_run}')
    print(f'Output           : {MERGED_DIR}')

    # Load v3.4 calibration tables.
    soft_cals    = load_soft_calibrations()
    sigma2_table = load_tc_variance()
    routes       = load_routes()
    lat_2d, _    = _make_lat_lon_grids()

    n_strata_sc = sum(len(v) for v in soft_cals.values())
    n_strata_tc = len(sigma2_table)
    print(f'Soft cal strata  : {n_strata_sc}'
          + ('' if n_strata_sc else '  (none — sensors will pass through uncorrected)'))
    print(f'TC σ² strata     : {n_strata_tc}'
          + ('' if n_strata_tc else f'  (none — falling back to EE floor σ²={ee_floor_sigma2():.4f})'))

    days: list[date] = []
    d = start_d
    while d <= end_d:
        days.append(d)
        d += timedelta(days=1)

    n_days        = len(days)
    total_slots   = n_days * SLOTS_PER_DAY
    print(f'\nProcessing {n_days} day(s) × {SLOTS_PER_DAY} slots = {total_slots} slots total')
    if args.dry_run:
        print('[DRY RUN — no files will be written]')

    total_written = 0
    total_errors  = 0
    wall_t0 = time.perf_counter()

    try:
        if args.workers > 1:
            _day_bar = (_tqdm(total=n_days, desc='Days', unit='day', ncols=80)
                        if _HAS_TQDM else None)
            try:
                _mp_ctx = mp.get_context('spawn')
                with concurrent.futures.ProcessPoolExecutor(
                    max_workers=args.workers, mp_context=_mp_ctx
                ) as ex:
                    futures = {
                        ex.submit(
                            run_day, day, sensors, soft_cals, sigma2_table, routes,
                            lat_2d, use_physics, args.dry_run,
                            False,   # show_slot_bar=False in subprocesses
                        ): day
                        for day in days
                    }
                    for fut in concurrent.futures.as_completed(futures):
                        day = futures[fut]
                        try:
                            n_ok, n_err = fut.result()
                            total_written += n_ok
                            total_errors  += n_err
                            msg = (f'  {day}: {n_ok}/{SLOTS_PER_DAY} slots written'
                                   + (f', {n_err} error(s)' if n_err else ''))
                        except Exception:
                            msg = f'  {day}: FAILED\n{traceback.format_exc()}'
                        if _day_bar is not None:
                            _day_bar.write(msg)
                            _day_bar.update(1)
                        else:
                            print(msg)
            finally:
                if _day_bar is not None:
                    _day_bar.close()
        else:
            _day_bar = (_tqdm(days, desc='Days', unit='day', ncols=80)
                        if _HAS_TQDM else None)
            day_iter = _day_bar if _day_bar is not None else days

            for day in day_iter:
                t0 = time.perf_counter()
                n_ok, n_err = run_day(
                    day, sensors, soft_cals, sigma2_table, routes, lat_2d,
                    use_physics, args.dry_run,
                    show_slot_bar=True,
                )
                elapsed = time.perf_counter() - t0
                total_written += n_ok
                total_errors  += n_err

                summary = (f'  {day}: {n_ok}/{SLOTS_PER_DAY} slots written'
                           + (f', {n_err} error(s)' if n_err else '')
                           + f'  [{_fmt_duration(elapsed)}]')
                if _day_bar is not None:
                    _day_bar.write(summary)
                else:
                    print(summary)

    finally:
        close_era5()

    wall_elapsed = time.perf_counter() - wall_t0
    print(f'\n{"─" * 64}')
    print(f'Done.  {total_written}/{total_slots} slots written across {n_days} day(s)'
          + (f', {total_errors} error(s)' if total_errors else '')
          + f'  [total: {_fmt_duration(wall_elapsed)}]')


if __name__ == '__main__':
    main()
