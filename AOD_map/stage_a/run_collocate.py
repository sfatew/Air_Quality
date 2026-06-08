"""CLI driver: produce Stage A2 gridded slots, extract pairs, train corrections.

Stage A2 (`grid`) writes one NetCDF per 30-min slot containing the raw
0.05° gridded AOD for every sensor, before any bias correction.  Both
`extract_satellite.py` (training pair extraction) and `run_stage_a.py`
(production) consume this intermediate instead of re-gridding from raw
files, so binning runs exactly once per slot per dataset.

Usage
-----
# Stage A2 — grid raw satellite slots (required before extract / run_stage_a)
python run_collocate.py grid --start 2022-09-01 --end 2024-12-31
python run_collocate.py grid --start 2022-09-01 --end 2024-12-31 --workers 4

# Extract satellite time series at stations from the gridded slots
python run_collocate.py extract --start 2022-09-01 --end 2024-12-31

# Match existing raw CSVs with AERONET (no satellite file I/O)
python run_collocate.py match

# Extract + match (equivalent to old 'collocate' command)
python run_collocate.py collocate --start 2022-09-01 --end 2024-12-31

# Train CDF corrections from existing matched CSVs
python run_collocate.py train

# Build the LEO–Himawari spatial offset map (thesis §7.4.2)
# Requires a prior Stage A run for [start, end] with Bug 1 fixed.
python run_collocate.py leo_offset --start 2022-09-01 --end 2024-12-31

# Full pipeline: grid + extract + match + train
python run_collocate.py all --start 2022-09-01 --end 2024-12-31

# Single site (for debugging)
python run_collocate.py collocate --site NGHIA_DO --start 2023-01-01 --end 2023-03-31
"""

from __future__ import annotations
import argparse
import concurrent.futures
import multiprocessing as mp
import sys
import time
import traceback
from datetime import date, timedelta
from pathlib import Path

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


import numpy as np

from config import (
    AERONET_SITES, EXTRACT_DIR, COLLOCATE_DIR, BIASC_DIR, GRIDDED_DIR,
    LEO_HIMAWARI_OFFSET_FILE, LEO_HIMAWARI_MIN_PAIRS, LEO_HIMAWARI_SMOOTH_SIGMA,
    SENSOR_RMSE_EE_OFFSET, SENSOR_RMSE_EE_SLOPE, SENSOR_RMSE_EE_REFAOD,
)
from extract_satellite import extract_site
from collocate import match_site, collocate_site
from bias_correction import train_all_corrections, build_leo_himawari_offset
from fusion import save_rmse
from grid import grid_day, ALL_SENSORS as _ALL_GRID_SENSORS

_ALL_SENSORS = ('himawari_l2', 'himawari_l3', 'viirs_snpp', 'viirs_noaa20', 'modis_maiac')
_ALL_SITES   = list(AERONET_SITES.keys())
_GRID_SLOTS_PER_DAY = 48


# ── Environment banner ────────────────────────────────────────────────────────

def _print_env_banner() -> None:
    W = 64
    print("=" * W)
    print("Stage A — Collocate/Train — Runtime Environment")
    print("-" * W)
    print(f"  {'Python':<12} {sys.version.split()[0]}")
    for mod_name, label in [
        ("numpy",    "NumPy"),
        ("pandas",   "pandas"),
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


# ── Sub-commands ──────────────────────────────────────────────────────────────

def _enumerate_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


def cmd_grid(args: argparse.Namespace) -> None:
    """Stage A2 — write raw per-sensor 0.05° gridded NetCDFs to GRIDDED_DIR."""
    start   = date.fromisoformat(args.start)
    end     = date.fromisoformat(args.end)
    sensors = tuple(args.sensors) if args.sensors else _ALL_GRID_SENSORS

    days = _enumerate_days(start, end)

    print(f'\nStage A2 grid  sensors={list(sensors)}  {start} → {end}')
    print(f'  Output    : {GRIDDED_DIR}')
    print(f'  Days      : {len(days)}  ({len(days) * _GRID_SLOTS_PER_DAY} slots)')
    print(f'  Workers   : {args.workers}')
    print(f'  Overwrite : {args.overwrite}')

    wall_t0 = time.perf_counter()
    total_written = 0
    total_errors  = 0

    if args.workers > 1:
        # 'spawn' (not the Linux default 'fork') so each worker starts with a
        # clean HDF5/netCDF4 state — fork-after-import of these libraries
        # corrupts HDF5 globals and silently kills workers on heavy IO.
        _mp_ctx = mp.get_context('spawn')
        day_bar = _make_bar(range(len(days)), desc='Days', unit='day', ncols=80)
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=args.workers, mp_context=_mp_ctx,
        ) as ex:
            futures = {
                ex.submit(grid_day, day, sensors, GRIDDED_DIR,
                          args.overwrite, False): day
                for day in days
            }
            for fut in concurrent.futures.as_completed(futures):
                day = futures[fut]
                try:
                    n_ok, n_err = fut.result()
                    total_written += n_ok
                    total_errors  += n_err
                    msg = f'  {day}: {n_ok} new slot(s)' + (f', {n_err} error(s)' if n_err else '')
                except Exception:
                    msg = f'  {day}: FAILED\n{traceback.format_exc()}'
                if day_bar is not None:
                    day_bar.write(msg)   # type: ignore[union-attr]
                    day_bar.update(1)    # type: ignore[union-attr]
                else:
                    print(msg)
        if day_bar is not None:
            day_bar.close()
    else:
        day_bar = _make_bar(days, desc='Days', unit='day', ncols=80)
        day_iter = day_bar if day_bar is not None else days
        for day in day_iter:
            t0 = time.perf_counter()
            n_ok, n_err = grid_day(
                day, sensors=sensors, base=GRIDDED_DIR,
                overwrite=args.overwrite, show_slot_bar=True,
            )
            total_written += n_ok
            total_errors  += n_err
            summary = (f'  {day}: {n_ok} new slot(s)'
                       + (f', {n_err} error(s)' if n_err else '')
                       + f'  [{_fmt_duration(time.perf_counter() - t0)}]')
            if day_bar is not None:
                day_bar.write(summary)   # type: ignore[union-attr]
            else:
                print(summary)
        if day_bar is not None:
            day_bar.close()

    print(f'\nStage A2 grid complete.  {total_written} slot(s) written'
          + (f', {total_errors} error(s)' if total_errors else '')
          + f'  [total: {_fmt_duration(time.perf_counter() - wall_t0)}]')


def cmd_extract(args: argparse.Namespace) -> None:
    """Read satellite files and write raw time-series CSVs to EXTRACT_DIR."""
    sites   = [args.site] if args.site else _ALL_SITES
    sensors = tuple(args.sensors) if args.sensors else _ALL_SENSORS
    start   = date.fromisoformat(args.start)
    end     = date.fromisoformat(args.end)

    print(f'\nExtracting  sites={len(sites)}  sensors={len(sensors)}  {start} → {end}')
    print(f'Output: {EXTRACT_DIR}')

    wall_t0 = time.perf_counter()
    bar     = _make_bar(sites, desc='Sites', unit='site', ncols=80)
    for site in (bar if bar is not None else sites):
        getattr(bar, 'write', print)(f'\n=== {site} ===')
        t0 = time.perf_counter()
        extract_site(site, start, end, sensors=sensors, output_dir=EXTRACT_DIR)
        getattr(bar, 'write', print)(
            f'  → {site} done  [{_fmt_duration(time.perf_counter() - t0)}]'
        )
    if bar is not None:
        bar.close()
    print(f'\nExtraction complete.  [total: {_fmt_duration(time.perf_counter() - wall_t0)}]')


def cmd_match(args: argparse.Namespace) -> None:
    """Match existing raw CSVs with AERONET and write matched-pair CSVs."""
    sites   = [args.site] if args.site else _ALL_SITES
    sensors = tuple(args.sensors) if args.sensors else _ALL_SENSORS

    print(f'\nMatching  sites={len(sites)}  sensors={len(sensors)}')
    print(f'  Input  : {EXTRACT_DIR}')
    print(f'  Output : {COLLOCATE_DIR}')

    wall_t0 = time.perf_counter()
    bar     = _make_bar(sites, desc='Sites', unit='site', ncols=80)
    for site in (bar if bar is not None else sites):
        getattr(bar, 'write', print)(f'\n=== {site} ===')
        t0 = time.perf_counter()
        match_site(site, sensors=sensors, extract_dir=EXTRACT_DIR, output_dir=COLLOCATE_DIR)
        getattr(bar, 'write', print)(
            f'  → {site} done  [{_fmt_duration(time.perf_counter() - t0)}]'
        )
    if bar is not None:
        bar.close()
    print(f'\nMatching complete.  [total: {_fmt_duration(time.perf_counter() - wall_t0)}]')


def cmd_collocate(args: argparse.Namespace) -> None:
    """Extract then match (equivalent to the old collocate command)."""
    sites   = [args.site] if args.site else _ALL_SITES
    sensors = tuple(args.sensors) if args.sensors else _ALL_SENSORS
    start   = date.fromisoformat(args.start)
    end     = date.fromisoformat(args.end)

    print(f'\nCollocating  sites={len(sites)}  sensors={len(sensors)}  {start} → {end}')
    print(f'  Extract → {EXTRACT_DIR}')
    print(f'  Match   → {COLLOCATE_DIR}')

    wall_t0 = time.perf_counter()
    bar     = _make_bar(sites, desc='Sites', unit='site', ncols=80)
    for site in (bar if bar is not None else sites):
        getattr(bar, 'write', print)(f'\n=== {site} ===')
        t0 = time.perf_counter()
        collocate_site(
            site, start, end,
            sensors=sensors,
            output_dir=COLLOCATE_DIR,
            extract_dir=EXTRACT_DIR,
        )
        getattr(bar, 'write', print)(
            f'  → {site} done  [{_fmt_duration(time.perf_counter() - t0)}]'
        )
    if bar is not None:
        bar.close()
    print(f'\nColocation complete.  [total: {_fmt_duration(time.perf_counter() - wall_t0)}]')


def cmd_leo_offset(args: argparse.Namespace) -> None:
    """Build the LEO–Himawari spatial offset map (thesis §7.4.2).

    Requires that Stage A has already been run for [start, end] with the
    Bug 1 fix in place (so AOD_himawari_l2 and AOD_himawari_l3 in MERGED_DIR
    are distinct sources, not duplicates).
    """
    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    out   = Path(args.out) if args.out else LEO_HIMAWARI_OFFSET_FILE
    min_pairs    = args.min_pairs    if args.min_pairs    is not None else LEO_HIMAWARI_MIN_PAIRS
    smooth_sigma = args.smooth_sigma if args.smooth_sigma is not None else LEO_HIMAWARI_SMOOTH_SIGMA

    print(f'\nBuilding LEO–Himawari offset map')
    print(f'  Period       : {start} → {end}')
    print(f'  Output       : {out}')
    print(f'  Min pairs    : {min_pairs}')
    print(f'  Smooth sigma : {smooth_sigma}')

    t0 = time.perf_counter()
    build_leo_himawari_offset(
        start_d=start, end_d=end,
        output_path=out,
        min_pairs=min_pairs,
        smooth_sigma=smooth_sigma,
    )
    print(f'\nDone.  [{_fmt_duration(time.perf_counter() - t0)}]')


def cmd_train(args: argparse.Namespace) -> None:
    sensors = list(args.sensors) if args.sensors else list(_ALL_SENSORS)
    print(f'\nTraining CDF corrections')
    print(f'  Input  : {COLLOCATE_DIR}')
    print(f'  Output : {BIASC_DIR}')
    print(f'  Sensors: {sensors}')
    t0 = time.perf_counter()
    corrections = train_all_corrections(
        collocated_csv_dir=COLLOCATE_DIR,
        sensors=sensors,
        output_dir=BIASC_DIR,
    )
    print(f'\nTrained {len(corrections)} correction(s) saved to {BIASC_DIR}'
          f'  [{_fmt_duration(time.perf_counter() - t0)}]')

    # Persist season-stratified post-correction RMSE so fusion.py uses real
    # weights instead of the SENSOR_RMSE_PRIOR fallback.
    #
    # Bug 2 fix: prefer the cross-validated rmse_after_cv; for strata whose CV
    # value is missing or wildly optimistic, floor against the Sayer/Levy
    # expected-error envelope at a representative AOD so the fusion weights
    # never trust an unrealistically small RMSE.
    ee_floor = SENSOR_RMSE_EE_OFFSET + SENSOR_RMSE_EE_SLOPE * SENSOR_RMSE_EE_REFAOD
    rmse_post: dict[tuple[str, str, str], float] = {}
    for (s, reg, sea), c in corrections.items():
        if c.correction_type == 'none':
            continue
        rmse_cv  = float(getattr(c, 'rmse_after_cv', float('nan')))
        rmse_in  = float(getattr(c, 'rmse_after',     float('nan')))
        candidate = rmse_cv if np.isfinite(rmse_cv) else rmse_in
        if not np.isfinite(candidate):
            continue
        rmse_post[(s, reg, sea)] = float(max(candidate, ee_floor))
    if rmse_post:
        save_rmse(rmse_post)
        print(f'  Post-correction RMSE saved ({len(rmse_post)} strata, '
              f'EE-floor={ee_floor:.3f}) → {BIASC_DIR / "post_correction_rmse.json"}')


# ── CLI ───────────────────────────────────────────────────────────────────────

def _add_date_args(p: argparse.ArgumentParser) -> None:
    p.add_argument('--start',   required=True, help='Start date YYYY-MM-DD')
    p.add_argument('--end',     required=True, help='End date YYYY-MM-DD')


def _add_site_sensor_args(p: argparse.ArgumentParser) -> None:
    p.add_argument('--site',    default=None, help='Single AERONET site (default: all)')
    p.add_argument('--sensors', nargs='+',    help='Sensor(s) to process (default: all)')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Stage A: satellite extraction, AERONET matching, bias-correction training',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest='command')

    # grid (Stage A2)
    p_grid = sub.add_parser(
        'grid',
        help='Stage A2 — produce raw per-sensor gridded NetCDFs in GRIDDED_DIR',
    )
    _add_date_args(p_grid)
    p_grid.add_argument('--sensors',   nargs='+',
                        help='Sensor(s) to grid (default: all)')
    p_grid.add_argument('--workers',   type=int, default=1,
                        help='Parallel worker processes (default: 1)')
    p_grid.add_argument('--overwrite', action='store_true',
                        help='Re-grid slots whose NetCDF already exists')

    # extract
    p_ext = sub.add_parser('extract', help='Extract satellite AOD time series at stations')
    _add_date_args(p_ext)
    _add_site_sensor_args(p_ext)

    # match
    p_match = sub.add_parser('match', help='Match raw CSVs with AERONET (no satellite I/O)')
    _add_site_sensor_args(p_match)

    # collocate (extract + match, backward-compatible)
    p_col = sub.add_parser('collocate', help='Extract then match (= extract + match)')
    _add_date_args(p_col)
    _add_site_sensor_args(p_col)

    # train
    p_tr = sub.add_parser('train', help='Train CDF corrections from matched CSVs')
    p_tr.add_argument('--sensors', nargs='+')

    # leo_offset (thesis §7.4.2)
    p_leo = sub.add_parser(
        'leo_offset',
        help='Build the LEO–Himawari spatial offset map (requires prior Stage A run)',
    )
    _add_date_args(p_leo)
    p_leo.add_argument('--out',           default=None,
                       help=f'Output NetCDF path (default: {LEO_HIMAWARI_OFFSET_FILE})')
    p_leo.add_argument('--min-pairs',     type=int,   default=None,
                       dest='min_pairs',
                       help=f'Mask cells with fewer pairs (default: {LEO_HIMAWARI_MIN_PAIRS})')
    p_leo.add_argument('--smooth-sigma',  type=float, default=None,
                       dest='smooth_sigma',
                       help=f'Gaussian sigma in grid-cells (default: {LEO_HIMAWARI_SMOOTH_SIGMA})')

    # all (grid + extract + match + train)
    p_all = sub.add_parser('all', help='Full pipeline: grid + extract + match + train')
    _add_date_args(p_all)
    _add_site_sensor_args(p_all)
    p_all.add_argument('--workers',   type=int, default=1,
                       help='Parallel workers for the grid step (default: 1)')
    p_all.add_argument('--overwrite', action='store_true',
                       help='Re-grid slots whose NetCDF already exists')

    args = parser.parse_args()
    _print_env_banner()

    if args.command == 'grid':
        cmd_grid(args)
    elif args.command == 'extract':
        cmd_extract(args)
    elif args.command == 'match':
        cmd_match(args)
    elif args.command == 'collocate':
        cmd_collocate(args)
    elif args.command == 'train':
        cmd_train(args)
    elif args.command == 'leo_offset':
        cmd_leo_offset(args)
    elif args.command == 'all':
        cmd_grid(args)
        cmd_collocate(args)
        cmd_train(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
