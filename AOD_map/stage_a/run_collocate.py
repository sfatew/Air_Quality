"""CLI driver — Stage A gridding, AERONET extraction, and v3.4 calibration.

Two distinct workflows live here:

  1. **Stage A2 gridding + AERONET validation extraction.**  These are the
     pre-requisite I/O steps that produce the per-slot gridded NetCDFs and
     (separately) the AERONET-cell extracts used for §8 held-out validation.
     Neither workflow consumes AERONET as training data — see §7.0.

  2. **v3.4 calibration.**  Two linearly-chained verbs:

         soft_cal     — fit `MERRA-2 = α · sat + β` per stratum (§7.4.1)
         tc_variance  — triple-collocation σ² per stratum (§7.4.2)

     The v3.3 `train` (CDF) and `leo_offset` verbs are gone.

Usage
-----
# Stage A2 grid (required first; everything else reads from GRIDDED_DIR).
python run_collocate.py grid --start 2022-09-01 --end 2024-12-31 --workers 4

# AERONET validation extraction (for §8 held-out validation only).
python run_collocate.py extract --start 2022-09-01 --end 2026-04-30
python run_collocate.py match
python run_collocate.py collocate --start 2022-09-01 --end 2026-04-30

# v3.4 calibration chain.
python run_collocate.py soft_cal    --train-start 2022-09-01 --train-end 2024-12-31
python run_collocate.py tc_variance --train-start 2022-09-01 --train-end 2024-12-31

# Everything: grid + extract + match + soft_cal + tc_variance.
python run_collocate.py all --start 2022-09-01 --end 2024-12-31
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
    REGIONS, SEASONS,
    SENSOR_RMSE_EE_OFFSET, SENSOR_RMSE_EE_SLOPE, SENSOR_RMSE_EE_REFAOD,
    SENSOR_EE_SLOPE,
    TC_MIN_TRIPLETS, TC_MIN_COLLOCATIONS,
    TRAIN_START, TRAIN_END,
    SOFT_CAL_FILE, TC_VARIANCE_FILE,
)
from extract_satellite import extract_site
from collocate import match_site, collocate_site
from grid import grid_day, ALL_SENSORS as _ALL_GRID_SENSORS

_ALL_SENSORS = ('himawari_l2', 'himawari_l3', 'viirs_snpp', 'viirs_noaa20', 'modis_maiac')
_ALL_SITES   = list(AERONET_SITES.keys())
_GRID_SLOTS_PER_DAY = 48


# ── Environment banner ────────────────────────────────────────────────────────

def _print_env_banner() -> None:
    W = 64
    print("=" * W)
    print("Stage A — run_collocate (v3.4)")
    print("-" * W)
    print(f"  {'Python':<12} {sys.version.split()[0]}")
    for mod_name, label in [
        ("numpy",    "NumPy"),
        ("pandas",   "pandas"),
        ("netCDF4",  "netCDF4"),
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


def _enumerate_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


# ── Sub-commands: Stage A2 grid ─────────────────────────────────────────────

def cmd_grid(args: argparse.Namespace) -> None:
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
                    msg = (f'  {day}: {n_ok} new slot(s)'
                           + (f', {n_err} error(s)' if n_err else ''))
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


# ── Sub-commands: AERONET-cell extraction (validation only) ─────────────────

def cmd_extract(args: argparse.Namespace) -> None:
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
    print(f'\nCollocation complete.  [total: {_fmt_duration(time.perf_counter() - wall_t0)}]')


# ── Sub-commands: v3.4 soft calibration (§7.4.1) ────────────────────────────

def cmd_soft_cal(args: argparse.Namespace) -> None:
    """Fit the per-stratum linear `MERRA-2 = α · sat + β` soft calibrations."""
    from bias_correction import train_all_sensors

    sensors = list(args.sensors) if args.sensors else None
    ts = getattr(args, 'train_start', None)
    te = getattr(args, 'train_end',   None)
    train_start = date.fromisoformat(ts) if ts else TRAIN_START
    train_end   = date.fromisoformat(te) if te else TRAIN_END
    stride      = int(getattr(args, 'sample_stride', 1) or 1)

    print(f'\n[soft_cal] linear (α, β) vs MERRA-2 per (sensor, region, season)')
    print(f'  Window  : {train_start} → {train_end} (inclusive)')
    print(f'  Sensors : {sensors or "all"}')
    print(f'  Output  : {SOFT_CAL_FILE}')
    if stride > 1:
        print(f'  Stride  : every {stride}th 30-min slot (training-window subsample)')

    t0 = time.perf_counter()
    fits = train_all_sensors(
        sensors=sensors or (
            'himawari_l2', 'himawari_l3',
            'modis_maiac', 'viirs_snpp', 'viirs_noaa20',
        ),
        train_start=train_start, train_end=train_end,
        sample_every_n_slots=stride,
        output_file=SOFT_CAL_FILE,
    )
    n_strata = sum(len(s) for s in fits.values())
    print(f'\n[soft_cal] done.  {n_strata} strata fit across {len(fits)} sensor(s)'
          f'  [{_fmt_duration(time.perf_counter() - t0)}]')


# ── Sub-commands: v3.4 triple-collocation σ² (§7.4.2) ──────────────────────

def cmd_tc_variance(args: argparse.Namespace) -> None:
    """Estimate σ²_TC per (sensor, region, season) from inter-sensor triplets."""
    from bias_correction import load_soft_calibrations
    from triple_collocation import (
        collect_stratum_triplets,
        triple_collocation_variance,
        aggregate_sigma2_per_sensor,
        ee_floor_sigma2 as _tc_ee_floor,
    )
    from fusion import save_tc_variance
    from config import TC_INPUT_SPACE

    ts = getattr(args, 'train_start', None)
    te = getattr(args, 'train_end',   None)
    train_start = date.fromisoformat(ts) if ts else TRAIN_START
    train_end   = date.fromisoformat(te) if te else TRAIN_END
    strict      = bool(getattr(args, 'strict', False))
    stride      = int(getattr(args, 'sample_stride', 1) or 1)

    print(f'\n[tc_variance] σ²_TC per (sensor, region, season)'
          + ('  [strict independence]' if strict else '  [permissive independence]'))
    print(f'  Window  : {train_start} → {train_end} (inclusive)')
    print(f'  Input   : {TC_INPUT_SPACE} '
          + ('(α·sat+β applied before TC)' if TC_INPUT_SPACE == 'calibrated'
             else '(raw AOD into TC; fusion rescales by α²)'))
    print(f'  Output  : {TC_VARIANCE_FILE}')
    if stride > 1:
        print(f'  Stride  : every {stride}th 30-min slot')

    soft_cals = load_soft_calibrations()
    if not soft_cals:
        print('  [tc_variance] WARNING: no soft_calibration.json found — '
              'σ²_TC will be computed on uncorrected satellite values.  '
              'Run `soft_cal` first per §7.4.4 bootstrap ordering.')

    t0 = time.perf_counter()
    print('  Collecting triplets ...')
    stratum_triplets = collect_stratum_triplets(
        train_start=train_start, train_end=train_end,
        soft_cals=soft_cals, strict=strict,
        sample_every_n_slots=stride,
    )

    # Compute σ²_TC per triplet × stratum.
    print('  Computing σ²_TC per triplet × stratum ...')
    sensor_table: dict[str, dict[str, dict]] = {}

    for stratum, triplet_dict in stratum_triplets.items():
        region, season = stratum
        triplet_results: list[dict] = []
        for triple, member_arrs in triplet_dict.items():
            x, y, z = (member_arrs[m] for m in triple)
            res = triple_collocation_variance(x, y, z)
            if not (np.isfinite(res['sigma2_x']) or np.isfinite(res['sigma2_y'])
                    or np.isfinite(res['sigma2_z'])):
                continue
            triplet_results.append({
                'members': triple,
                'sigma2':  {
                    triple[0]: res['sigma2_x'],
                    triple[1]: res['sigma2_y'],
                    triple[2]: res['sigma2_z'],
                },
                'n': res['n'],
            })

        # Per-sensor aggregation (median across triplets, §7.4.2).
        # NOTE: aggregate_sigma2_per_sensor is the user-implemented spot in
        # triple_collocation.py; it raises NotImplementedError until filled.
        agg = aggregate_sigma2_per_sensor(
            triplet_results,
            min_triplets=TC_MIN_TRIPLETS,
            min_collocations=TC_MIN_COLLOCATIONS,
        )
        for sensor, entry in agg.items():
            sigma2 = entry.get('sigma2', float('nan'))
            slope = SENSOR_EE_SLOPE.get(sensor, SENSOR_RMSE_EE_SLOPE)
            ee = _tc_ee_floor(SENSOR_RMSE_EE_OFFSET, slope,
                              SENSOR_RMSE_EE_REFAOD)
            if not np.isfinite(sigma2):
                sigma2 = ee
            else:
                sigma2 = max(float(sigma2), ee)
            sensor_table.setdefault(sensor, {})[f'{region}|{season}'] = {
                'sigma2':          float(sigma2),
                'n_triplets':      int(entry.get('n_triplets', 0)),
                'n_collocations':  int(entry.get('n_collocations', 0)),
            }

    save_tc_variance(sensor_table, TC_VARIANCE_FILE)
    print(f'  σ²_TC table saved → {TC_VARIANCE_FILE}'
          f'  [{_fmt_duration(time.perf_counter() - t0)}]')


# ── 'all' meta-verb: end-to-end calibration chain ───────────────────────────

def cmd_all(args: argparse.Namespace) -> None:
    cmd_grid(args)
    cmd_collocate(args)   # AERONET-cell extracts for §8 validation
    # Soft cal + TC re-use the args' --start / --end as the training window.
    args.train_start = args.start
    args.train_end   = args.end
    cmd_soft_cal(args)
    cmd_tc_variance(args)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _add_date_args(p: argparse.ArgumentParser) -> None:
    p.add_argument('--start', required=True, help='Start date YYYY-MM-DD')
    p.add_argument('--end',   required=True, help='End date YYYY-MM-DD')


def _add_site_sensor_args(p: argparse.ArgumentParser) -> None:
    p.add_argument('--site',    default=None, help='Single AERONET site (default: all)')
    p.add_argument('--sensors', nargs='+',    help='Sensor(s) to process (default: all)')


def _add_train_window_args(p: argparse.ArgumentParser) -> None:
    p.add_argument('--train-start', dest='train_start', default=None,
                   help=f'Inclusive training-window start (default: {TRAIN_START})')
    p.add_argument('--train-end',   dest='train_end',   default=None,
                   help=f'Inclusive training-window end   (default: {TRAIN_END})')
    p.add_argument('--sample-stride', dest='sample_stride', type=int, default=1,
                   help='Subsample every N-th 30-min slot during pair collection '
                        '(default: 1 — every slot)')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Stage A v3.4 — Stage A2 gridding, AERONET extraction, '
                    'soft calibration, and triple-collocation σ²',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest='command')

    # grid (Stage A2)
    p_grid = sub.add_parser('grid', help='Stage A2 — write per-sensor gridded NetCDFs')
    _add_date_args(p_grid)
    p_grid.add_argument('--sensors',   nargs='+')
    p_grid.add_argument('--workers',   type=int, default=1)
    p_grid.add_argument('--overwrite', action='store_true')

    # extract / match / collocate (AERONET validation only)
    p_ext = sub.add_parser('extract', help='Extract satellite AOD time series at AERONET sites')
    _add_date_args(p_ext); _add_site_sensor_args(p_ext)

    p_match = sub.add_parser('match', help='Match raw CSVs with AERONET (no satellite I/O)')
    _add_site_sensor_args(p_match)

    p_col = sub.add_parser('collocate', help='Extract + match (one CLI verb)')
    _add_date_args(p_col); _add_site_sensor_args(p_col)

    # v3.4 calibration verbs
    p_sc = sub.add_parser('soft_cal',
                          help='Fit `MERRA-2 = α · sat + β` per (sensor, region, season)')
    _add_train_window_args(p_sc)
    p_sc.add_argument('--sensors', nargs='+')

    p_tc = sub.add_parser('tc_variance',
                          help='Estimate σ²_TC per (sensor, region, season) from triplets')
    _add_train_window_args(p_tc)
    p_tc.add_argument('--strict', action='store_true',
                      help='Reject pairs sharing the underlying instrument '
                           '(MERRA-2+MAIAC, Himawari L2+L3); '
                           'permissive otherwise (§7.4.2 sensitivity check)')

    # all (grid + collocate + soft_cal + tc_variance)
    p_all = sub.add_parser('all',
                           help='Full pipeline: grid + collocate + soft_cal + tc_variance')
    _add_date_args(p_all); _add_site_sensor_args(p_all)
    p_all.add_argument('--workers',   type=int, default=1)
    p_all.add_argument('--overwrite', action='store_true')
    p_all.add_argument('--strict',    action='store_true')
    p_all.add_argument('--sample-stride', dest='sample_stride', type=int, default=1)

    args = parser.parse_args()
    _print_env_banner()

    dispatch = {
        'grid':        cmd_grid,
        'extract':     cmd_extract,
        'match':       cmd_match,
        'collocate':   cmd_collocate,
        'soft_cal':    cmd_soft_cal,
        'tc_variance': cmd_tc_variance,
        'all':         cmd_all,
    }
    if args.command in dispatch:
        dispatch[args.command](args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
