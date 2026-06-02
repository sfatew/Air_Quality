"""CLI driver: collocate satellite AOD at AERONET stations, then train CDF corrections.

Usage
-----
# Collocate only (generates training CSVs in COLLOCATE_DIR)
python run_collocate.py collocate --start 2022-09-01 --end 2024-12-31

# Train CDF corrections from existing collocated CSVs
python run_collocate.py train

# Full pipeline: collocate + train
python run_collocate.py all --start 2022-09-01 --end 2024-12-31

# Single site (for debugging)
python run_collocate.py collocate --site NGHIA_DO --start 2023-01-01 --end 2023-03-31
"""

from __future__ import annotations
import argparse
import sys
import time
from datetime import date

try:
    from tqdm import tqdm as _tqdm_cls
    _HAS_TQDM = True
except ImportError:
    _tqdm_cls = None  # type: ignore[assignment]
    _HAS_TQDM = False


def _make_bar(iterable, **kwargs):
    """Return a tqdm bar wrapping *iterable*, or None when tqdm is not installed."""
    if _HAS_TQDM and _tqdm_cls is not None:
        return _tqdm_cls(iterable, **kwargs)
    return None

from config import AERONET_SITES, COLLOCATE_DIR, BIASC_DIR
from collocate import collocate_site
from bias_correction import train_all_corrections

_ALL_SENSORS = ('himawari_l2', 'himawari_l3', 'viirs_snpp', 'viirs_noaa20', 'modis_maiac')
_ALL_SITES   = list(AERONET_SITES.keys())


# ── Environment banner ────────────────────────────────────────────────────────

def _print_env_banner() -> None:
    """Print library versions to stdout."""
    W = 64
    print("=" * W)
    print("Stage A — Collocate/Train — Runtime Environment")
    print("-" * W)
    print(f"  {'Python':<12} {sys.version.split()[0]}")

    _libs = [
        ("numpy",    "NumPy"),
        ("pandas",   "pandas"),
        ("rasterio", "rasterio"),
        ("tqdm",     "tqdm"),
    ]
    for mod_name, label in _libs:
        try:
            mod = __import__(mod_name)
            ver = getattr(mod, "__version__", "?")
            print(f"  {label:<12} {ver}")
        except ImportError:
            print(f"  {label:<12} not installed")

    print("=" * W)


def _fmt_duration(seconds: float) -> str:
    if seconds >= 60:
        m = int(seconds // 60)
        s = seconds - m * 60
        return f"{m}m {s:.1f}s"
    return f"{seconds:.1f}s"


# ── Sub-commands ──────────────────────────────────────────────────────────────

def cmd_collocate(args: argparse.Namespace) -> None:
    sites   = [args.site] if args.site else _ALL_SITES
    sensors = tuple(args.sensors) if args.sensors else _ALL_SENSORS
    start   = date.fromisoformat(args.start)
    end     = date.fromisoformat(args.end)

    print(f'\nCollocating  sites={len(sites)}  sensors={len(sensors)}  {start} → {end}')
    print(f'Output: {COLLOCATE_DIR}')

    wall_t0 = time.perf_counter()
    bar     = _make_bar(sites, desc='Sites', unit='site', ncols=80)

    for site in (bar if bar is not None else sites):
        getattr(bar, 'write', print)(f'\n=== {site} ===')

        t0 = time.perf_counter()
        collocate_site(site, start, end, sensors=sensors, output_dir=COLLOCATE_DIR)

        getattr(bar, 'write', print)(
            f'  → {site} done  [{_fmt_duration(time.perf_counter() - t0)}]'
        )

    if bar is not None:
        bar.close()

    print(f'\nColocation complete.  [total: {_fmt_duration(time.perf_counter() - wall_t0)}]')


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
    elapsed = _fmt_duration(time.perf_counter() - t0)
    print(f'\nTrained {len(corrections)} correction(s) saved to {BIASC_DIR}  [{elapsed}]')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Stage A: AERONET colocation and CDF bias-correction training',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest='command')

    # --- collocate ---
    p_col = sub.add_parser('collocate', help='Extract satellite AOD at AERONET stations')
    p_col.add_argument('--start',   required=True, help='Start date YYYY-MM-DD')
    p_col.add_argument('--end',     required=True, help='End date YYYY-MM-DD')
    p_col.add_argument('--site',    default=None,  help='Single AERONET site (default: all)')
    p_col.add_argument('--sensors', nargs='+',     help='Sensor(s) to process (default: all)')

    # --- train ---
    p_tr = sub.add_parser('train', help='Train CDF corrections from collocated CSVs')
    p_tr.add_argument('--sensors', nargs='+', help='Sensor(s) to train (default: all)')

    # --- all ---
    p_all = sub.add_parser('all', help='Collocate then train')
    p_all.add_argument('--start',   required=True)
    p_all.add_argument('--end',     required=True)
    p_all.add_argument('--site',    default=None)
    p_all.add_argument('--sensors', nargs='+')

    args = parser.parse_args()

    _print_env_banner()

    if args.command == 'collocate':
        cmd_collocate(args)
    elif args.command == 'train':
        cmd_train(args)
    elif args.command == 'all':
        cmd_collocate(args)
        cmd_train(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
