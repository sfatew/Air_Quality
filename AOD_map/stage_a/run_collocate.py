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
from datetime import date

from config import AERONET_SITES, COLLOCATE_DIR, BIASC_DIR
from collocate import collocate_site
from bias_correction import train_all_corrections

_ALL_SENSORS = ('himawari_l2', 'himawari_l3', 'viirs_snpp', 'viirs_noaa20', 'modis_maiac')
_ALL_SITES   = list(AERONET_SITES.keys())


def cmd_collocate(args: argparse.Namespace) -> None:
    sites   = [args.site] if args.site else _ALL_SITES
    sensors = tuple(args.sensors) if args.sensors else _ALL_SENSORS
    start   = date.fromisoformat(args.start)
    end     = date.fromisoformat(args.end)

    print(f'Collocating {len(sites)} site(s) | {len(sensors)} sensor(s) | '
          f'{start} → {end}')
    print(f'Output: {COLLOCATE_DIR}')

    for site in sites:
        print(f'\n=== {site} ===')
        collocate_site(site, start, end, sensors=sensors, output_dir=COLLOCATE_DIR)

    print('\nColocation complete.')


def cmd_train(args: argparse.Namespace) -> None:
    sensors = list(args.sensors) if args.sensors else list(_ALL_SENSORS)
    print(f'Training CDF corrections from {COLLOCATE_DIR}')
    print(f'Sensors: {sensors}')
    corrections = train_all_corrections(
        collocated_csv_dir=COLLOCATE_DIR,
        sensors=sensors,
        output_dir=BIASC_DIR,
    )
    print(f'\nTrained {len(corrections)} corrections saved to {BIASC_DIR}')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Stage A: AERONET colocation and CDF bias-correction training',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest='command')

    # --- collocate ---
    p_col = sub.add_parser('collocate', help='Extract satellite AOD at AERONET stations')
    p_col.add_argument('--start', required=True, help='Start date YYYY-MM-DD')
    p_col.add_argument('--end',   required=True, help='End date YYYY-MM-DD')
    p_col.add_argument('--site',  default=None,  help='Single AERONET site (default: all)')
    p_col.add_argument('--sensors', nargs='+',   help='Sensor(s) to process (default: all)')

    # --- train ---
    p_tr = sub.add_parser('train', help='Train CDF corrections from collocated CSVs')
    p_tr.add_argument('--sensors', nargs='+',    help='Sensor(s) to train (default: all)')

    # --- all ---
    p_all = sub.add_parser('all', help='Collocate then train')
    p_all.add_argument('--start',   required=True)
    p_all.add_argument('--end',     required=True)
    p_all.add_argument('--site',    default=None)
    p_all.add_argument('--sensors', nargs='+')

    args = parser.parse_args()

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
