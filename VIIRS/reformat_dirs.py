"""
VIIRS L2 Directory Refactoring Tool
=====================================
Reorganises existing VIIRS L2 files from the old flat layout to the
MODIS-style year/doy layout:

  FROM: /home/slow_data/Air_Quality/VIIRS/L2/{PLATFORM}/{year}/*.nc
  TO:   /home/slow_data/Air_Quality/VIIRS/{SHORT_NAME}/{year}/{doy}/*.nc

Dry-run by default — prints what would happen without touching anything.
Pass --move to perform the actual moves.

Usage:
    python reformat_dirs.py               # dry run
    python reformat_dirs.py --move        # actually move files
    python reformat_dirs.py --move --product NOAA20   # one platform only
"""

import argparse
import shutil
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
OLD_BASE = Path("/home/slow_data/Air_Quality/VIIRS/L2")
NEW_BASE = Path("/home/slow_data/Air_Quality/VIIRS/L2")

PLATFORM_MAP = {
    "NOAA20": "AERDB_L2_VIIRS_NOAA20",
    "SNPP":   "AERDB_L2_VIIRS_SNPP",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_year_doy(filename: str) -> tuple[str, str]:
    """
    Extract year and day-of-year from a VIIRS granule filename.

    Example:
        AERDB_L2_VIIRS_SNPP.A2022292.0548.002.2023075221420.nc
        -> year='2022', doy='292'
    """
    parts = filename.split(".")
    # parts[1] is A{year}{doy}, e.g. A2022292
    date_part = parts[1]
    if not date_part.startswith("A") or len(date_part) < 8:
        raise ValueError(f"Unexpected date token: {date_part!r}")
    year = date_part[1:5]
    doy  = date_part[5:8]
    return year, doy


# ---------------------------------------------------------------------------
# Main refactor logic
# ---------------------------------------------------------------------------
def reformat(dry_run: bool = True, platforms: list[str] | None = None) -> None:
    platforms = platforms or list(PLATFORM_MAP.keys())

    total_moved   = 0
    total_skipped = 0
    total_errors  = 0

    for platform in platforms:
        short_name   = PLATFORM_MAP[platform]
        platform_dir = OLD_BASE / platform

        if not platform_dir.exists():
            print(f"⚠️  Source directory not found, skipping: {platform_dir}")
            continue

        nc_files = sorted(platform_dir.rglob("*.nc"))
        print(f"\n📂 {platform} ({short_name}): {len(nc_files)} .nc files found")

        moved = skipped = errors = 0

        for src in nc_files:
            try:
                year, doy = parse_year_doy(src.name)
            except Exception as exc:
                print(f"  ⚠️  Cannot parse filename {src.name!r}: {exc}")
                errors += 1
                continue

            dest_dir = NEW_BASE / short_name / year / doy
            dest     = dest_dir / src.name

            if dest.exists():
                skipped += 1
                continue

            rel_src  = src.relative_to(OLD_BASE)
            rel_dest = dest.relative_to(NEW_BASE)

            if dry_run:
                print(f"  WOULD MOVE: {rel_src}  →  {rel_dest}")
                moved += 1
            else:
                dest_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dest))
                moved += 1

        label = "Would move" if dry_run else "Moved"
        print(f"  {label}: {moved} | Already at destination: {skipped} | Errors: {errors}")

        total_moved   += moved
        total_skipped += skipped
        total_errors  += errors

    print("\n" + ("=" * 60))
    prefix = "DRY RUN — " if dry_run else ""
    print(f"{prefix}Total: {total_moved} moved, {total_skipped} skipped, {total_errors} errors")

    if dry_run:
        print("\nRun with --move to perform the actual reorganisation.")

    # After moving, try to clean up empty old directories
    if not dry_run:
        _remove_empty_dirs(OLD_BASE)


def _remove_empty_dirs(root: Path) -> None:
    """Remove leaf-empty directories bottom-up under `root`."""
    removed = 0
    for dirpath in sorted(root.rglob("*"), reverse=True):
        if dirpath.is_dir():
            try:
                dirpath.rmdir()   # only succeeds when empty
                removed += 1
            except OSError:
                pass
    if removed:
        print(f"🗑️  Removed {removed} empty directories under {root}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reorganise VIIRS L2 directories from flat year/ to year/doy/ layout."
    )
    parser.add_argument(
        "--move",
        action="store_true",
        help="Actually move files (default: dry run — print only).",
    )
    parser.add_argument(
        "--product",
        choices=list(PLATFORM_MAP.keys()),
        default=None,
        help="Restrict to one platform (default: both NOAA20 and SNPP).",
    )
    args = parser.parse_args()

    platforms = [args.product] if args.product else None

    if not args.move:
        print("🔍 DRY RUN — no files will be moved.\n")
    else:
        print("🚀 Moving files …\n")

    reformat(dry_run=not args.move, platforms=platforms)
