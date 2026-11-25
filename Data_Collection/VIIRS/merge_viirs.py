"""merge_viirs.py — Merge per-station VIIRS L2 NOAA-20 + SNPP into one CSV.

For each station appearing in either L2_NOAA20 or L2_SNPP:
  - concat the two frames, tagging each row's source
  - if the same `datetime` appears in both products (rare — overpass minutes
    differ by ~30 min), collapse to a single row whose numeric columns are
    the mean of the two; source label becomes 'both'

Output: /home/slow_data/Air_Quality/VIIRS/output/process/<station>.csv with:
    datetime, station, station_lat, station_lon,
    aod_mean, aod_median, aod_std, n_pixels, source
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("merge_viirs")


NOAA20_DIR = Path("/home/slow_data/Air_Quality/VIIRS/output/L2_NOAA20")
SNPP_DIR   = Path("/home/slow_data/Air_Quality/VIIRS/output/L2_SNPP")
OUT_DIR    = Path("/home/slow_data/Air_Quality/VIIRS/output/process")

# Columns kept from each L2 product and emitted in the merged CSV
KEEP_COLS = ["datetime", "station", "station_lat", "station_lon",
             "aod_mean", "aod_median", "aod_std", "n_pixels"]
NUMERIC_COLS = ["station_lat", "station_lon",
                "aod_mean", "aod_median", "aod_std", "n_pixels"]


def load_l2(path: Path, source_tag: str) -> pd.DataFrame:
    """Read one L2 per-station CSV, restrict to the columns we keep."""
    df = pd.read_csv(path, usecols=KEEP_COLS, parse_dates=["datetime"], low_memory=False)
    df["source"] = source_tag
    return df


def collapse_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """When `datetime` appears in both NOAA20 and SNPP, average the numerics
    and relabel `source='both'`. Non-duplicated rows pass through unchanged.
    """
    dup_mask = df.duplicated(subset="datetime", keep=False)
    if not dup_mask.any():
        return df

    unique = df.loc[~dup_mask].copy()
    dups   = df.loc[dup_mask].copy()

    agg = (dups.groupby("datetime", as_index=False)
                .agg({**{c: "mean"  for c in NUMERIC_COLS},
                     "station": "first"}))
    agg["source"] = "both"
    out = pd.concat([unique, agg], ignore_index=True)
    return out.sort_values("datetime").reset_index(drop=True)


def merge_one(station_file: str) -> tuple[str, int, int, int]:
    """Merge a single station file. Returns (status, n_total, n_dup, n_only)."""
    n_path = NOAA20_DIR / station_file
    s_path = SNPP_DIR   / station_file

    frames = []
    if n_path.exists():
        frames.append(load_l2(n_path, "noaa20"))
    if s_path.exists():
        frames.append(load_l2(s_path, "snpp"))

    if not frames:
        return "missing", 0, 0, 0

    merged = pd.concat(frames, ignore_index=True)
    n_raw = len(merged)
    n_dup = int(merged.duplicated(subset="datetime", keep=False).sum())
    merged = collapse_duplicates(merged)
    merged = merged.sort_values("datetime").reset_index(drop=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / station_file
    merged.to_csv(out_path, index=False)
    return "ok", len(merged), n_dup // 2, n_raw


def run() -> None:
    files = sorted({p.name for p in NOAA20_DIR.glob("*.csv")}
                   | {p.name for p in SNPP_DIR.glob("*.csv")})
    logger.info(f"{len(files)} unique station files across NOAA20 + SNPP")

    n_ok = n_missing = total_dup = 0
    for f in files:
        try:
            status, n_rows, n_dup, n_raw = merge_one(f)
        except Exception as exc:
            logger.error(f"{f}: {exc}")
            continue
        if status == "ok":
            n_ok += 1
            total_dup += n_dup
            if n_dup:
                logger.info(f"  {f}: {n_rows} rows ({n_dup} datetimes averaged from both)")
        else:
            n_missing += 1

    logger.info(f"done: ok={n_ok} missing={n_missing} duplicate_datetimes_avg={total_dup}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.parse_args()  # no flags yet, kept for future --provider-style options
    run()


if __name__ == "__main__":
    main()
