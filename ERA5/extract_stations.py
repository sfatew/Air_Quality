#!/usr/bin/env python3
"""Extract ERA5 per-station time series from the per-month post-processed
NetCDFs produced by fetch_era5_bbox.py (the `_monthly_raw/era5_YYYYMM.nc`
files — already unit-converted with RH/WS10m/WD10m derived).

Each monthly file is opened sequentially; per-station chunks are concatenated
in memory and written once at the end as a single CSV per station.

Output: one CSV per station at OUTPUT_DIR/<station_name>.csv with columns
    timestamp, station_name, latitude, longitude, T2m, Td2m, RH, Psfc, MSLP,
    U10, V10, WS10m, WD10m, U100, V100, WS100m, WD100m, PBLH, CloudCover,
    CBH, TCWV, SolarRad, Precip, Albedo, CAPE

`timestamp` is written in Vietnam local time (UTC+7, no DST) to match
satellite product CSVs that are already in LT downstream.
"""

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Defaults ───────────────────────────────────────────────────────────────────
DEFAULT_STATION_CSV = Path(
    "/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv"
)
DEFAULT_OUTPUT_DIR    = Path("/home/slow_data/Air_Quality/ERA5/stations")
DEFAULT_MONTHLY_DIR   = Path("/home/slow_data/Air_Quality/ERA5/_monthly_raw")
MONTHLY_GLOB          = "era5_??????.nc"

# Vietnam local time = UTC + 7 (no DST). Source NetCDFs from CDS are UTC;
# we shift on write so all downstream files share the same LT clock.
LOCAL_TZ_OFFSET = pd.Timedelta(hours=7)

EXTRA_STATIONS = {
    "NGHIA_DO": {"lat": 21.048, "lon": 105.800, "city": "Hà Nội"},
    "Bac_Lieu": {"lat": 9.30,   "lon": 105.70,  "city": "Bạc Liêu"},
}

# Output column order (only columns present in the dataset are written)
FEATURE_COLUMNS = [
    "T2m", "Td2m", "RH", "Psfc", "MSLP",
    "U10", "V10", "WS10m", "WD10m",
    "U100", "V100", "WS100m", "WD100m",
    "PBLH", "CloudCover", "CBH", "TCWV",
    "SolarRad", "Precip", "Albedo", "CAPE",
]


# ── Station loading ───────────────────────────────────────────────────────────
def load_stations(csv_path: Path = DEFAULT_STATION_CSV,
                  include_extra: bool = True) -> dict:
    df = pd.read_csv(csv_path)
    stations = {}
    for _, row in df.iterrows():
        name = row["stationName"]
        city = name.split(":")[0].strip()
        stations[name] = {
            "lat":  float(row["latitude"]),
            "lon":  float(row["longitude"]),
            "city": city,
        }
    if include_extra:
        stations.update(EXTRA_STATIONS)
    return stations


# ── Helpers ───────────────────────────────────────────────────────────────────
def safe_filename(name: str) -> str:
    """Filename-safe slug; preserves diacritics-free station names."""
    keep = []
    for ch in name:
        if ch.isalnum() or ch in ("-", "_"):
            keep.append(ch)
        elif ch in (" ", ":", "/"):
            keep.append("_")
    return "".join(keep).strip("_")


def derive_100m_wind(ds: xr.Dataset) -> xr.Dataset:
    """Add WS100m / WD100m if U100 / V100 are present (fetch script only
    derives the 10-m wind diagnostics)."""
    if "U100" in ds and "V100" in ds and "WS100m" not in ds:
        u, v = ds["U100"], ds["V100"]
        ds["WS100m"] = np.sqrt(u**2 + v**2).astype(np.float32)
        ds["WS100m"].attrs = {"long_name": "100-m wind speed", "units": "m s-1"}
        ds["WD100m"] = ((270.0 - np.degrees(np.arctan2(v, u))) % 360.0).astype(np.float32)
        ds["WD100m"].attrs = {
            "long_name": "100-m wind direction (meteorological)",
            "units":     "degrees",
        }
    return ds


def extract_all_stations(ds: xr.Dataset, stations: dict,
                         names: list[str],
                         lat_da: xr.DataArray,
                         lon_da: xr.DataArray) -> dict[str, pd.DataFrame]:
    """Vectorized nearest-pixel extraction for ALL stations at once.
    Returns {station_name: DataFrame} for a single monthly dataset."""
    pt = ds.sel(latitude=lat_da, longitude=lon_da, method="nearest").load()

    df = pt.to_dataframe().reset_index()
    df = df.rename(columns={"time": "timestamp", "station": "station_name"})
    df["timestamp"] = pd.to_datetime(df["timestamp"]) + LOCAL_TZ_OFFSET

    feature_cols = [c for c in FEATURE_COLUMNS if c in df.columns]
    out_cols = ["timestamp", "station_name", "latitude", "longitude"] + feature_cols
    df = df[out_cols].sort_values(["station_name", "timestamp"])

    return {name: sub.reset_index(drop=True)
            for name, sub in df.groupby("station_name", sort=False)}


# ── Main pipeline ─────────────────────────────────────────────────────────────
def run(monthly_dir: Path, output_dir: Path, station_csv: Path,
        include_extra: bool = True):
    stations = load_stations(station_csv, include_extra=include_extra)
    logger.info(f"Loaded {len(stations)} stations")

    if not monthly_dir.is_dir():
        raise SystemExit(f"Monthly ERA5 directory not found: {monthly_dir}")

    monthly_files = sorted(monthly_dir.glob(MONTHLY_GLOB))
    if not monthly_files:
        raise SystemExit(f"No monthly files matching {MONTHLY_GLOB} in {monthly_dir}")
    logger.info(f"Found {len(monthly_files)} monthly files "
                f"({monthly_files[0].name} → {monthly_files[-1].name})")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Build the station selector once — reused for every monthly file.
    names = list(stations.keys())
    lats  = np.array([stations[n]["lat"] for n in names], dtype=np.float64)
    lons  = np.array([stations[n]["lon"] for n in names], dtype=np.float64)
    lat_da = xr.DataArray(lats, dims="station", coords={"station": names})
    lon_da = xr.DataArray(lons, dims="station", coords={"station": names})

    per_station_chunks: dict[str, list[pd.DataFrame]] = {n: [] for n in names}

    for i, nc_path in enumerate(monthly_files, 1):
        logger.info(f"[{i:>3}/{len(monthly_files)}] Reading {nc_path.name} …")
        with xr.open_dataset(nc_path, engine="netcdf4") as ds:
            ds = derive_100m_wind(ds)
            chunks = extract_all_stations(ds, stations, names, lat_da, lon_da)
        for name, sub in chunks.items():
            if not sub.empty:
                per_station_chunks[name].append(sub)

    n_ok = 0
    for name in names:
        parts = per_station_chunks[name]
        if not parts:
            logger.warning(f"No data for station {name}")
            continue
        df = (pd.concat(parts, ignore_index=True)
                .drop_duplicates(subset=["timestamp"])
                .sort_values("timestamp")
                .reset_index(drop=True))
        out_path = output_dir / f"{safe_filename(name)}.csv"
        df.to_csv(out_path, index=False, float_format="%.4f")
        n_ok += 1
        logger.info(f"[{n_ok:>3}/{len(stations)}] {out_path.name} "
                    f"({len(df):,} rows, {df.timestamp.min()} → "
                    f"{df.timestamp.max()})")

    logger.info(f"Done. Wrote {n_ok}/{len(stations)} CSV files to {output_dir}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--monthly-dir", type=Path, default=DEFAULT_MONTHLY_DIR,
                   help="Directory of per-month ERA5 NetCDFs "
                        "(era5_YYYYMM.nc) produced by fetch_era5_bbox.py")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--station-csv", type=Path, default=DEFAULT_STATION_CSV)
    p.add_argument("--no-extra", action="store_true",
                   help="Skip EXTRA_STATIONS (NGHIA_DO, Bac_Lieu).")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.monthly_dir, args.output_dir, args.station_csv,
        include_extra=not args.no_extra)
