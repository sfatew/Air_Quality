#!/usr/bin/env python3
"""Extract ERA5 per-station time series from the monthly NetCDF files
produced by fetch_era5_bbox.py.

Input layout (created by the fetch script):
    ERA5_ROOT/
    ├── 202401/
    │   ├── era5_vietnam_t2m_202401.nc
    │   ├── era5_vietnam_d2m_202401.nc
    │   └── ... (16 files)
    ├── 202402/
    └── ...

Output: one CSV per station at OUTPUT_DIR/<station_name>.csv with columns
    timestamp, station_name, T2m, Td2m, RH, Psfc, MSL, U10, V10,
    WS10m, WD10m, U100, V100, WS100m, WD100m, PBLH, CloudCover,
    CBH, TCWV, SolarRad, Precip, Albedo, CAPE
"""

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm

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
DEFAULT_OUTPUT_DIR = Path("/home/slow_data/Air_Quality/ERA5/stations")
DEFAULT_ERA5_ROOT  = Path("/home/slow_data/Air_Quality/ERA5/raw")

EXTRA_STATIONS = {
    "NGHIA_DO": {"lat": 21.048, "lon": 105.800, "city": "Hà Nội"},
    "Bac_Lieu": {"lat": 9.30,   "lon": 105.70,  "city": "Bạc Liêu"},
}

FEATURE_COLUMNS = [
    "T2m", "Td2m", "RH", "Psfc", "MSL",
    "U10", "V10", "WS10m", "WD10m",
    "U100", "V100", "WS100m", "WD100m",
    "PBLH", "CloudCover", "CBH", "TCWV",
    "SolarRad", "Precip", "Albedo", "CAPE",
]

# Map ERA5 short-name (in NC files) → our feature name
ERA5_TO_FEATURE = {
    "t2m":  "T2m",        # K   → °C
    "d2m":  "Td2m",       # K   → °C
    "sp":   "Psfc",       # Pa  → hPa
    "msl":  "MSL",        # Pa  → hPa
    "u10":  "U10",
    "v10":  "V10",
    "u100": "U100",
    "v100": "V100",
    "blh":  "PBLH",
    "tcc":  "CloudCover",
    "cbh":  "CBH",
    "tcwv": "TCWV",
    "ssrd": "SolarRad",   # J/m² accum/hour → W/m²
    "tp":   "Precip",     # m   → mm
    "fal":  "Albedo",
    "cape": "CAPE",
}


# ── Station loading ───────────────────────────────────────────────────────────
def load_stations(csv_path: Path = DEFAULT_STATION_CSV,
                  include_extra: bool = True) -> dict:
    df = pd.read_csv(csv_path)
    stations = {}
    for _, row in df.iterrows():
        city = row["stationName"].split(":")[0].strip()
        stations[row["stationName"]] = {
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


def open_month(month_dir: Path) -> xr.Dataset:
    """Open all 16 single-variable NCs of a month into one Dataset."""
    ds = xr.open_mfdataset(str(month_dir / "*.nc"), combine="by_coords",
                           engine="netcdf4", compat="override")
    # CDS-Beta quirks
    if "valid_time" in ds.dims:
        ds = ds.rename({"valid_time": "time"})
    for extra in ("number", "expver"):
        if extra in ds.dims:
            ds = ds.isel({extra: 0}, drop=True) if ds.sizes[extra] == 1 \
                else ds.mean(extra)
    return ds


def extract_points(ds: xr.Dataset, stations: dict) -> pd.DataFrame:
    """Vectorized nearest-pixel extraction for ALL stations at once."""
    names = list(stations.keys())
    lats = np.array([stations[n]["lat"] for n in names])
    lons = np.array([stations[n]["lon"] for n in names])

    lat_da = xr.DataArray(lats, dims="station", coords={"station": names})
    lon_da = xr.DataArray(lons, dims="station", coords={"station": names})

    pt = ds.sel(latitude=lat_da, longitude=lon_da, method="nearest")
    df = pt.to_dataframe().reset_index()

    # df now has columns: time, station, latitude, longitude, t2m, d2m, ...
    # Rename short-names → feature names where they exist
    df = df.rename(columns={k: v for k, v in ERA5_TO_FEATURE.items()
                            if k in df.columns})
    return df


def convert_units_and_derive(df: pd.DataFrame) -> pd.DataFrame:
    """Convert units & compute RH, WS, WD."""
    # Temperature K → °C
    for col in ("T2m", "Td2m"):
        if col in df.columns:
            df[col] = df[col] - 273.15

    # Pressure Pa → hPa
    for col in ("Psfc", "MSL"):
        if col in df.columns:
            df[col] = df[col] / 100.0

    # Precip m → mm
    if "Precip" in df.columns:
        df["Precip"] = df["Precip"] * 1000.0

    # SolarRad J/m² (1-hour accumulation) → W/m²
    if "SolarRad" in df.columns:
        df["SolarRad"] = df["SolarRad"] / 3600.0

    # RH (Magnus) — needs both T2m and Td2m in °C (done above)
    if {"T2m", "Td2m"}.issubset(df.columns):
        a, b = 17.625, 243.04
        es = np.exp((a * df["T2m"])  / (b + df["T2m"]))
        e  = np.exp((a * df["Td2m"]) / (b + df["Td2m"]))
        df["RH"] = (100.0 * e / es).clip(0, 100)

    # Wind speed & direction (meteorological: degrees FROM which wind blows)
    for h in ("10", "100"):
        u, v = f"U{h}", f"V{h}"
        if {u, v}.issubset(df.columns):
            df[f"WS{h}m"] = np.sqrt(df[u] ** 2 + df[v] ** 2)
            df[f"WD{h}m"] = (270.0 - np.degrees(np.arctan2(df[v], df[u]))) % 360.0

    return df


def process_month(month_dir: Path, stations: dict) -> pd.DataFrame:
    ds = open_month(month_dir)
    try:
        df = extract_points(ds, stations)
    finally:
        ds.close()

    df = convert_units_and_derive(df)
    df = df.rename(columns={"time": "timestamp", "station": "station_name"})

    cols = ["timestamp", "station_name"] + [c for c in FEATURE_COLUMNS
                                            if c in df.columns]
    return df[cols].sort_values(["station_name", "timestamp"])


# ── Main pipeline ─────────────────────────────────────────────────────────────
def run(era5_root: Path, output_dir: Path, station_csv: Path,
        include_extra: bool = True):
    stations = load_stations(station_csv, include_extra=include_extra)
    logger.info(f"Loaded {len(stations)} stations")

    output_dir.mkdir(parents=True, exist_ok=True)

    month_dirs = sorted(p for p in era5_root.iterdir()
                        if p.is_dir() and p.name.isdigit() and len(p.name) == 6)
    if not month_dirs:
        raise SystemExit(f"No YYYYMM subfolders found in {era5_root}")
    logger.info(f"Found {len(month_dirs)} month folders: "
                f"{month_dirs[0].name} → {month_dirs[-1].name}")

    # Per-station buffer of monthly chunks
    buffers: dict[str, list[pd.DataFrame]] = {name: [] for name in stations}

    for mdir in tqdm(month_dirs, desc="months"):
        try:
            df_month = process_month(mdir, stations)
        except Exception as e:
            logger.error(f"Failed to process {mdir.name}: {e}")
            continue

        for name, sub in df_month.groupby("station_name", sort=False):
            buffers[name].append(sub)

    # Write one CSV per station
    n_ok, n_empty = 0, 0
    for name, parts in buffers.items():
        if not parts:
            logger.warning(f"No data for station {name}")
            n_empty += 1
            continue
        df = (pd.concat(parts, ignore_index=True)
                .drop_duplicates(subset=["timestamp", "station_name"])
                .sort_values("timestamp")
                .reset_index(drop=True))

        out_path = output_dir / f"{safe_filename(name)}.csv"
        df.to_csv(out_path, index=False, float_format="%.4f")
        n_ok += 1
        logger.info(f"[{n_ok:>3}/{len(stations)}] {out_path.name} "
                    f"({len(df):,} rows, {df.timestamp.min()} → "
                    f"{df.timestamp.max()})")

    logger.info(f"Done. Wrote {n_ok} CSV, {n_empty} empty/missing")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--era5-root", type=Path, default=DEFAULT_ERA5_ROOT,
                   help="Folder containing YYYYMM/ subfolders of NC files.")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--station-csv", type=Path, default=DEFAULT_STATION_CSV)
    p.add_argument("--no-extra", action="store_true",
                   help="Skip EXTRA_STATIONS (NGHIA_DO, Bac_Lieu).")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.era5_root, args.output_dir, args.station_csv,
        include_extra=not args.no_extra)