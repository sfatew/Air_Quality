#!/usr/bin/env python3
"""Extract ERA5 per-station time series from the merged NetCDF produced by
fetch_era5_bbox.py.

Input:
    /home/slow_data/Air_Quality/ERA5/Vietnam_ERA5_bbox.nc
    (already post-processed: unit conversions and RH/WS10m/WD10m derived)

Output: one CSV per station at OUTPUT_DIR/<station_name>.csv with columns
    timestamp, station_name, latitude, longitude, T2m, Td2m, RH, Psfc, MSLP,
    U10, V10, WS10m, WD10m, U100, V100, WS100m, WD100m, PBLH, CloudCover,
    CBH, TCWV, SolarRad, Precip, Albedo, CAPE
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
DEFAULT_OUTPUT_DIR = Path("/home/slow_data/Air_Quality/ERA5/stations")
DEFAULT_ERA5_NC    = Path("/home/slow_data/Air_Quality/ERA5/Vietnam_ERA5_bbox.nc")

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


def extract_all_stations(ds: xr.Dataset, stations: dict) -> dict[str, pd.DataFrame]:
    """Vectorized nearest-pixel extraction for ALL stations at once.
    Returns {station_name: DataFrame}."""
    names = list(stations.keys())
    lats  = np.array([stations[n]["lat"] for n in names], dtype=np.float64)
    lons  = np.array([stations[n]["lon"] for n in names], dtype=np.float64)

    lat_da = xr.DataArray(lats, dims="station", coords={"station": names})
    lon_da = xr.DataArray(lons, dims="station", coords={"station": names})

    pt = ds.sel(latitude=lat_da, longitude=lon_da, method="nearest")

    # Materialise once (triggers the dask read); much faster than per-station
    logger.info("Loading point data into memory …")
    pt = pt.load()

    df = pt.to_dataframe().reset_index()
    df = df.rename(columns={"time": "timestamp", "station": "station_name"})

    feature_cols = [c for c in FEATURE_COLUMNS if c in df.columns]
    out_cols = ["timestamp", "station_name", "latitude", "longitude"] + feature_cols
    df = df[out_cols].sort_values(["station_name", "timestamp"])

    return {name: sub.reset_index(drop=True)
            for name, sub in df.groupby("station_name", sort=False)}


# ── Main pipeline ─────────────────────────────────────────────────────────────
def run(era5_nc: Path, output_dir: Path, station_csv: Path,
        include_extra: bool = True):
    stations = load_stations(station_csv, include_extra=include_extra)
    logger.info(f"Loaded {len(stations)} stations")

    if not era5_nc.exists():
        raise SystemExit(f"ERA5 file not found: {era5_nc}")

    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Opening {era5_nc} (chunked) …")
    ds = xr.open_dataset(era5_nc, engine="netcdf4", chunks={"time": 24 * 30})
    logger.info(f"  time: {ds.sizes['time']} steps "
                f"({ds.time.values[0]} → {ds.time.values[-1]})")
    logger.info(f"  grid: {ds.sizes['latitude']} lat × {ds.sizes['longitude']} lon")
    logger.info(f"  vars: {sorted(ds.data_vars)}")

    ds = derive_100m_wind(ds)

    try:
        per_station = extract_all_stations(ds, stations)
    finally:
        ds.close()

    n_ok = 0
    for name in stations:
        df = per_station.get(name)
        if df is None or df.empty:
            logger.warning(f"No data for station {name}")
            continue
        out_path = output_dir / f"{safe_filename(name)}.csv"
        df.to_csv(out_path, index=False, float_format="%.4f")
        n_ok += 1
        logger.info(f"[{n_ok:>3}/{len(stations)}] {out_path.name} "
                    f"({len(df):,} rows, {df.timestamp.min()} → "
                    f"{df.timestamp.max()})")

    logger.info(f"Done. Wrote {n_ok}/{len(stations)} CSV files to {output_dir}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--era5-nc", type=Path, default=DEFAULT_ERA5_NC,
                   help="Merged ERA5 NetCDF produced by fetch_era5_bbox.py")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--station-csv", type=Path, default=DEFAULT_STATION_CSV)
    p.add_argument("--no-extra", action="store_true",
                   help="Skip EXTRA_STATIONS (NGHIA_DO, Bac_Lieu).")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.era5_nc, args.output_dir, args.station_csv,
        include_extra=not args.no_extra)
