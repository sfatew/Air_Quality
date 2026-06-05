"""
ERA5 Per-Station Extractor
==========================

Sample the Vietnam-wide ERA5 NetCDF produced by `fetch_era5_bbox.py` at the
nearest 0.25° grid cell of each station listed in a station-map CSV, then
write one hourly time-series CSV per station.

Pattern mirrors VIIRS/crawl.py:
  - `load_stations` from a CSV with columns
      stationId, stationName, latitude, longitude
  - `EXTRA_STATIONS` dict for ad-hoc points (NGHIA_DO, Bac_Lieu, …)
  - `group_stations_by_city` for the job report
  - filename-safe station naming
  - `write_job_report` style log

If the merged ERA5 file does not yet exist OR does not span the requested
date range, this script invokes `fetch_era5_bbox.fetch_bbox()` to fill in
the missing months (the per-month checkpoint cache in MONTHLY_DIR makes
this idempotent).

Usage (CLI):
    python extract_stations.py \\
        --station-map /home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv \\
        --start 2024-01-01 \\
        --end   2024-12-31

Usage (Python):
    from ERA5.extract_stations import extract_stations
    df_by_station = extract_stations(start="2024-01-01", end="2024-12-31")
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Make the project root importable so `from ERA5.fetch_era5_bbox import …`
# works whether this file is run as a script or imported as a module.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
import xarray as xr

from ERA5.fetch_era5_bbox import fetch_bbox, BBOX, OUTPUT_FILE, MONTHLY_DIR

# ── Logging ────────────────────────────────────────────────────────────────────
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
DEFAULT_OUTPUT_DIR  = Path("/home/slow_data/Air_Quality/ERA5/stations")
DEFAULT_ERA5_NC     = Path(OUTPUT_FILE)

# Ad-hoc stations not in the envisoft CSV but used downstream
EXTRA_STATIONS = {
    "NGHIA_DO": {"lat": 21.048, "lon": 105.800, "city": "Hà Nội"},
    "Bac_Lieu": {"lat": 9.30,   "lon": 105.70,  "city": "Bạc Liêu"},
}

# Order of feature columns written to each per-station CSV
FEATURE_COLUMNS = [
    "T2m", "Td2m", "RH", "Psfc", "MSL",
    "U10", "V10", "WS10m", "WD10m",
    "U100", "V100", "WS100m", "WD100m",
    "PBLH", "CloudCover", "CBH", "TCWV",
    "SolarRad", "Precip", "Albedo", "CAPE",
]


# ── Station loading (mirrors VIIRS/crawl.py) ───────────────────────────────────
def load_stations(csv_path: Path = DEFAULT_STATION_CSV,
                  include_extra: bool = True) -> dict:
    df = pd.read_csv(csv_path)
    stations: dict = {}
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


def group_stations_by_city(stations: dict) -> dict[str, dict]:
    groups: dict[str, dict] = {}
    for name, info in stations.items():
        groups.setdefault(info["city"], {})[name] = info
    return groups


def safe_filename(station_name: str) -> str:
    return (
        station_name.replace(":", "")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(" ", "_")
        .strip("_")
    )


# ── ERA5 file management ───────────────────────────────────────────────────────
def _months_in_range(start_dt: datetime, end_dt: datetime) -> list[tuple[int, int]]:
    months = []
    y, m = start_dt.year, start_dt.month
    while (y, m) <= (end_dt.year, end_dt.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return months


def ensure_era5_file(start_dt: datetime, end_dt: datetime,
                     era5_nc: Path, bbox: list[float],
                     monthly_dir: str) -> Path:
    """
    Ensure `era5_nc` covers [start_dt, end_dt]. If the file is missing or its
    time axis does not span the requested range, call fetch_bbox() to download
    the missing months and rebuild the merged file.

    Returns the (possibly newly-built) path.
    """
    needs_fetch = False
    if not era5_nc.exists():
        logger.info("ERA5 file %s not found — will fetch.", era5_nc)
        needs_fetch = True
    else:
        with xr.open_dataset(era5_nc) as ds:
            t_min = pd.Timestamp(ds.time.values.min())
            t_max = pd.Timestamp(ds.time.values.max())
        if t_min > pd.Timestamp(start_dt) or t_max < pd.Timestamp(end_dt):
            logger.info(
                "ERA5 file covers %s → %s, requested %s → %s — will fetch.",
                t_min, t_max, start_dt, end_dt,
            )
            needs_fetch = True
        else:
            logger.info("ERA5 file %s already covers requested range.", era5_nc)

    if needs_fetch:
        # Fetch the entire merge range; cached months are skipped by fetch_bbox.
        # Use the union of the requested range and any existing coverage so
        # the rebuilt merged file does not lose previously-downloaded data.
        first, last = (start_dt.year, start_dt.month), (end_dt.year, end_dt.month)
        if era5_nc.exists():
            with xr.open_dataset(era5_nc) as ds:
                existing_min = pd.Timestamp(ds.time.values.min()).to_pydatetime()
                existing_max = pd.Timestamp(ds.time.values.max()).to_pydatetime()
            first = min(first, (existing_min.year, existing_min.month))
            last  = max(last,  (existing_max.year, existing_max.month))
        fetch_bbox(
            start=first, end=last, bbox=bbox,
            output_file=str(era5_nc), monthly_dir=monthly_dir,
        )

    return era5_nc


# ── Per-station sampling ──────────────────────────────────────────────────────
def _sample_station(ds: xr.Dataset, lat: float, lon: float) -> xr.Dataset:
    """Nearest-neighbour sample. ERA5 is 0.25° — no interpolation, by design."""
    lat_name = "latitude" if "latitude" in ds.dims else "lat"
    lon_name = "longitude" if "longitude" in ds.dims else "lon"
    return ds.sel({lat_name: lat, lon_name: lon}, method="nearest")


def extract_stations(
    station_map_csv: Path = DEFAULT_STATION_CSV,
    start: str = None,
    end: str = None,
    era5_nc: Path = DEFAULT_ERA5_NC,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    bbox: list[float] = None,
    monthly_dir: str = MONTHLY_DIR,
    include_extra: bool = True,
) -> dict[str, dict]:
    """
    Extract per-station hourly ERA5 time series.

    Parameters
    ----------
    station_map_csv : path to envisoft-style station CSV
    start, end      : 'YYYY-MM-DD' (end defaults to today). Times are
                      interpreted as the UTC+7 timestamps stored in the ERA5
                      NetCDF — matching what `fetch_era5_bbox.py` writes.

    Returns
    -------
    dict { station_name : {records, date_min, date_max, csv_path} }
    """
    if start is None:
        raise ValueError("`start` is required (YYYY-MM-DD)")
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt   = datetime.strptime(end, "%Y-%m-%d") if end else datetime.now()
    if bbox is None:
        bbox = BBOX

    station_map_csv = Path(station_map_csv)
    era5_nc         = Path(era5_nc)
    output_dir      = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stations = load_stations(station_map_csv, include_extra=include_extra)
    city_groups = group_stations_by_city(stations)
    logger.info("Loaded %d stations across %d cities.",
                len(stations), len(city_groups))

    # Make sure the Vietnam-wide ERA5 file covers the requested window
    ensure_era5_file(start_dt, end_dt, era5_nc, bbox, monthly_dir)

    # Lazy open — only the per-station slice is materialised
    ds = xr.open_dataset(era5_nc, chunks={"time": 24})

    # Restrict to requested window (timestamps in the file are UTC+7, tz-naive)
    t_end_inclusive = pd.Timestamp(f"{end_dt:%Y-%m-%d} 23:00")
    ds = ds.sel(time=slice(pd.Timestamp(start_dt), t_end_inclusive))

    lat_name = "latitude" if "latitude" in ds.dims else "lat"
    lon_name = "longitude" if "longitude" in ds.dims else "lon"

    summary: dict[str, dict] = {}
    feature_cols_present = [c for c in FEATURE_COLUMNS if c in ds.data_vars]
    missing_cols = [c for c in FEATURE_COLUMNS if c not in ds.data_vars]
    if missing_cols:
        logger.warning(
            "ERA5 file is missing expected variables (will be omitted from CSV): %s",
            missing_cols,
        )

    for name, coords in stations.items():
        slat, slon = coords["lat"], coords["lon"]
        sub = _sample_station(ds, slat, slon)[feature_cols_present].load()

        grid_lat = float(sub[lat_name].values)
        grid_lon = float(sub[lon_name].values)

        df = sub.to_dataframe().reset_index()
        # to_dataframe yields lat/lon columns too — drop them to avoid duplicates
        df = df.drop(columns=[c for c in (lat_name, lon_name) if c in df.columns])
        df.insert(0, "station_lon", slon)
        df.insert(0, "station_lat", slat)
        df.insert(0, "station",     name)
        df.insert(0, "datetime",    df.pop("time"))
        df["grid_lat"] = grid_lat
        df["grid_lon"] = grid_lon

        ordered = (
            ["datetime", "station", "station_lat", "station_lon",
             "grid_lat", "grid_lon"]
            + feature_cols_present
        )
        df = df[ordered].sort_values("datetime").reset_index(drop=True)

        out_csv = output_dir / f"{safe_filename(name)}.csv"
        df.to_csv(out_csv, index=False)

        summary[name] = {
            "records":  len(df),
            "date_min": df["datetime"].min().strftime("%Y-%m-%d"),
            "date_max": df["datetime"].max().strftime("%Y-%m-%d"),
            "csv_path": str(out_csv),
        }
        logger.info("  %s → %d rows (%s → %s)",
                    name, len(df), summary[name]["date_min"], summary[name]["date_max"])

    ds.close()
    return summary


# ── Job report (mirrors VIIRS/crawl.py::write_job_report) ─────────────────────
def write_job_report(
    summary: dict[str, dict],
    stations: dict,
    start: str, end: str,
    log_path: Path,
) -> str:
    city_groups = group_stations_by_city(stations)
    lines: list[str] = []
    w = lines.append

    w("=" * 70)
    w(f"ERA5 station extraction — {datetime.now():%Y-%m-%d %H:%M:%S}")
    w("=" * 70)
    w(f"Period: {start} → {end}")
    w(f"Stations: {len(stations)} in {len(city_groups)} cities")
    w("-" * 70)

    for city, members in sorted(city_groups.items()):
        w(f"  {city} ({len(members)} station(s)):")
        for name, info in members.items():
            w(f"    - [{info['lat']:.4f}, {info['lon']:.4f}] {name}")
    w("")

    w("EXTRACTION:")
    if summary:
        by_city: dict[str, list] = {}
        for sname, info in summary.items():
            city = stations.get(sname, {}).get("city", "Unknown")
            by_city.setdefault(city, []).append((sname, info))
        for city in sorted(by_city):
            members = by_city[city]
            total   = sum(s["records"] for _, s in members)
            dmin    = min(s["date_min"] for _, s in members)
            dmax    = max(s["date_max"] for _, s in members)
            w(f"  {city}: {total} records ({dmin} → {dmax})")
            for sname, sinfo in members:
                short = sname.split(":")[-1].strip()[:40]
                w(f"    - {short}: {sinfo['records']} rows "
                  f"({sinfo['date_min']} → {sinfo['date_max']})")

        missing = set(stations) - set(summary)
        if missing:
            w("")
            w(f"NO DATA ({len(missing)}):")
            for sname in sorted(missing):
                w(f"  - {sname}")
    else:
        w("  no records extracted")

    w("")
    w("=" * 70)
    report = "\n".join(lines)
    log_path.write_text(report, encoding="utf-8")
    logger.info("Job report written to %s", log_path)
    return report


# ── CLI ───────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract per-station ERA5 time series.")
    p.add_argument("--station-map", type=Path, default=DEFAULT_STATION_CSV,
                   help="Path to envisoft-style station CSV.")
    p.add_argument("--start", type=str, required=True, help="YYYY-MM-DD")
    p.add_argument("--end",   type=str, default=None,
                   help="YYYY-MM-DD (default: today)")
    p.add_argument("--era5-nc", type=Path, default=DEFAULT_ERA5_NC,
                   help="Path to merged Vietnam ERA5 NetCDF.")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                   help="Where to write per-station CSVs.")
    p.add_argument("--no-extra", action="store_true",
                   help="Skip the built-in EXTRA_STATIONS dict.")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    end_str = args.end or datetime.now().strftime("%Y-%m-%d")
    summary = extract_stations(
        station_map_csv=args.station_map,
        start=args.start,
        end=end_str,
        era5_nc=args.era5_nc,
        output_dir=args.output_dir,
        include_extra=not args.no_extra,
    )

    stations = load_stations(args.station_map, include_extra=not args.no_extra)
    log_path = Path(__file__).parent / f"{datetime.now():%Y%m%d}_era5_extract.log"
    write_job_report(summary, stations, args.start, end_str, log_path)


if __name__ == "__main__":
    main()
