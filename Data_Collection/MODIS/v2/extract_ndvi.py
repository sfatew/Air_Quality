"""
Extract MOD13Q1 NDVI / EVI features for target stations.

For each station, this script extracts:
  * Single nearest pixel values (12 SDS columns)
  * 3x3 window aggregates (mean, std, valid fraction) for NDVI/EVI/reflectances
  * 5x5 window aggregates (mean, std, valid fraction) for NDVI/EVI/reflectances

Both the centre pixel and window means are QA-masked: only pixels with
pixel_reliability ∈ {0, 1} contribute. Windows additionally require
≥ MIN_VALID_FRAC of pixels to pass before the mean is reported.
This lets downstream users choose any aggregation scale without re-extracting.

Output: one CSV per station under OUTPUT_DIR, incrementally appended.
"""

import os
import re
import gc
import glob
import argparse
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyproj
import rasterio
from osgeo import gdal
from rasterio.transform import rowcol, xy as rasterio_xy
from rasterio.windows import Window
from tqdm import tqdm

gdal.UseExceptions()

# ── Paths ─────────────────────────────────────────────────────────────────────
STATION_CSV = "/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv"
HDF_DIR     = "/home/slow_data/Air_Quality/MODIS_MOD13Q1/raw"
OUTPUT_DIR  = "/home/slow_data/Air_Quality/MODIS_MOD13Q1/stations"
ERROR_LOG   = "/home/slow_data/Air_Quality/MODIS_MOD13Q1/extract_errors.log"

MAX_DIST_KM = 0.5
EOS_GRID = "MODIS_Grid_16DAY_250m_500m_VI"

# Window sizes for spatial aggregation (in addition to single-pixel)
WINDOW_SIZES = [3, 5]
# Minimum fraction of valid pixels required in a window for mean to be reported
MIN_VALID_FRAC = 0.2

# ── SDS metadata ─────────────────────────────────────────────────────────────
SDS_SPECS: dict[str, dict] = {
    "250m 16 days NDVI":                       {"col": "NDVI",              "fill": -3000,  "scale": 10000.0, "vlo": -2000, "vhi": 10000, "aggregate": True},
    "250m 16 days EVI":                        {"col": "EVI",               "fill": -3000,  "scale": 10000.0, "vlo": -2000, "vhi": 10000, "aggregate": True},
    "250m 16 days VI Quality":                 {"col": "VI_Quality",        "fill": 65535,  "scale": None,    "vlo": 0,     "vhi": 65534, "aggregate": False},
    "250m 16 days pixel reliability":          {"col": "pixel_reliability", "fill": -1,     "scale": None,    "vlo": 0,     "vhi": 3,     "aggregate": False},
    "250m 16 days red reflectance":            {"col": "red_reflectance",   "fill": -1000,  "scale": 10000.0, "vlo": 0,     "vhi": 10000, "aggregate": True},
    "250m 16 days NIR reflectance":            {"col": "NIR_reflectance",   "fill": -1000,  "scale": 10000.0, "vlo": 0,     "vhi": 10000, "aggregate": True},
    "250m 16 days blue reflectance":           {"col": "blue_reflectance",  "fill": -1000,  "scale": 10000.0, "vlo": 0,     "vhi": 10000, "aggregate": True},
    "250m 16 days MIR reflectance":            {"col": "MIR_reflectance",   "fill": -1000,  "scale": 10000.0, "vlo": 0,     "vhi": 10000, "aggregate": True},
    "250m 16 days view zenith angle":          {"col": "view_zenith",       "fill": -10000, "scale": 100.0,   "vlo": 0,     "vhi": 18000, "aggregate": False},
    "250m 16 days sun zenith angle":           {"col": "sun_zenith",        "fill": -10000, "scale": 100.0,   "vlo": 0,     "vhi": 18000, "aggregate": False},
    "250m 16 days relative azimuth angle":     {"col": "relative_azimuth",  "fill": -4000,  "scale": 100.0,   "vlo": -18000,"vhi": 18000, "aggregate": False},
    "250m 16 days composite day of the year":  {"col": "composite_doy",     "fill": -1,     "scale": None,    "vlo": 1,     "vhi": 366,   "aggregate": False},
}
NDVI_SDS = "250m 16 days NDVI"
RELIABILITY_SDS = "250m 16 days pixel reliability"

MODIS_CRS = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs"
_WGS84_TO_SINU = pyproj.Transformer.from_crs("EPSG:4326", MODIS_CRS, always_xy=True)
_SINU_TO_WGS84 = pyproj.Transformer.from_crs(MODIS_CRS, "EPSG:4326", always_xy=True)

_FILENAME_RE = re.compile(r"MOD13Q1\.A(\d{4})(\d{3})\.h\d{2}v\d{2}")


# ── Coordinate / distance helpers ─────────────────────────────────────────────
def build_station_df(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["stationId"] = df["stationId"].astype(str)
    df["modis_x"], df["modis_y"] = _WGS84_TO_SINU.transform(
        df["longitude"].values, df["latitude"].values,
    )
    return df


def pixel_center_wgs84(transform, row: int, col: int) -> tuple[float, float]:
    sinu_x, sinu_y = rasterio_xy(transform, row, col, offset="center")
    lon, lat = _SINU_TO_WGS84.transform(sinu_x, sinu_y)
    return float(lat), float(lon)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (np.sin(dlat / 2) ** 2
         + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2) ** 2)
    return float(R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))


def parse_composite_start_date(hdf_path: str) -> datetime | None:
    m = _FILENAME_RE.search(os.path.basename(hdf_path))
    if not m:
        return None
    year, doy = int(m.group(1)), int(m.group(2))
    return datetime(year, 1, 1) + timedelta(days=doy - 1)


# ── SDS loading ───────────────────────────────────────────────────────────────
def list_subdatasets(hdf_path: str) -> dict[str, str]:
    """Return {sds_name: gdal_path} by parsing the HDF directly."""
    ds = gdal.Open(hdf_path)
    if ds is None:
        raise IOError(f"Cannot open {hdf_path}")
    try:
        out = {}
        for sd_path, desc in ds.GetSubDatasets():
            for sds_name in SDS_SPECS:
                if sds_name in desc:
                    out[sds_name] = sd_path
                    break
        return out
    finally:
        ds = None  # release GDAL handle


def read_patch(src, row: int, col: int, half: int, spec: dict) -> np.ndarray | None:
    """Read a (2*half+1)x(2*half+1) patch centred at (row, col), scaled, NaN-masked."""
    size = 2 * half + 1
    r0, c0 = row - half, col - half
    if r0 < 0 or c0 < 0 or r0 + size > src.height or c0 + size > src.width:
        return None
    raw = src.read(1, window=Window(c0, r0, size, size)).astype(np.float32)
    invalid = (raw == spec["fill"]) | (raw < spec["vlo"]) | (raw > spec["vhi"])
    raw[invalid] = np.nan
    if spec["scale"] is not None:
        raw = raw / spec["scale"]
    return raw


# ── Window aggregation ───────────────────────────────────────────────────────
def window_aggregate(data: np.ndarray, qa_mask: np.ndarray, min_valid_frac: float) -> dict:
    """
    QA-masked window statistics.
    data:    NxN window of scaled values (NaN for invalid)
    qa_mask: NxN bool window (True = reliability ∈ {0, 1})
    """
    n_total = data.size
    valid = np.isfinite(data) & qa_mask
    n_valid = int(valid.sum())
    valid_frac = n_valid / n_total

    if valid_frac < min_valid_frac or n_valid == 0:
        return {"mean": np.nan, "std": np.nan, "valid_frac": valid_frac}

    vals = data[valid]
    return {
        "mean":       float(np.mean(vals)),
        "std":        float(np.std(vals)),
        "valid_frac": valid_frac,
    }


# ── Per-file extraction ───────────────────────────────────────────────────────
def extract_mod13q1_for_stations(
    hdf_path: str,
    df_stations: pd.DataFrame,
    max_dist_km: float = MAX_DIST_KM,
    window_sizes: list[int] = WINDOW_SIZES,
    min_valid_frac: float = MIN_VALID_FRAC,
) -> pd.DataFrame:
    composite_start = parse_composite_start_date(hdf_path)
    if composite_start is None:
        return pd.DataFrame()

    try:
        sd_paths = list_subdatasets(hdf_path)
    except Exception as exc:
        tqdm.write(f"  [!] Cannot list subdatasets: {exc}")
        return pd.DataFrame()

    if NDVI_SDS not in sd_paths:
        return pd.DataFrame()

    max_half = max(window_sizes) // 2  # largest window radius (2 for 5x5)
    patch_dim = 2 * max_half + 1

    # ---- Locate stations on the NDVI grid (single open) ----
    s_lats = df_stations["latitude"].astype(float).tolist()
    s_lons = df_stations["longitude"].astype(float).tolist()
    station_tuples = list(df_stations.itertuples())

    with rasterio.open(sd_paths[NDVI_SDS]) as ref:
        transform = ref.transform
        height, width = ref.height, ref.width
        rs, cs = rowcol(
            transform,
            df_stations["modis_x"].values,
            df_stations["modis_y"].values,
        )

    valid = []  # list of dicts: idx, r, c, plat, plon, dist, station
    for i, station in enumerate(station_tuples):
        r, c = int(rs[i]), int(cs[i])
        if not (0 <= r < height and 0 <= c < width):
            continue
        plat, plon = pixel_center_wgs84(transform, r, c)
        dist = haversine_km(s_lats[i], s_lons[i], plat, plon)
        if dist > max_dist_km:
            continue
        valid.append({
            "i": i, "r": r, "c": c, "plat": plat, "plon": plon,
            "dist": dist, "station": station,
        })

    if not valid:
        return pd.DataFrame()

    # ---- Per-SDS windowed reads: open each subdataset once, read N tiny patches ----
    # patches[sds_name] -> list aligned with `valid`, each entry is patch or None
    patches: dict[str, list] = {}
    for sds_name, spec in SDS_SPECS.items():
        if sds_name not in sd_paths:
            continue
        try:
            with rasterio.open(sd_paths[sds_name]) as src:
                patches[sds_name] = [
                    read_patch(src, vs["r"], vs["c"], max_half, spec)
                    for vs in valid
                ]
        except Exception as exc:
            tqdm.write(f"  [!] Could not read {sds_name}: {exc}")

    if NDVI_SDS not in patches:
        return pd.DataFrame()

    # ---- Per-station QA-masked aggregates ----
    records = []
    for k, vs in enumerate(valid):
        i = vs["i"]

        single = {}
        for sds_name, spec in SDS_SPECS.items():
            patch = patches.get(sds_name, [None] * len(valid))[k]
            single[spec["col"]] = (
                float(patch[max_half, max_half]) if patch is not None else np.nan
            )

        # Skip only when the centre pixel has no NDVI/EVI value at all
        # (fill/range failure) — station likely outside the MODIS valid grid.
        # QA-failed pixels are handled below by NaN'ing the value but
        # KEEPING the row.
        if not (np.isfinite(single["NDVI"]) or np.isfinite(single["EVI"])):
            continue

        # QA filter at the centre pixel: NaN out NDVI/EVI/reflectances when
        # pixel_reliability ∉ {0, 1}. The row itself is kept so downstream
        # still sees composite_start_date / observation_date / angles /
        # QA flags; only the aggregate-eligible values become NaN.
        rel_center = single.get("pixel_reliability")
        if (rel_center is None or not np.isfinite(rel_center)
                or int(rel_center) not in (0, 1)):
            for sds_name, spec in SDS_SPECS.items():
                if spec["aggregate"]:
                    single[spec["col"]] = np.nan

        doy = single.get("composite_doy")
        if doy is not None and np.isfinite(doy):
            obs = datetime(composite_start.year, 1, 1) + timedelta(days=int(doy) - 1)
            obs_date = obs.strftime("%Y-%m-%d")
        else:
            obs_date = composite_start.strftime("%Y-%m-%d")

        rel_patch = patches.get(RELIABILITY_SDS, [None] * len(valid))[k]
        qa_full = (
            np.isin(rel_patch, [0.0, 1.0])
            if rel_patch is not None
            else np.ones((patch_dim, patch_dim), dtype=bool)
        )

        window_features = {}
        for size in window_sizes:
            sub_half = size // 2
            r0 = max_half - sub_half
            r1 = max_half + sub_half + 1
            qa_win = qa_full[r0:r1, r0:r1]
            for sds_name, spec in SDS_SPECS.items():
                if not spec["aggregate"] or sds_name not in patches:
                    continue
                full_patch = patches[sds_name][k]
                prefix = f"{spec['col']}_{size}x{size}"
                if full_patch is None:
                    window_features[f"{prefix}_mean"]       = np.nan
                    window_features[f"{prefix}_std"]        = np.nan
                    window_features[f"{prefix}_valid_frac"] = np.nan
                else:
                    data_win = full_patch[r0:r1, r0:r1]
                    stats = window_aggregate(data_win, qa_win, min_valid_frac)
                    window_features[f"{prefix}_mean"]       = stats["mean"]
                    window_features[f"{prefix}_std"]        = stats["std"]
                    window_features[f"{prefix}_valid_frac"] = stats["valid_frac"]

        station = vs["station"]
        records.append({
            "stationId":            str(station.stationId),
            "stationName":          station.stationName,
            "station_lat":          s_lats[i],
            "station_lon":          s_lons[i],
            "pixel_lat":            vs["plat"],
            "pixel_lon":            vs["plon"],
            "dist_km":              vs["dist"],
            "composite_start_date": composite_start.strftime("%Y-%m-%d"),
            "observation_date":     obs_date,
            **single,
            **window_features,
        })

    patches.clear()
    return pd.DataFrame(records)


# ── Incremental key cache ─────────────────────────────────────────────────────
def get_existing_keys(csv_path: str) -> set:
    if not os.path.isfile(csv_path):
        return set()
    try:
        df = pd.read_csv(csv_path, usecols=["composite_start_date"])
        return set(df["composite_start_date"].astype(str).tolist())
    except Exception:
        return set()


# ── Main extraction loop ──────────────────────────────────────────────────────
def run_extraction(hdf_dir: str, output_dir: str, error_log: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    df_stations = build_station_df(STATION_CSV)
    print(f"Loaded {len(df_stations)} stations from {STATION_CSV}")

    station_csv_paths = {
        str(row.stationId): os.path.join(output_dir, f"{row.stationName}.csv")
        for row in df_stations.itertuples()
    }
    key_cache = {sid: get_existing_keys(p) for sid, p in station_csv_paths.items()}
    print("Incremental key cache loaded.")

    hdf_files = sorted(
        glob.glob(os.path.join(hdf_dir, "**", "MOD13Q1*.hdf"), recursive=True)
    )
    print(f"Found {len(hdf_files)} HDF files to process.")
    print(f"Aggregations: single pixel + windows {WINDOW_SIZES}\n")

    stats = {"total": len(hdf_files), "skipped": 0, "processed": 0, "error": 0}

    for n, hdf_file in enumerate(tqdm(hdf_files, unit="file", desc="MOD13Q1"), start=1):
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message=".*no geotransform.*")
                df_file = extract_mod13q1_for_stations(hdf_file, df_stations)

            if df_file.empty:
                stats["skipped"] += 1
                continue

            for station_id, group in df_file.groupby("stationId"):
                sid = str(station_id)
                csv_path = station_csv_paths.get(sid)
                if csv_path is None:
                    continue

                new_rows = group[~group["composite_start_date"].isin(key_cache[sid])]
                if new_rows.empty:
                    continue

                file_exists = os.path.isfile(csv_path)
                if file_exists:
                    existing_cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
                    new_rows = new_rows.reindex(columns=existing_cols)
                new_rows.to_csv(csv_path, mode="a", header=not file_exists, index=False)

                key_cache[sid].update(
                    new_rows["composite_start_date"].astype(str).tolist()
                )

            stats["processed"] += 1

        except Exception as exc:
            stats["error"] += 1
            tqdm.write(f"[!] Error processing {os.path.basename(hdf_file)}: {exc}")
            os.makedirs(os.path.dirname(error_log), exist_ok=True)
            with open(error_log, "a") as f:
                f.write(f"{hdf_file}\t{exc}\n")

        if n % 50 == 0:
            gc.collect()

    print("\nDone.")
    print(f"  total={stats['total']}  processed={stats['processed']}  "
          f"skipped={stats['skipped']}  error={stats['error']}")


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Extract MOD13Q1 features at station coordinates "
                    "(single pixel + 3x3 + 5x5 window aggregates)."
    )
    p.add_argument("--hdf-dir",        default=HDF_DIR)
    p.add_argument("--output-dir",     default=OUTPUT_DIR)
    p.add_argument("--error-log",      default=ERROR_LOG)
    p.add_argument("--max-dist-km",    type=float, default=MAX_DIST_KM)
    p.add_argument("--min-valid-frac", type=float, default=MIN_VALID_FRAC,
                   help="Min fraction of valid pixels in a window (default 0.5)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    MAX_DIST_KM = args.max_dist_km
    MIN_VALID_FRAC = args.min_valid_frac
    run_extraction(args.hdf_dir, args.output_dir, args.error_log)