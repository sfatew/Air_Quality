"""
VIIRS AOD Downloader & Processor v3
=====================================
- CMR API search by bounding box before download
- FIXED QA filter: Deep Blue QA 0=none, 1=low, 2=medium, 3=high
  → Best Estimate SDS already contains QA>=2
  → Filter: qa >= qa_threshold (default 2)
- Output: raw CSV + filtered CSV + diagnostics + plots

References:
  - Hsu et al. (2019) JGR: QA=2/3 recommended, used in L3 products
  - QA Plan: atmosphere-imager.gsfc.nasa.gov
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import json
import requests
import numpy as np
import netCDF4 as nc
import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CMR_SEARCH_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
LAADS_BASE_URL = "https://ladsweb.modaps.eosdis.nasa.gov"

# Deep Blue QA flag meaning:
#   0 = No retrieval / fill
#   1 = Low confidence
#   2 = Medium confidence (recommended minimum)
#   3 = High confidence
# Best Estimate SDS = only QA >= 2
# L3 products use only QA >= 2

PRODUCTS = {
    "deep_blue_snpp": {
        "short_name": "AERDB_L2_VIIRS_SNPP",
        "collection": "5200",
        "aod_variable": "Aerosol_Optical_Thickness_550_Land_Ocean_Best_Estimate",
        "aod_all_variable": "Aerosol_Optical_Thickness_550_Land_Ocean",
        "qa_variable": "Aerosol_Optical_Thickness_QA_Flag_Land",
        "qa_variable_ocean": "Aerosol_Optical_Thickness_QA_Flag_Ocean",
        "description": "Deep Blue L2 Suomi NPP",
    },
    "deep_blue_noaa20": {
        "short_name": "AERDB_L2_VIIRS_NOAA20",
        "collection": "5200",
        "aod_variable": "Aerosol_Optical_Thickness_550_Land_Ocean_Best_Estimate",
        "aod_all_variable": "Aerosol_Optical_Thickness_550_Land_Ocean",
        "qa_variable": "Aerosol_Optical_Thickness_QA_Flag_Land",
        "qa_variable_ocean": "Aerosol_Optical_Thickness_QA_Flag_Ocean",
        "description": "Deep Blue L2 NOAA-20",
    }
}

STATIONS = {
    "HN: 556 Nguyễn Văn Cừ":    {"lat": 21.0491, "lon": 105.8831},
    "HN: CV Nhân Chính":         {"lat": 21.0031, "lon": 105.7947},
    "HN: ĐHBK Giải Phóng":      {"lat": 21.0052, "lon": 105.8418},
}

_lats = [s["lat"] for s in STATIONS.values()]
_lons = [s["lon"] for s in STATIONS.values()]
BBOX = {
    "lat_min": min(_lats) - 0.5,
    "lat_max": max(_lats) + 0.5,
    "lon_min": min(_lons) - 0.5,
    "lon_max": max(_lons) + 0.5,
}


# ---------------------------------------------------------------------------
# CMR Search
# ---------------------------------------------------------------------------
class CMRSearcher:
    def __init__(self, product: str = "deep_blue_noaa20", bbox: dict = None):
        self.product = PRODUCTS[product]
        self.bbox = bbox or BBOX
        self.session = requests.Session()

    def search(self, start_date: datetime, end_date: datetime, page_size: int = 200) -> list[dict]:
        bbox_str = (
            f"{self.bbox['lon_min']},{self.bbox['lat_min']},"
            f"{self.bbox['lon_max']},{self.bbox['lat_max']}"
        )
        params = {
            "short_name": self.product["short_name"],
            "bounding_box": bbox_str,
            "temporal": (
                f"{start_date.strftime('%Y-%m-%dT00:00:00Z')},"
                f"{end_date.strftime('%Y-%m-%dT23:59:59Z')}"
            ),
            "page_size": page_size,
            "sort_key": "-start_date",
        }
        logger.info(
            "CMR search: %s | bbox=%s | %s → %s",
            self.product["short_name"], bbox_str,
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"),
        )

        all_granules = []
        page = 1
        while True:
            params["page_num"] = page
            resp = self.session.get(CMR_SEARCH_URL, params=params, timeout=60)
            resp.raise_for_status()
            entries = resp.json().get("feed", {}).get("entry", [])
            if not entries:
                break

            for entry in entries:
                download_url = None
                for link in entry.get("links", []):
                    href = link.get("href", "")
                    if href.endswith(".nc") and "ladsweb" in href:
                        download_url = href
                        break

                if not download_url:
                    gname = entry.get("producer_granule_id", "")
                    if gname.endswith(".nc"):
                        parts = gname.split(".")
                        year = parts[1][1:5]
                        doy = parts[1][5:8]
                        download_url = (
                            f"{LAADS_BASE_URL}/archive/allData/"
                            f"{self.product['collection']}/{self.product['short_name']}/"
                            f"{year}/{doy}/{gname}"
                        )

                if not download_url:
                    continue

                granule_name = entry.get("producer_granule_id", "") or download_url.split("/")[-1]
                all_granules.append({
                    "name": granule_name,
                    "url": download_url,
                    "size": entry.get("granule_size", "0"),
                    "time_start": entry.get("time_start", ""),
                    "time_end": entry.get("time_end", ""),
                })

            if len(entries) < page_size:
                break
            page += 1

        logger.info("Found %d granules covering bbox", len(all_granules))
        # return all_granules

searcher = CMRSearcher()
searcher.search(start_date=datetime(2025, 1, 1), end_date=datetime(2026, 3, 1))
# ---------------------------------------------------------------------------
# Downloader
# ---------------------------------------------------------------------------
class VIIRSAODDownloader:
    def __init__(self, token: str = None, output_dir: str = "./data/raw"):
        self.token = token
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def download(self, granule: dict, overwrite: bool = False) -> Optional[Path]:
        name = granule["name"]
        try:
            year = name.split(".")[1][1:5]  # A2026078 → 2026
        except (IndexError, ValueError):
            year = "unknown"
        year_dir = self.output_dir / year
        year_dir.mkdir(parents=True, exist_ok=True)
        filepath = year_dir / name

        if filepath.exists() and not overwrite:
            return filepath

        logger.info("Downloading: %s", granule["name"])
        try:
            resp = self.session.get(granule["url"], timeout=300, stream=True)
            resp.raise_for_status()
            ctype = resp.headers.get("Content-Type", "")
            if "html" in ctype or "text" in ctype:
                logger.error("Got HTML instead of NetCDF — check token")
                return None

            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            with open(filepath, "rb") as f:
                magic = f.read(4)
            if magic[:3] not in (b"CDF", b"\x89HD"):
                logger.error("Invalid file: %s", granule["name"])
                filepath.unlink()
                return None

            logger.info("Saved: %s (%.1f MB)", granule["name"], filepath.stat().st_size / 1e6)
            return filepath
        except Exception as e:
            logger.error("Download failed %s: %s", granule["name"], e)
            if filepath.exists():
                filepath.unlink()
            return None

    def download_all(self, granules: list[dict], overwrite: bool = False) -> list[Path]:
        paths = []
        for g in granules:
            p = self.download(g, overwrite)
            if p:
                paths.append(p)
        logger.info("Downloaded %d / %d files", len(paths), len(granules))
        return paths


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------
class VIIRSAODProcessor:
    """
    QA flag for Deep Blue (AERDB):
        0 = No retrieval
        1 = Low confidence
        2 = Medium confidence  ← recommended minimum
        3 = High confidence

    Best_Estimate SDS already pre-filtered to QA >= 2.
    Land_Ocean SDS contains ALL retrievals (QA >= 1).

    Filter logic: qa >= qa_threshold (default=2, i.e. medium+high)
    """

    def __init__(
        self,
        product: str = "deep_blue_noaa20",
        bbox: dict = None,
        stations: dict = None,
        qa_threshold: int = 2,
    ):
        self.product = PRODUCTS[product]
        self.bbox = bbox or BBOX
        self.stations = stations or STATIONS
        self.qa_threshold = qa_threshold

    def _read_variable(self, ds, var_name):
        """Read variable from flat or grouped structure."""
        if var_name in ds.variables:
            return ds.variables[var_name]
        for grp in ds.groups.values():
            if var_name in grp.variables:
                return grp.variables[var_name]
        return None

    def _to_float_array(self, nc_var):
        """Convert NC variable to float32 array, handling mask/fill/scale."""
        raw = nc_var[:]
        if hasattr(raw, "mask"):
            arr = np.where(raw.mask, np.nan, raw.data.astype(np.float32))
        else:
            arr = raw.astype(np.float32)
            fill = getattr(nc_var, "_FillValue", -999.0)
            arr[arr == fill] = np.nan

        scale = getattr(nc_var, "scale_factor", None)
        offset = getattr(nc_var, "add_offset", None)
        if scale is not None:
            arr = arr * float(scale)
        if offset is not None:
            arr = arr + float(offset)
        return arr

    def read_granule(self, filepath: str | Path) -> Optional[dict]:
        filepath = Path(filepath)
        try:
            ds = nc.Dataset(str(filepath), "r")
        except Exception as e:
            logger.error("Cannot open %s: %s", filepath.name, e)
            return None

        try:
            # --- Read lat/lon ---
            lat_var = self._read_variable(ds, "Latitude") or self._read_variable(ds, "latitude")
            lon_var = self._read_variable(ds, "Longitude") or self._read_variable(ds, "longitude")
            if lat_var is None or lon_var is None:
                logger.error("No lat/lon in %s", filepath.name)
                return None

            lat = np.array(lat_var[:], dtype=np.float32)
            lon = np.array(lon_var[:], dtype=np.float32)

            # --- Read AOD Best Estimate (pre-filtered QA>=2) ---
            aod_best_var = self._read_variable(ds, self.product["aod_variable"])
            aod_best = self._to_float_array(aod_best_var) if aod_best_var else None

            # --- Read AOD All QA (unfiltered, QA>=1) ---
            aod_all_var = self._read_variable(ds, self.product["aod_all_variable"])
            aod_all = self._to_float_array(aod_all_var) if aod_all_var else None

            # --- Read QA flags ---
            qa_land_var = self._read_variable(ds, self.product["qa_variable"])
            qa_ocean_var = self._read_variable(ds, self.product.get("qa_variable_ocean", ""))

            qa_land = np.array(qa_land_var[:], dtype=np.int16) if qa_land_var else None
            qa_ocean = np.array(qa_ocean_var[:], dtype=np.int16) if qa_ocean_var else None

            # Combine land+ocean QA: take max (both valid means land takes priority)
            if qa_land is not None and qa_ocean is not None:
                # Where land is fill (-127 or similar), use ocean, and vice versa
                qa_combined = np.where(qa_land > 0, qa_land, qa_ocean)
                qa_combined = np.where(qa_combined > 0, qa_combined, 0)
            elif qa_land is not None:
                qa_combined = np.clip(qa_land, 0, 3).astype(np.int8)
            else:
                qa_combined = np.zeros_like(lat, dtype=np.int8)

            # --- Crop to bbox ---
            mask = (
                (lat >= self.bbox["lat_min"])
                & (lat <= self.bbox["lat_max"])
                & (lon >= self.bbox["lon_min"])
                & (lon <= self.bbox["lon_max"])
            )
            if not np.any(mask):
                return None

            # --- Parse datetime ---
            parts = filepath.stem.split(".")
            date_str, time_str = parts[1], parts[2]
            year = int(date_str[1:5])
            doy = int(date_str[5:8])
            dt = datetime(year, 1, 1) + timedelta(days=doy - 1)
            dt = dt.replace(hour=int(time_str[:2]), minute=int(time_str[2:]))

            result = {
                "filepath": str(filepath),
                "datetime": dt,
                "lat": lat[mask],
                "lon": lon[mask],
                "qa": qa_combined[mask],
                "n_pixels_bbox": int(np.sum(mask)),
            }

            if aod_best is not None:
                result["aod_best"] = aod_best[mask]
                result["n_valid_best"] = int(np.sum(~np.isnan(aod_best[mask])))

            if aod_all is not None:
                result["aod_all"] = aod_all[mask]
                result["n_valid_all"] = int(np.sum(~np.isnan(aod_all[mask])))

            return result

        except Exception as e:
            logger.error("Error processing %s: %s", filepath.name, e)
            return None
        finally:
            ds.close()

    def extract_station_aod(self, granule_data: dict, radius_km: float = 25.0) -> tuple[list[dict], list[dict]]:
        """
        Returns (raw_records, filtered_records).
        raw:      AOD All QA within radius, no filter
        filtered: AOD Best Estimate within radius + QA >= threshold
        """
        if granule_data is None:
            return [], []

        lat = granule_data["lat"]
        lon = granule_data["lon"]
        qa = granule_data["qa"]
        aod_best = granule_data.get("aod_best")
        aod_all = granule_data.get("aod_all")

        raw_records, filtered_records = [], []

        for name, coords in self.stations.items():
            slat, slon = coords["lat"], coords["lon"]

            # Haversine distance
            dlat = np.radians(lat - slat)
            dlon = np.radians(lon - slon)
            a = (
                np.sin(dlat / 2) ** 2
                + np.cos(np.radians(slat)) * np.cos(np.radians(lat)) * np.sin(dlon / 2) ** 2
            )
            dist_km = 6371.0 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

            base_info = {
                "datetime": granule_data["datetime"],
                "station": name,
                "station_lat": slat,
                "station_lon": slon,
            }

            in_radius = dist_km <= radius_km

            # ---- RAW: AOD All QA, no filter ----
            if aod_all is not None and np.any(in_radius):
                a_raw = aod_all[in_radius]
                q_raw = qa[in_radius]
                has_valid = np.any(~np.isnan(a_raw))

                # QA distribution
                qa_unique, qa_counts = np.unique(q_raw, return_counts=True)
                qa_dist = dict(zip(qa_unique.tolist(), qa_counts.tolist()))

                raw_records.append({
                    **base_info,
                    "aod_mean": float(np.nanmean(a_raw)) if has_valid else np.nan,
                    "aod_median": float(np.nanmedian(a_raw)) if has_valid else np.nan,
                    "aod_std": float(np.nanstd(a_raw)) if has_valid else np.nan,
                    "aod_min": float(np.nanmin(a_raw)) if has_valid else np.nan,
                    "aod_max": float(np.nanmax(a_raw)) if has_valid else np.nan,
                    "n_pixels_total": int(np.sum(in_radius)),
                    "n_pixels_valid_aod": int(np.sum(~np.isnan(a_raw))),
                    "n_pixels_nan": int(np.sum(np.isnan(a_raw))),
                    "n_qa0_no_retrieval": int(qa_dist.get(0, 0)),
                    "n_qa1_low": int(qa_dist.get(1, 0)),
                    "n_qa2_medium": int(qa_dist.get(2, 0)),
                    "n_qa3_high": int(qa_dist.get(3, 0)),
                    "qa_distribution": str(qa_dist),
                    "mean_dist_km": float(np.mean(dist_km[in_radius])),
                    "source_sds": "Aerosol_Optical_Thickness_550_Land_Ocean",
                })

            # ---- FILTERED: AOD Best Estimate + QA >= threshold ----
            if aod_best is not None and np.any(in_radius):
                aod_be = aod_best[in_radius]
                q_be = qa[in_radius]

                # Filter: valid AOD + QA >= threshold
                good = (~np.isnan(aod_be)) & (q_be >= self.qa_threshold)

                if np.any(good):
                    a_filt = aod_be[good]
                    filtered_records.append({
                        **base_info,
                        "aod_mean": float(np.nanmean(a_filt)),
                        "aod_median": float(np.nanmedian(a_filt)),
                        "aod_std": float(np.nanstd(a_filt)),
                        "aod_min": float(np.nanmin(a_filt)),
                        "aod_max": float(np.nanmax(a_filt)),
                        "n_pixels": int(np.sum(good)),
                        "qa_threshold": self.qa_threshold,
                        "mean_dist_km": float(np.mean(dist_km[in_radius][good])),
                        "source_sds": "Best_Estimate (QA>=2 pre-filtered)",
                    })

        return raw_records, filtered_records

    def process_directory(self, data_dir: str | Path, radius_km: float = 25.0) -> tuple[pd.DataFrame, pd.DataFrame]:
        data_dir = Path(data_dir)
        nc_files = sorted(data_dir.rglob("*.nc"))
        logger.info("Processing %d .nc files from %s", len(nc_files), data_dir)

        all_raw, all_filt = [], []
        n_with_data = 0

        for f in nc_files:
            granule = self.read_granule(f)
            if granule is None:
                continue

            n_best = granule.get("n_valid_best", 0)
            n_all = granule.get("n_valid_all", 0)

            if n_all > 0 or n_best > 0:
                n_with_data += 1

            raw, filt = self.extract_station_aod(granule, radius_km)
            all_raw.extend(raw)
            all_filt.extend(filt)

            if raw or filt:
                logger.info(
                    "%s: raw=%d filt=%d | bbox_px=%d valid_all=%d valid_best=%d",
                    f.name, len(raw), len(filt),
                    granule["n_pixels_bbox"], n_all, n_best,
                )

        def _to_df(records):
            if not records:
                return pd.DataFrame()
            df = pd.DataFrame(records)
            df["datetime"] = pd.to_datetime(df["datetime"])
            df.sort_values(["datetime", "station"], inplace=True)
            df.reset_index(drop=True, inplace=True)
            return df

        df_raw, df_filt = _to_df(all_raw), _to_df(all_filt)

        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info("Total .nc files scanned:       %d", len(nc_files))
        logger.info("Files with data in bbox:        %d", n_with_data)
        logger.info("Raw records (all QA):           %d", len(df_raw))
        logger.info("Filtered records (QA>=%d):      %d", self.qa_threshold, len(df_filt))
        if not df_raw.empty:
            logger.info("Date range: %s → %s",
                        df_raw["datetime"].min().strftime("%Y-%m-%d"),
                        df_raw["datetime"].max().strftime("%Y-%m-%d"))
            logger.info("Stations with raw data:         %d", df_raw["station"].nunique())
        if not df_filt.empty:
            logger.info("Stations with filtered data:    %d", df_filt["station"].nunique())
        logger.info("=" * 60)

        return df_raw, df_filt


# ---------------------------------------------------------------------------
# Diagnostics & Statistics
# ---------------------------------------------------------------------------
def print_diagnostics(df_raw: pd.DataFrame, df_filt: pd.DataFrame):
    """Print detailed data quality diagnostics."""

    print("\n" + "=" * 70)
    print("DATA QUALITY DIAGNOSTICS")
    print("=" * 70)

    if df_raw.empty:
        print("\n⚠ No raw data available.")
        if df_filt.empty:
            print("⚠ No filtered data either. Likely all pixels masked (cloud cover).")
            print("\nPossible causes:")
            print("  1. Cloud cover in study area during selected dates")
            print("  2. Night-time granules (AOD retrieval requires daylight)")
            print("  3. No granules covering the bbox")
            print("\nSuggestions:")
            print("  - Try a longer date range or different season")
            print("  - Oct-Dec typically clearer in northern Vietnam")
        return

    print("\n--- RAW DATA (All QA, no filter) ---")
    print(f"Total records: {len(df_raw)}")
    print(f"Date range:    {df_raw['datetime'].min()} → {df_raw['datetime'].max()}")
    print(f"Stations:      {df_raw['station'].nunique()}")

    # Per-station raw summary
    print("\nPer-station raw summary:")
    raw_summary = df_raw.groupby("station").agg(
        n_obs=("aod_mean", "count"),
        aod_mean=("aod_mean", "mean"),
        aod_std=("aod_mean", "std"),
        px_total=("n_pixels_total", "sum"),
        px_valid=("n_pixels_valid_aod", "sum"),
        px_nan=("n_pixels_nan", "sum"),
        qa0=("n_qa0_no_retrieval", "sum"),
        qa1=("n_qa1_low", "sum"),
        qa2=("n_qa2_medium", "sum"),
        qa3=("n_qa3_high", "sum"),
    ).round(4)
    print(raw_summary.to_string())

    # Data completeness
    total_px = raw_summary["px_total"].sum()
    valid_px = raw_summary["px_valid"].sum()
    pct_valid = (valid_px / total_px * 100) if total_px > 0 else 0
    print(f"\nOverall pixel completeness: {valid_px}/{total_px} ({pct_valid:.1f}%)")

    # QA distribution overall
    qa_total = raw_summary[["qa0", "qa1", "qa2", "qa3"]].sum()
    qa_sum = qa_total.sum()
    if qa_sum > 0:
        print("\nQA Flag Distribution (all stations combined):")
        print(f"  QA=0 (No retrieval): {int(qa_total['qa0']):>8} ({qa_total['qa0']/qa_sum*100:5.1f}%)")
        print(f"  QA=1 (Low):          {int(qa_total['qa1']):>8} ({qa_total['qa1']/qa_sum*100:5.1f}%)")
        print(f"  QA=2 (Medium):       {int(qa_total['qa2']):>8} ({qa_total['qa2']/qa_sum*100:5.1f}%)")
        print(f"  QA=3 (High):         {int(qa_total['qa3']):>8} ({qa_total['qa3']/qa_sum*100:5.1f}%)")

    if not df_filt.empty:
        print("\n--- FILTERED DATA (Best Estimate, QA>=2) ---")
        print(f"Total records: {len(df_filt)}")
        print(f"Retention:     {len(df_filt)}/{len(df_raw)} ({len(df_filt)/len(df_raw)*100:.1f}%) of raw records")

        filt_summary = df_filt.groupby("station").agg(
            n_obs=("aod_mean", "count"),
            aod_mean=("aod_mean", "mean"),
            aod_median=("aod_median", "mean"),
            aod_std=("aod_mean", "std"),
            aod_min=("aod_min", "min"),
            aod_max=("aod_max", "max"),
            n_pixels=("n_pixels", "sum"),
        ).round(4)
        print("\nPer-station filtered summary:")
        print(filt_summary.to_string())

        # AOD ranges interpretation
        print("\nAOD Interpretation Guide:")
        print("  < 0.1  : Very clean atmosphere")
        print("  0.1-0.3: Low aerosol loading")
        print("  0.3-0.5: Moderate aerosol")
        print("  0.5-1.0: High aerosol (haze/pollution)")
        print("  > 1.0  : Very high (heavy smoke/dust)")
    else:
        print("\n⚠ No data passed QA filter.")
        print("  All pixels in Best Estimate SDS were NaN (masked/cloud).")

    print("=" * 70)


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------
def plot_qa_distribution(df_raw: pd.DataFrame, output_path: str = "qa_distribution.png"):
    """Bar chart of QA flag distribution per station."""
    import matplotlib.pyplot as plt

    if df_raw.empty or "n_qa0_no_retrieval" not in df_raw.columns:
        return

    agg = df_raw.groupby("station").agg(
        qa0=("n_qa0_no_retrieval", "sum"),
        qa1=("n_qa1_low", "sum"),
        qa2=("n_qa2_medium", "sum"),
        qa3=("n_qa3_high", "sum"),
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(agg))
    w = 0.2

    colors = {"qa0": "#d32f2f", "qa1": "#ff9800", "qa2": "#4caf50", "qa3": "#1565c0"}
    labels = {"qa0": "QA=0 (No retrieval)", "qa1": "QA=1 (Low)", "qa2": "QA=2 (Medium)", "qa3": "QA=3 (High)"}

    for i, (col, color) in enumerate(colors.items()):
        ax.bar(x + i * w, agg[col], w, label=labels[col], color=color, alpha=0.85)

    ax.set_xticks(x + 1.5 * w)
    ax.set_xticklabels(agg.index, rotation=25, ha="right", fontsize=9)
    ax.set_ylabel("Number of pixels")
    ax.set_title("QA Flag Distribution per Station\n(0=No retrieval, 1=Low, 2=Medium, 3=High)")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    logger.info("QA distribution plot: %s", output_path)
    plt.close()


def plot_data_completeness(df_raw: pd.DataFrame, output_path: str = "data_completeness.png"):
    """Heatmap: date × station showing data availability."""
    import matplotlib.pyplot as plt

    if df_raw.empty:
        return

    df_raw["date"] = df_raw["datetime"].dt.date
    pivot = df_raw.pivot_table(
        index="date", columns="station", values="n_pixels_valid_aod", aggfunc="sum", fill_value=0,
    )

    fig, ax = plt.subplots(figsize=(12, max(6, len(pivot) * 0.3)))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn", interpolation="nearest")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([str(d) for d in pivot.index], fontsize=8)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=30, ha="right", fontsize=9)
    ax.set_title("Data Completeness: Valid AOD Pixels per Day × Station")
    plt.colorbar(im, ax=ax, label="Valid pixels", shrink=0.7)

    # Annotate cells
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            val = pivot.values[i, j]
            if val > 0:
                ax.text(j, i, str(int(val)), ha="center", va="center", fontsize=7, fontweight="bold")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    logger.info("Completeness heatmap: %s", output_path)
    plt.close()


def plot_raw_vs_filtered(df_raw: pd.DataFrame, df_filt: pd.DataFrame, output_path: str = "raw_vs_filtered.png"):
    """Compare raw vs filtered AOD time series."""
    import matplotlib.pyplot as plt

    if df_raw.empty:
        return

    stations = df_raw["station"].unique()
    n = len(stations)
    fig, axes = plt.subplots(n, 1, figsize=(14, 4 * n), sharex=True)
    if n == 1:
        axes = [axes]

    for ax, station in zip(axes, stations):
        # Raw
        sr = df_raw[df_raw["station"] == station].sort_values("datetime")
        ax.scatter(sr["datetime"], sr["aod_mean"], c="gray", s=20, alpha=0.5, label="Raw (all QA)", zorder=2)

        # Filtered
        if not df_filt.empty:
            sf = df_filt[df_filt["station"] == station].sort_values("datetime")
            if not sf.empty:
                ax.scatter(sf["datetime"], sf["aod_mean"], c="blue", s=30, alpha=0.8, label="Filtered (QA≥2)", zorder=3)
                ax.plot(sf["datetime"], sf["aod_mean"], "b-", alpha=0.4, zorder=2)

        ax.axhline(y=0.4, color="red", ls="--", alpha=0.4, label="High pollution")
        ax.set_ylabel("AOD 550nm")
        ax.set_title(station, fontsize=10)
        ax.legend(fontsize=8, loc="upper right")
        ax.grid(True, alpha=0.3)
        ax.set_ylim(bottom=-0.05)

    axes[-1].set_xlabel("Thời gian")
    fig.suptitle("Raw vs Filtered AOD Comparison", fontsize=14, y=1.01)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    logger.info("Raw vs filtered plot: %s", output_path)
    plt.close()


def plot_aod_map(df, output_path="aod_map.png", title_suffix=""):
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    try:
        import cartopy.crs as ccrs
        import cartopy.feature as cfeature
        has_cartopy = True
    except ImportError:
        has_cartopy = False

    if df.empty:
        return

    agg = df.groupby("station").agg(
        aod_mean=("aod_mean", "mean"), lat=("station_lat", "first"),
        lon=("station_lon", "first"), n=("aod_mean", "count"),
    ).reset_index()

    kw = {"projection": ccrs.PlateCarree()} if has_cartopy else {}
    fig, ax = plt.subplots(figsize=(10, 8), subplot_kw=kw)
    if has_cartopy:
        ax.set_extent([BBOX["lon_min"], BBOX["lon_max"], BBOX["lat_min"], BBOX["lat_max"]], crs=ccrs.PlateCarree())
        ax.add_feature(cfeature.BORDERS, linewidth=0.5)
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
        ax.add_feature(cfeature.RIVERS, alpha=0.5)
        ax.gridlines(draw_labels=True, alpha=0.3)

    norm = mcolors.Normalize(vmin=0, vmax=1.5)
    scatter = ax.scatter(
        agg["lon"], agg["lat"], c=agg["aod_mean"], s=200, cmap="YlOrRd", norm=norm,
        edgecolors="black", linewidths=1.5, zorder=5,
        **({"transform": ccrs.PlateCarree()} if has_cartopy else {}),
    )
    for _, r in agg.iterrows():
        ax.annotate(
            f'{r["station"]}\nAOD={r["aod_mean"]:.3f} (n={r["n"]})',
            xy=(r["lon"], r["lat"]), xytext=(8, 8), textcoords="offset points",
            fontsize=7, fontweight="bold", bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.8),
        )
    plt.colorbar(scatter, ax=ax, label="AOD 550nm", shrink=0.7)
    dates = f'{df["datetime"].min():%Y-%m-%d} → {df["datetime"].max():%Y-%m-%d}'
    ax.set_title(f"VIIRS AOD{title_suffix}\n{dates}", fontsize=14)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    logger.info("Map: %s", output_path)
    plt.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
TOKEN = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImZjY2hpZW4iLCJleHAiOjE3NzkxMTU2NDEsImlhdCI6MTc3MzkzMTY0MSwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.o5wxaKCfh2rs6nKDpnCmNlcr9VmXceqG1byZPI794s3AXvnLfRvToQTzbF18JPj62z211_DLa0hD2QeaWcuXo7m_NCabdSBGFfbOGIV8LTwgyyn9KnJr18iWsPFQlEATgptvYrpQ2xrUweu92chOn0BYe75kQAy5Cc3T1XKed-OXENGXlhtU01K5oYjIijkJ3cs38erTEpRf2IR60caBmgbKXOKSeHga1lC2MXKcJn78vUSd4e0bhGYsgUqHE3pJBluzP4u6VdO86gPgGKMkqy8eFn4Uo0Fv05AxfBd_XNxqpgys2IjOpaZQW4407xtM3Q1_GZEDgcsfmuulOBAeHw'


def run_viirs_aod(
    product: str = "deep_blue_noaa20",
    start: str = None,
    end: str = None,
    days: int = 1,
    data_dir: str = "./data/raw",
    output_dir: str = "./output",
    radius: float = 25.0,
    token: str = TOKEN,
    overwrite: bool = False,
    actions: list = None,
    qa_threshold: int = 2,
):
    """
    Parameters
    ----------
    qa_threshold : int
        Minimum QA flag for filtered output.
        Deep Blue: 0=none, 1=low, 2=medium(recommended), 3=high
        Default=2 means keep medium + high confidence.
    """
    if actions is None:
        actions = ["download", "process", "plot"]

    if start:
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d") if end else datetime.now()
    else:
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

    os.makedirs(output_dir, exist_ok=True)

    if "download" in actions:
        searcher = CMRSearcher(product=product, bbox=BBOX)
        granules = searcher.search(start_date, end_date)
        if not granules:
            logger.warning("No granules found.")
        else:
            downloader = VIIRSAODDownloader(token=token, output_dir=data_dir)
            downloader.download_all(granules, overwrite=overwrite)

    df_raw, df_filt = pd.DataFrame(), pd.DataFrame()
    raw_csv = os.path.join(output_dir, "viirs_aod_raw.csv")
    filt_csv = os.path.join(output_dir, "viirs_aod_filtered.csv")

    if "process" in actions:
        processor = VIIRSAODProcessor(
            product=product, bbox=BBOX, stations=STATIONS, qa_threshold=qa_threshold,
        )
        df_raw, df_filt = processor.process_directory(data_dir, radius_km=radius)

        if not df_raw.empty:
            df_raw.to_csv(raw_csv, index=False)
            logger.info("Raw CSV: %s (%d records)", raw_csv, len(df_raw))
        if not df_filt.empty:
            df_filt.to_csv(filt_csv, index=False)
            logger.info("Filtered CSV: %s (%d records)", filt_csv, len(df_filt))

        # Print diagnostics
        print_diagnostics(df_raw, df_filt)

    if "plot" in actions:
        if df_raw.empty and os.path.exists(raw_csv):
            df_raw = pd.read_csv(raw_csv, parse_dates=["datetime"])
        if df_filt.empty and os.path.exists(filt_csv):
            df_filt = pd.read_csv(filt_csv, parse_dates=["datetime"])

        # Data quality plots
        plot_qa_distribution(df_raw, os.path.join(output_dir, "qa_distribution.png"))
        plot_data_completeness(df_raw, os.path.join(output_dir, "data_completeness.png"))
        plot_raw_vs_filtered(df_raw, df_filt, os.path.join(output_dir, "raw_vs_filtered.png"))

        # Map
        if not df_filt.empty:
            plot_aod_map(df_filt, os.path.join(output_dir, "aod_map_filtered.png"), " — Filtered (QA≥2)")
        if not df_raw.empty:
            plot_aod_map(df_raw, os.path.join(output_dir, "aod_map_raw.png"), " — Raw (all QA)")

    return df_raw, df_filt

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
from matplotlib.patches import Patch
from pathlib import Path

if __name__ == "__main__":
    # df_raw, df_filt = run_viirs_aod(
    #     product="deep_blue_noaa20",
    #     start="2022-01-01", end="2026-03-20", 
    #     data_dir="/home/slow_data/Air_Quality/VIIRS_NOAA20/raw",
    #     output_dir="/home/slow_data/Air_Quality/VIIRS_NOAA20/output",
    #     qa_threshold=2,
    # )

    def _filter_dates(df: pd.DataFrame, start: str = None, end: str = None) -> pd.DataFrame:
        """Filter DataFrame by date range."""
        df = df.copy()
        if start:
            df = df[df["datetime"] >= pd.to_datetime(start)]
        if end:
            df = df[df["datetime"] <= pd.to_datetime(end)]
        return df.reset_index(drop=True)


    def plot_hourly_distribution(df: pd.DataFrame, output_path: str = "hourly_distribution.png"):
        """Phân bố granule và valid pixel theo giờ UTC."""
        df = df.copy()
        df["hour"] = df["datetime"].dt.hour

        hourly = df.groupby("hour").agg(
            n_granules=("datetime", "nunique"),
            total_px=("n_pixels_total", "sum"),
            valid_px=("n_pixels_valid_aod", "sum"),
        ).reindex(range(24), fill_value=0)
        hourly["pct_valid"] = (hourly["valid_px"] / hourly["total_px"] * 100).fillna(0)

        fig, axes = plt.subplots(1, 2, figsize=(20, 5))

        # Left: granule count per hour
        ax = axes[0]
        bars = ax.bar(hourly.index, hourly["n_granules"], color="#1565c0", alpha=0.8)
        # Highlight hours with valid data
        for i, (h, row) in enumerate(hourly.iterrows()):
            if row["valid_px"] > 0:
                bars[i].set_color("#2e7d32")
        ax.set_xlabel("Giờ UTC")
        ax.set_ylabel("Số granules")
        ax.set_title("Số granule theo giờ UTC")
        ax.set_xticks(range(24))
        ax.grid(axis="y", alpha=0.3)

        # Right: valid pixel % per hour
        ax = axes[1]
        colors = ["#2e7d32" if p > 0 else "#c62828" for p in hourly["pct_valid"]]
        ax.bar(hourly.index, hourly["pct_valid"], color=colors, alpha=0.8)
        ax.set_xlabel("Giờ UTC (Hà Nội = UTC+7)")
        ax.set_ylabel("% pixel valid")
        ax.set_title("Tỷ lệ pixel có AOD hợp lệ theo giờ")
        ax.set_xticks(range(24))
        ax.set_ylim(0, 105)
        ax.grid(axis="y", alpha=0.3)

        # Add local time labels
        for ax_ in axes:
            ax2 = ax_.twiny()
            ax2.set_xlim(ax_.get_xlim())
            ax2.set_xticks(range(0, 24, 3))
            ax2.set_xticklabels([f"{(h+7)%24}h" for h in range(0, 24, 3)], fontsize=8, color="gray")
            ax2.set_xlabel("Giờ Hà Nội (UTC+7)", fontsize=8, color="gray")

        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
        plt.close()


    def plot_monthly_summary(df: pd.DataFrame, output_path: str = "monthly_summary.png"):
        """Thống kê theo tháng: số ngày có data, avg valid pixels/day."""
        df = df.copy()
        df["date"] = df["datetime"].dt.date
        df["month"] = df["datetime"].dt.to_period("M")
        df["has_valid"] = df["n_pixels_valid_aod"] > 0

        monthly = df.groupby("month").agg(
            n_days_total=("date", "nunique"),
            n_days_valid=("has_valid", lambda x: x.groupby(df.loc[x.index, "date"]).any().sum()),
            valid_px=("n_pixels_valid_aod", "sum"),
            total_px=("n_pixels_total", "sum"),
        )
        # Simpler: count days where any station has valid
        day_valid = df[df["has_valid"]].groupby("month")["date"].nunique()
        monthly["n_days_valid"] = day_valid.reindex(monthly.index, fill_value=0)
        monthly["avg_valid_px_per_day"] = (monthly["valid_px"] / monthly["n_days_total"]).round(1)
        monthly["pct_days_valid"] = (monthly["n_days_valid"] / monthly["n_days_total"] * 100).round(1)

        fig, axes = plt.subplots(1, 3, figsize=(22, 5))
        months = [str(m) for m in monthly.index]
        x = range(len(months))

        # 1: Days with valid data
        ax = axes[0]
        ax.bar(x, monthly["n_days_total"], color="#e0e0e0", label="Tổng ngày")
        ax.bar(x, monthly["n_days_valid"], color="#2e7d32", alpha=0.8, label="Ngày có data")
        ax.set_xticks(x)
        ax.set_xticklabels(months, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("Số ngày")
        ax.set_title("Số ngày có valid AOD / tháng")
        ax.legend(fontsize=8)
        ax.grid(axis="y", alpha=0.3)

        # 2: % days valid
        ax = axes[1]
        colors = ["#2e7d32" if p > 30 else "#ff9800" if p > 0 else "#c62828"
                for p in monthly["pct_days_valid"]]
        ax.bar(x, monthly["pct_days_valid"], color=colors, alpha=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(months, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("% ngày có data")
        ax.set_title("Tỷ lệ ngày có data / tháng")
        ax.set_ylim(0, 105)
        ax.axhline(y=50, color="orange", ls="--", alpha=0.4)
        ax.grid(axis="y", alpha=0.3)

        # 3: Avg valid pixels per day
        ax = axes[2]
        ax.bar(x, monthly["avg_valid_px_per_day"], color="#6a1b9a", alpha=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(months, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("Avg valid pixels / ngày")
        ax.set_title("Trung bình pixel valid mỗi ngày")
        ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
        plt.close()


    def plot_daily_valid_pixels(df: pd.DataFrame, output_path: str = "daily_valid_pixels.png"):
        """Bar chart: số pixel valid AOD mỗi ngày, wide layout."""
        df = df.copy()
        df["date"] = pd.to_datetime(df["datetime"].dt.date)

        daily = df.groupby("date").agg(
            valid_px=("n_pixels_valid_aod", "sum"),
            total_px=("n_pixels_total", "sum"),
        ).reset_index()
        daily["has_data"] = daily["valid_px"] > 0

        n_days = len(daily)
        width = max(20, n_days * 0.08)

        fig, ax = plt.subplots(figsize=(width, 4))
        colors = ["#2e7d32" if h else "#e0e0e0" for h in daily["has_data"]]
        ax.bar(daily["date"], daily["valid_px"], color=colors, width=0.8)

        ax.set_ylabel("Valid AOD pixels")
        ax.set_title(f"Số pixel AOD hợp lệ mỗi ngày ({daily['date'].min():%Y-%m-%d} → {daily['date'].max():%Y-%m-%d})")
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=8)
        ax.grid(axis="y", alpha=0.3)

        # Stats annotation
        avg_valid = daily["valid_px"].mean()
        days_with = daily["has_data"].sum()
        ax.annotate(
            f"Avg: {avg_valid:.0f} px/day | {days_with}/{n_days} ngày có data ({days_with/n_days*100:.0f}%)",
            xy=(0.02, 0.95), xycoords="axes fraction", fontsize=9,
            bbox=dict(boxstyle="round", fc="lightyellow", alpha=0.8),
        )

        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
        plt.close()


    def plot_station_timeseries(df: pd.DataFrame, output_path: str = "station_timeseries.png"):
        """Time series AOD per station — clean, wide, no variance."""
        df_valid = df[df["n_pixels_valid_aod"] > 0].copy()
        stations = df["station"].unique()
        n = len(stations)

        n_days = df["datetime"].dt.date.nunique()
        width = max(20, n_days * 0.06)

        fig, axes = plt.subplots(n, 1, figsize=(width, 3.5 * n), sharex=True)
        if n == 1:
            axes = [axes]

        colors = ["#1565c0", "#2e7d32", "#d32f2f", "#ff9800", "#6a1b9a"]

        for ax, station, color in zip(axes, stations, colors):
            s_valid = df_valid[df_valid["station"] == station].sort_values("datetime")

            if s_valid.empty:
                ax.text(0.5, 0.5, "Không có dữ liệu AOD hợp lệ",
                        transform=ax.transAxes, ha="center", va="center",
                        fontsize=11, color="red", fontstyle="italic")
                ax.set_xlim(df["datetime"].min(), df["datetime"].max())
            else:
                ax.plot(s_valid["datetime"], s_valid["aod_mean"], "o-",
                        ms=3, color=color, alpha=0.8, linewidth=0.8)
                # Annotate stats
                mean_aod = s_valid["aod_mean"].mean()
                n_obs = len(s_valid)
                ax.annotate(f"Mean AOD: {mean_aod:.3f} | n={n_obs}",
                            xy=(0.02, 0.88), xycoords="axes fraction", fontsize=8,
                            bbox=dict(boxstyle="round", fc="white", alpha=0.7))

            ax.axhline(y=0.4, color="red", ls="--", alpha=0.3)
            ax.set_ylabel("AOD", fontsize=9)
            ax.set_title(station, fontsize=10, fontweight="bold", loc="left")
            ax.grid(True, alpha=0.2)
            ax.set_ylim(bottom=-0.05)

        axes[-1].set_xlabel("Thời gian")
        axes[-1].xaxis.set_major_locator(mdates.MonthLocator())
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        plt.setp(axes[-1].xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=8)

        fig.suptitle("VIIRS AOD Time Series", fontsize=13, fontweight="bold")
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
        plt.close()


    def plot_data_availability_heatmap(df: pd.DataFrame, output_path: str = "availability_heatmap.png"):
        """Heatmap: date × station, color = valid pixels. Wide layout."""
        df = df.copy()
        df["date"] = pd.to_datetime(df["datetime"].dt.date)

        # Sum valid pixels per day × station
        pivot = df.pivot_table(
            index="date", columns="station", values="n_pixels_valid_aod",
            aggfunc="sum", fill_value=0,
        )

        n_days = len(pivot)
        width = max(16, n_days * 0.05)

        fig, ax = plt.subplots(figsize=(width, max(3, len(pivot.columns) * 1.5)))

        # Transpose so stations are rows, dates are columns
        data = pivot.T.values
        im = ax.imshow(data, aspect="auto", cmap="YlGn", interpolation="nearest",
                        vmin=0, vmax=max(data.max(), 1))

        ax.set_yticks(range(len(pivot.columns)))
        ax.set_yticklabels(pivot.columns, fontsize=9)

        # X-axis: show monthly ticks
        dates = pivot.index
        month_starts = [i for i, d in enumerate(dates) if d.day <= 2]
        ax.set_xticks(month_starts)
        ax.set_xticklabels([dates[i].strftime("%Y-%m") for i in month_starts],
                            rotation=45, ha="right", fontsize=8)

        ax.set_title("Data Availability: Valid Pixels (date × station)", fontsize=12, fontweight="bold")
        plt.colorbar(im, ax=ax, label="Valid pixels", shrink=0.5, pad=0.02)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
        plt.close()


    def print_summary(df: pd.DataFrame):
        """Print concise summary."""
        df = df.copy()
        df["date"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour
        df["month"] = df["datetime"].dt.month

        n_days = df["date"].nunique()
        total_px = df["n_pixels_total"].sum()
        valid_px = df["n_pixels_valid_aod"].sum()
        days_with_data = df[df["n_pixels_valid_aod"] > 0]["date"].nunique()

        print("\n" + "=" * 50)
        print("VIIRS AOD DATA SUMMARY")
        print("=" * 50)
        print(f"Period:        {df['datetime'].min():%Y-%m-%d} → {df['datetime'].max():%Y-%m-%d}")
        print(f"Days total:    {n_days}")
        print(f"Days w/ data:  {days_with_data} ({days_with_data/n_days*100:.0f}%)")
        print(f"Avg valid px/day: {valid_px/n_days:.1f}")
        print(f"Valid pixels:  {valid_px:,} / {total_px:,} ({valid_px/total_px*100:.1f}%)" if total_px else "")

        # Hour distribution
        hour_valid = df[df["n_pixels_valid_aod"] > 0].groupby("hour")["n_pixels_valid_aod"].sum()
        if not hour_valid.empty:
            print(f"\nGiờ có data nhiều nhất (UTC): {hour_valid.idxmax()}h ({hour_valid.max():,} px)")
            print(f"  → Giờ Hà Nội: {(hour_valid.idxmax()+7)%24}h")
            print("  Phân bố giờ UTC:")
            for h, px in hour_valid.items():
                local = (h + 7) % 24
                print(f"    {h:02d}h UTC ({local:02d}h local): {px:>8,} px")
        else:
            print("\nKhông có valid pixel nào.")

        # Month distribution
        month_valid = df[df["n_pixels_valid_aod"] > 0]
        if not month_valid.empty:
            month_days = month_valid.groupby("month")["date"].nunique()
            print("\nSố ngày có data theo tháng:")
            month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                        7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
            for m, d in month_days.items():
                print(f"    {month_names.get(m, m):>3}: {d} ngày")

        print("=" * 50)


    def plot_all_diagnostics(
        df: pd.DataFrame,
        output_dir: str = "./plots",
        start: str = None,
        end: str = None,
    ):
        """
        Run all plots with optional date filter.

        Parameters
        ----------
        df : DataFrame from viirs_aod_raw.csv
        output_dir : folder to save plots
        start, end : 'YYYY-MM-DD' date filter (optional)
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        df = _filter_dates(df, start, end)

        if df.empty:
            print("No data in selected date range.")
            return

        print_summary(df)
        plot_hourly_distribution(df, str(output_dir / "hourly_distribution.png"))
        plot_monthly_summary(df, str(output_dir / "monthly_summary.png"))
        plot_daily_valid_pixels(df, str(output_dir / "daily_valid_pixels.png"))
        plot_station_timeseries(df, str(output_dir / "station_timeseries.png"))
        plot_data_availability_heatmap(df, str(output_dir / "availability_heatmap.png"))

        print(f"\nAll plots saved to: {output_dir}/")

    # df = pd.read_csv("/home/slow_data/Air_Quality/VIIRS_NOAA20/output/viirs_aod_raw.csv", parse_dates=["datetime"])
    # plot_all_diagnostics(df, output_dir="/home/slow_data/Air_Quality/VIIRS_NOAA20/output/test1", start="2022-01-01", end="2022-12-31")
