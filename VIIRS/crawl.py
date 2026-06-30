"""
VIIRS AOD Downloader & Processor v4 — OOM-fixed
=================================================
Key fixes vs original:
  1. read_granule_d3: use Latitude_1D/Longitude_1D (not 2D Latitude/Longitude)
  2. read_granule_d3: read only bbox subset via _read_subset (64800 → ~2 cells)
  3. process_d3 / process_l2: del + gc.collect() every N files
  4. All other logic preserved
"""

import os
import gc
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
import numpy as np
import netCDF4 as nc
import pandas as pd
import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))
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

PRODUCTS = {
    "L2": {
        "deep_blue_noaa20": {
            "short_name": "AERDB_L2_VIIRS_NOAA20",
            "collection": "5200",
            "platform": "NOAA20",
            "aod_variable": "vi",
            "aod_all_variable": "Aerosol_Optical_Thickness_550_Land_Ocean",
            "qa_variable": "Aerosol_Optical_Thickness_QA_Flag_Land",
            "qa_variable_ocean": "Aerosol_Optical_Thickness_QA_Flag_Ocean",
        },
        "deep_blue_snpp": {
            "short_name": "AERDB_L2_VIIRS_SNPP",
            "collection": "5200",
            "platform": "SNPP",
            "aod_variable": "Aerosol_Optical_Thickness_550_Land_Ocean_Best_Estimate",
            "aod_all_variable": "Aerosol_Optical_Thickness_550_Land_Ocean",
            "qa_variable": "Aerosol_Optical_Thickness_QA_Flag_Land",
            "qa_variable_ocean": "Aerosol_Optical_Thickness_QA_Flag_Ocean",
        },
    },
    "D3": {
        "deep_blue_noaa20_d3": {
            "short_name": "AERDB_D3_VIIRS_NOAA20",
            "collection": "5200",
            "platform": "NOAA20",
            "aod_mean": "Aerosol_Optical_Thickness_550_Land_Ocean_Mean",
            "aod_count": "Aerosol_Optical_Thickness_550_Land_Ocean_Count",
            "aod_min": "Aerosol_Optical_Thickness_550_Land_Ocean_Minimum",
            "aod_max": "Aerosol_Optical_Thickness_550_Land_Ocean_Maximum",
        },
        "deep_blue_snpp_d3": {
            "short_name": "AERDB_D3_VIIRS_SNPP",
            "collection": "5200",
            "platform": "SNPP",
            "aod_mean": "Aerosol_Optical_Thickness_550_Land_Ocean_Mean",
            "aod_count": "Aerosol_Optical_Thickness_550_Land_Ocean_Count",
            "aod_min": "Aerosol_Optical_Thickness_550_Land_Ocean_Minimum",
            "aod_max": "Aerosol_Optical_Thickness_550_Land_Ocean_Maximum",
        },
    },
}

SITES_CSV = Path("/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv")


def load_stations(csv_path: Path = SITES_CSV) -> dict:
    df = pd.read_csv(csv_path)
    stations = {}
    for _, row in df.iterrows():
        city = row["stationName"].split(":")[0].strip()
        stations[row["stationName"]] = {
            "lat": row["latitude"],
            "lon": row["longitude"],
            "city": city,
        }
    return stations




def compute_bbox(stations: dict, padding: float = 0.5) -> dict:
    lats = [s["lat"] for s in stations.values()]
    lons = [s["lon"] for s in stations.values()]
    return {
        "lat_min": min(lats) - padding,
        "lat_max": max(lats) + padding,
        "lon_min": min(lons) - padding,
        "lon_max": max(lons) + padding,
    }


def group_stations_by_city(stations: dict) -> dict[str, dict]:
    groups = {}
    for name, info in stations.items():
        city = info["city"]
        if city not in groups:
            groups[city] = {}
        groups[city][name] = info
    return groups


EXTRA_STATIONS = {
    "NGHIA_DO": {"lat": 21.048, "lon": 105.800, "city": "Hà Nội"},
    "Bac_Lieu":  {"lat": 9.30,  "lon": 105.70,  "city": "Bạc Liêu"},
}

STATIONS = {**load_stations(), **EXTRA_STATIONS}
BBOX = compute_bbox(STATIONS)

# How often to force garbage collection (every N files)
GC_EVERY = 50


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def get_product_config(product_key: str) -> tuple[str, dict]:
    for level, products in PRODUCTS.items():
        if product_key in products:
            return level, products[product_key]
    all_keys = [k for p in PRODUCTS.values() for k in p]
    raise ValueError(f"Unknown product: {product_key}. Available: {all_keys}")


# ---------------------------------------------------------------------------
# CMR Search
# ---------------------------------------------------------------------------
class CMRSearcher:
    def __init__(self, product_key: str, bbox: dict = None):
        self.level, self.product = get_product_config(product_key)
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
                granule_name = entry.get("producer_granule_id", "")
                if not granule_name.endswith(".nc"):
                    continue

                parts = granule_name.split(".")
                year = parts[1][1:5]
                doy = parts[1][5:8]
                download_url = (
                    f"{LAADS_BASE_URL}/archive/allData/"
                    f"{self.product['collection']}/{self.product['short_name']}/"
                    f"{year}/{doy}/{granule_name}"
                )

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

        logger.info("Found %d granules", len(all_granules))
        return all_granules


# ---------------------------------------------------------------------------
# Downloader
# ---------------------------------------------------------------------------
class VIIRSAODDownloader:
    def __init__(self, token: str, output_dir: str, level: str, platform: str):
        self.token = token
        self.base_dir = Path(output_dir) / level / platform
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        self.stats = {
            "granules_found": 0,
            "skipped": 0,
            "downloaded": 0,
            "failed": [],
        }

    def _get_filepath(self, granule_name: str) -> Path:
        try:
            year = granule_name.split(".")[1][1:5]
        except (IndexError, ValueError):
            year = "unknown"
        year_dir = self.base_dir / year
        year_dir.mkdir(parents=True, exist_ok=True)
        return year_dir / granule_name

    def download(self, granule: dict, overwrite: bool = False) -> Optional[Path]:
        filepath = self._get_filepath(granule["name"])
        if filepath.exists() and not overwrite:
            self.stats["skipped"] += 1
            return filepath

        logger.info("Downloading: %s", granule["name"])
        try:
            resp = self.session.get(granule["url"], timeout=300, stream=True)
            resp.raise_for_status()

            ctype = resp.headers.get("Content-Type", "")
            if "html" in ctype or "text" in ctype:
                reason = "Got HTML instead of NetCDF — check token"
                logger.error("%s: %s", granule["name"], reason)
                self.stats["failed"].append({"file": granule["name"], "reason": reason})
                return None

            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            with open(filepath, "rb") as f:
                magic = f.read(4)
            if magic[:3] not in (b"CDF", b"\x89HD"):
                reason = "Invalid file format (not NetCDF/HDF)"
                logger.error("%s: %s", granule["name"], reason)
                self.stats["failed"].append({"file": granule["name"], "reason": reason})
                filepath.unlink()
                return None

            self.stats["downloaded"] += 1
            logger.info("Saved: %s (%.1f MB)", granule["name"], filepath.stat().st_size / 1e6)
            return filepath

        except Exception as e:
            logger.error("Download failed %s: %s", granule["name"], e)
            self.stats["failed"].append({"file": granule["name"], "reason": str(e)})
            if filepath.exists():
                filepath.unlink()
            return None

    def download_all(self, granules: list[dict], overwrite: bool = False) -> list[Path]:
        self.stats["granules_found"] = len(granules)
        existing = sum(1 for g in granules if self._get_filepath(g["name"]).exists())
        to_download = len(granules) - existing if not overwrite else len(granules)
        logger.info("Granules: %d total, %d already exist, %d to download",
                     len(granules), existing, to_download)

        paths = []
        for g in granules:
            p = self.download(g, overwrite)
            if p:
                paths.append(p)

        logger.info("Done: %d files available", len(paths))
        return paths


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------
class VIIRSAODProcessor:
    def __init__(
        self,
        product_key: str,
        bbox: dict = None,
        stations: dict = None,
        qa_threshold: int = 2,
    ):
        self.level, self.product = get_product_config(product_key)
        self.product_key = product_key
        self.bbox = bbox or BBOX
        self.stations = stations or STATIONS
        self.qa_threshold = qa_threshold
        self.process_stats = {
            "files_total": 0,
            "files_processed": 0,
            "files_no_data": 0,
            "files_error": [],
        }

    def _read_variable(self, ds, var_name):
        """Find a variable in the dataset (top-level or in groups)."""
        if not var_name:
            return None
        if var_name in ds.variables:
            return ds.variables[var_name]
        for grp in ds.groups.values():
            if var_name in grp.variables:
                return grp.variables[var_name]
        return None

    def _to_float_array(self, nc_var):
        """Read full variable → float32 with fill/scale/offset handling."""
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

    def _read_subset_2d(self, ds, var_name, lat_slice, lon_slice):
        """Read only the [lat_slice, lon_slice] subset of a 2D variable.
        Returns float32 array or None."""
        v = self._read_variable(ds, var_name)
        if v is None:
            return None
        raw = v[lat_slice, lon_slice]
        if hasattr(raw, "mask"):
            arr = np.where(raw.mask, np.nan, raw.data.astype(np.float32))
        else:
            arr = raw.astype(np.float32)
            fill = getattr(v, "_FillValue", -999.0)
            arr[arr == fill] = np.nan
        scale = getattr(v, "scale_factor", None)
        offset = getattr(v, "add_offset", None)
        if scale is not None:
            arr = arr * float(scale)
        if offset is not None:
            arr = arr + float(offset)
        return arr

    def _haversine(self, lat, lon, slat, slon):
        dlat = np.radians(lat - slat)
        dlon = np.radians(lon - slon)
        a = (
            np.sin(dlat / 2) ** 2
            + np.cos(np.radians(slat)) * np.cos(np.radians(lat)) * np.sin(dlon / 2) ** 2
        )
        return 6371.0 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    def _parse_datetime_from_filename(self, filepath: Path) -> datetime:
        parts = filepath.stem.split(".")
        date_str = parts[1]  # e.g. A2026078
        year = int(date_str[1:5])
        doy = int(date_str[5:8])
        dt = datetime(year, 1, 1) + timedelta(days=doy - 1)
        # L2 has time (0612), D3 does not
        if len(parts) > 2 and len(parts[2]) == 4 and parts[2].isdigit():
            dt = dt.replace(hour=int(parts[2][:2]), minute=int(parts[2][2:]))
        return dt

    # -----------------------------------------------------------------------
    # L2 methods
    # -----------------------------------------------------------------------
    def read_granule_l2(self, filepath: Path) -> tuple[Optional[dict], Optional[str]]:
        """Returns (data, error_msg). error_msg is None for success or no-data-in-bbox."""
        try:
            ds = nc.Dataset(str(filepath), "r")
        except Exception as e:
            logger.error("Cannot open %s: %s", filepath.name, e)
            return None, f"Cannot open: {e}"

        try:
            lat_var = self._read_variable(ds, "Latitude") or self._read_variable(ds, "latitude")
            lon_var = self._read_variable(ds, "Longitude") or self._read_variable(ds, "longitude")
            if lat_var is None or lon_var is None:
                return None, "Missing Latitude/Longitude variables"

            lat = np.array(lat_var[:], dtype=np.float32)
            lon = np.array(lon_var[:], dtype=np.float32)

            aod_best_var = self._read_variable(ds, self.product["aod_variable"])
            aod_best = self._to_float_array(aod_best_var) if aod_best_var else None

            qa_land_var = self._read_variable(ds, self.product["qa_variable"])
            qa_ocean_var = self._read_variable(ds, self.product.get("qa_variable_ocean", ""))
            qa_land = np.array(qa_land_var[:], dtype=np.int16) if qa_land_var else None
            qa_ocean = np.array(qa_ocean_var[:], dtype=np.int16) if qa_ocean_var else None

            if qa_land is not None and qa_ocean is not None:
                qa = np.where(qa_land > 0, qa_land, qa_ocean)
                qa = np.where(qa > 0, qa, 0).astype(np.int8)
            elif qa_land is not None:
                qa = np.clip(qa_land, 0, 3).astype(np.int8)
            else:
                qa = np.zeros_like(lat, dtype=np.int8)

            mask = (
                (lat >= self.bbox["lat_min"]) & (lat <= self.bbox["lat_max"])
                & (lon >= self.bbox["lon_min"]) & (lon <= self.bbox["lon_max"])
            )
            if not np.any(mask):
                return None, None  # no data in bbox — not an error

            dt = self._parse_datetime_from_filename(filepath)

            result = {
                "filepath": str(filepath),
                "datetime": dt,
                "lat": lat[mask], "lon": lon[mask], "qa": qa[mask],
                "n_pixels_bbox": int(np.sum(mask)),
            }
            if aod_best is not None:
                result["aod_best"] = aod_best[mask]
                result["n_valid_best"] = int(np.sum(~np.isnan(aod_best[mask])))
            return result, None

        except Exception as e:
            logger.error("Error %s: %s", filepath.name, e)
            return None, str(e)
        finally:
            ds.close()

    def extract_station_l2(self, data: dict, threshold_km: float = 25.0) -> list[dict]:
        """Best-estimate radius averaging only: pixels within threshold_km of station
        with valid AOD and qa >= self.qa_threshold are averaged."""
        if data is None:
            return []

        lat, lon, qa = data["lat"], data["lon"], data["qa"]
        aod_best = data.get("aod_best")
        if aod_best is None:
            return []

        records = []
        for name, coords in self.stations.items():
            slat, slon = coords["lat"], coords["lon"]
            dist_km = self._haversine(lat, lon, slat, slon)
            in_radius = dist_km <= threshold_km
            if not np.any(in_radius):
                continue

            ab = aod_best[in_radius]
            qb = qa[in_radius]
            good = (~np.isnan(ab)) & (qb >= self.qa_threshold)
            if not np.any(good):
                continue

            af = ab[good]
            records.append({
                "datetime": data["datetime"],
                "station": name,
                "station_lat": slat,
                "station_lon": slon,
                "aod_mean": float(np.mean(af)),
                "aod_median": float(np.median(af)),
                "aod_std": float(np.std(af)),
                "aod_min": float(np.min(af)),
                "aod_max": float(np.max(af)),
                "n_pixels": int(np.sum(good)),
                "qa_threshold": self.qa_threshold,
                "threshold_km": threshold_km,
                "mean_dist_km": float(np.mean(dist_km[in_radius][good])),
                "source_sds": "Best_Estimate",
            })

        return records

    def process_l2(self, data_dir: Path, threshold_km: float = 25.0) -> pd.DataFrame:
        nc_files = sorted(data_dir.rglob("*.nc"))
        self.process_stats["files_total"] = len(nc_files)
        logger.info("L2: processing %d files from %s", len(nc_files), data_dir)

        all_records = []

        for i, f in enumerate(nc_files):
            granule, err = self.read_granule_l2(f)
            if err is not None:
                self.process_stats["files_error"].append({"file": f.name, "reason": err})
            elif granule is None:
                self.process_stats["files_no_data"] += 1
            else:
                self.process_stats["files_processed"] += 1
                all_records.extend(self.extract_station_l2(granule, threshold_km))

            del granule
            if (i + 1) % GC_EVERY == 0:
                gc.collect()
                logger.info("L2: %d/%d files, %d records",
                            i + 1, len(nc_files), len(all_records))

        gc.collect()
        df = self._to_df(all_records)
        logger.info("L2 best-estimate records: %d", len(df))
        return df

    # -----------------------------------------------------------------------
    # D3 methods  — OOM-fixed
    # -----------------------------------------------------------------------
    def read_granule_d3(self, filepath: Path) -> tuple[Optional[dict], Optional[str]]:
        """Returns (data, error_msg). error_msg is None for success or no-data-in-bbox."""
        try:
            ds = nc.Dataset(str(filepath), "r")
        except Exception as e:
            logger.error("Cannot open %s: %s", filepath.name, e)
            return None, f"Cannot open: {e}"

        try:
            lat_1d_var = self._read_variable(ds, "Latitude_1D")
            lon_1d_var = self._read_variable(ds, "Longitude_1D")

            if lat_1d_var is None or lon_1d_var is None:
                lat_2d_var = self._read_variable(ds, "Latitude") or self._read_variable(ds, "latitude")
                lon_2d_var = self._read_variable(ds, "Longitude") or self._read_variable(ds, "longitude")
                if lat_2d_var is None or lon_2d_var is None:
                    return None, "Missing Latitude/Longitude variables"
                lat_1d = np.array(lat_2d_var[:, 0], dtype=np.float32)
                lon_1d = np.array(lon_2d_var[0, :], dtype=np.float32)
            else:
                lat_1d = np.array(lat_1d_var[:], dtype=np.float32)
                lon_1d = np.array(lon_1d_var[:], dtype=np.float32)

            lat_idx = np.where(
                (lat_1d >= self.bbox["lat_min"]) & (lat_1d <= self.bbox["lat_max"])
            )[0]
            lon_idx = np.where(
                (lon_1d >= self.bbox["lon_min"]) & (lon_1d <= self.bbox["lon_max"])
            )[0]

            if len(lat_idx) == 0 or len(lon_idx) == 0:
                return None, None  # no data in bbox

            lon_sub, lat_sub = np.meshgrid(lon_1d[lon_idx], lat_1d[lat_idx])

            lat_sl = slice(int(lat_idx[0]), int(lat_idx[-1]) + 1)
            lon_sl = slice(int(lon_idx[0]), int(lon_idx[-1]) + 1)

            aod_mean = self._read_subset_2d(ds, self.product["aod_mean"], lat_sl, lon_sl)
            aod_count = self._read_subset_2d(ds, self.product["aod_count"], lat_sl, lon_sl)
            aod_min = self._read_subset_2d(ds, self.product["aod_min"], lat_sl, lon_sl)
            aod_max = self._read_subset_2d(ds, self.product["aod_max"], lat_sl, lon_sl)

            dt = self._parse_datetime_from_filename(filepath)

            return {
                "filepath": str(filepath),
                "datetime": dt,
                "lat": lat_sub.ravel(),
                "lon": lon_sub.ravel(),
                "aod_mean": aod_mean.ravel() if aod_mean is not None else None,
                "aod_count": aod_count.ravel() if aod_count is not None else None,
                "aod_min": aod_min.ravel() if aod_min is not None else None,
                "aod_max": aod_max.ravel() if aod_max is not None else None,
                "n_pixels_bbox": int(lat_sub.size),
                "n_valid": (int(np.sum(~np.isnan(aod_mean.ravel())))
                            if aod_mean is not None else 0),
            }, None
        except Exception as e:
            logger.error("Error %s: %s", filepath.name, e)
            return None, str(e)
        finally:
            ds.close()

    def extract_station_d3(self, data: dict) -> list[dict]:
        """D3: nearest grid cell to each station. D3 already QA-filtered by NASA."""
        if data is None or data.get("aod_mean") is None:
            return []

        lat, lon = data["lat"], data["lon"]
        aod_mean = data["aod_mean"]
        aod_count = data["aod_count"]
        aod_min_arr = data["aod_min"]
        aod_max_arr = data["aod_max"]

        records = []
        for name, coords in self.stations.items():
            slat, slon = coords["lat"], coords["lon"]
            dist = np.sqrt((lat - slat) ** 2 + (lon - slon) ** 2)
            idx = np.argmin(dist)

            if dist[idx] > 1.5:
                continue

            val = aod_mean[idx]
            if np.isnan(val):
                continue

            records.append({
                "datetime": data["datetime"],
                "station": name,
                "station_lat": slat,
                "station_lon": slon,
                "aod_mean": float(val),
                "aod_count": (float(aod_count[idx])
                              if aod_count is not None and not np.isnan(aod_count[idx])
                              else np.nan),
                "aod_min": (float(aod_min_arr[idx])
                            if aod_min_arr is not None and not np.isnan(aod_min_arr[idx])
                            else np.nan),
                "aod_max": (float(aod_max_arr[idx])
                            if aod_max_arr is not None and not np.isnan(aod_max_arr[idx])
                            else np.nan),
                "grid_lat": float(lat[idx]),
                "grid_lon": float(lon[idx]),
                "dist_deg": float(dist[idx]),
                "source": "D3_daily_1deg",
            })
        return records

    def process_d3(self, data_dir: Path) -> pd.DataFrame:
        nc_files = sorted(data_dir.rglob("*.nc"))
        self.process_stats["files_total"] = len(nc_files)
        logger.info("D3: processing %d files from %s", len(nc_files), data_dir)

        all_records = []
        for i, f in enumerate(nc_files):
            granule, err = self.read_granule_d3(f)
            if err is not None:
                self.process_stats["files_error"].append({"file": f.name, "reason": err})
            elif granule is None:
                self.process_stats["files_no_data"] += 1
            else:
                self.process_stats["files_processed"] += 1
                records = self.extract_station_d3(granule)
                all_records.extend(records)

            del granule
            if (i + 1) % GC_EVERY == 0:
                gc.collect()
                logger.info("D3: %d/%d files, %d records",
                            i + 1, len(nc_files), len(all_records))

        gc.collect()
        return self._to_df(all_records)

    # -----------------------------------------------------------------------
    # Shared
    # -----------------------------------------------------------------------
    def _to_df(self, records: list) -> pd.DataFrame:
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df["datetime"] = pd.to_datetime(df["datetime"]) + pd.Timedelta(hours=7)
        df.sort_values(["datetime", "station"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
import yaml

with open(f"{ROOT_DIR}/config/config.yaml", "r") as f:
    config = yaml.safe_load(f)
TOKEN = config["VIIRS"]["TOKEN"]

def run_viirs_aod(
    product: str = "deep_blue_noaa20",
    start: str = None,
    end: str = None,
    days: int = 1,
    base_dir: str = "/home/slow_data/Air_Quality/VIIRS",
    output_dir: str = None,
    threshold: float = 25.0,
    token: str = TOKEN,
    overwrite: bool = False,
    actions: list = None,
    qa_threshold: int = 2,
    bbox: dict = None,
    stations: dict = None,
):
    """
    Download & process VIIRS AOD.

    Folder structure:
        {base_dir}/L2/NOAA20/2022/*.nc
        {base_dir}/D3/SNPP/2023/*.nc
    """
    if actions is None:
        actions = ["download", "process"]
    if bbox is None:
        bbox = BBOX
    if stations is None:
        stations = STATIONS

    level, product_config = get_product_config(product)
    platform = product_config["platform"]
    short_name = product_config["short_name"]

    if output_dir is None:
        output_dir = os.path.join(base_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    # Resolve dates
    if start:
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d") if end else datetime.now()
    else:
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

    # Extract reads from {base_dir}/{level}/{short_name} (e.g. L2/AERDB_L2_VIIRS_NOAA20).
    # Download still writes to {base_dir}/{level}/{platform} via VIIRSAODDownloader.
    data_dir = Path(base_dir) / level / short_name

    run_stats = {
        "product": product,
        "level": level,
        "platform": platform,
        "start": start_date.strftime("%Y-%m-%d"),
        "end": end_date.strftime("%Y-%m-%d"),
        "download": None,
        "process": None,
        "station_summary": {},
    }

    # --- DOWNLOAD ---
    downloader = None
    if "download" in actions:
        searcher = CMRSearcher(product_key=product, bbox=bbox)
        granules = searcher.search(start_date, end_date)
        if not granules:
            logger.warning("No granules found.")
            run_stats["download"] = {"granules_found": 0, "skipped": 0, "downloaded": 0, "failed": []}
        else:
            downloader = VIIRSAODDownloader(
                token=token, output_dir=base_dir,
                level=level, platform=platform,
            )
            downloader.download_all(granules, overwrite=overwrite)
            run_stats["download"] = downloader.stats.copy()

    # --- PROCESS ---
    df = pd.DataFrame()

    if "process" in actions:
        processor = VIIRSAODProcessor(
            product_key=product, bbox=bbox,
            stations=stations, qa_threshold=qa_threshold,
        )

        if level == "L2":
            df = processor.process_l2(data_dir, threshold_km=threshold)
        elif level == "D3":
            df = processor.process_d3(data_dir)

        if not df.empty:
            level_dir = Path(output_dir) / f"{level}_{platform}"
            level_dir.mkdir(parents=True, exist_ok=True)
            for station in df["station"].unique():
                fname = (
                    station.replace(":", "")
                    .replace("/", "_")
                    .replace("\\", "_")
                    .replace(" ", "_")
                    .strip("_")
                )
                s_df = df[df["station"] == station].reset_index(drop=True)
                s_df.to_csv(level_dir / f"{fname}.csv", index=False)
            logger.info("Saved %d stations under %s", df["station"].nunique(), level_dir)

        run_stats["process"] = processor.process_stats.copy()

        # Build per-station summary
        if not df.empty and "station" in df.columns:
            for station, grp in df.groupby("station"):
                run_stats["station_summary"][station] = {
                    "records": len(grp),
                    "date_min": grp["datetime"].min().strftime("%Y-%m-%d"),
                    "date_max": grp["datetime"].max().strftime("%Y-%m-%d"),
                }

    return df, run_stats


timestamp = datetime.now().strftime("%Y%m%d")
JOB_LOG = Path(__file__).parent / f"logs/{timestamp}_job.log"

def write_job_report(
    all_run_stats: list[dict],
    stations: dict,
    log_path: Path = JOB_LOG,
):
    city_groups = group_stations_by_city(stations)
    lines = []
    w = lines.append

    w("=" * 70)
    w(f"VIIRS AOD Job Report — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    w("=" * 70)
    w("")

    # --- Station overview ---
    w(f"STATIONS: {len(stations)} stations in {len(city_groups)} cities")
    w("-" * 70)
    for city, members in sorted(city_groups.items()):
        w(f"  {city} ({len(members)} station(s)):")
        for name, info in members.items():
            w(f"    - [{info['lat']:.4f}, {info['lon']:.4f}] {name}")
    w("")

    # --- Per-product run ---
    for run in all_run_stats:
        product = run["product"]
        w(f"PRODUCT: {product}  ({run['level']}/{run['platform']})")
        w(f"  Period: {run['start']} → {run['end']}")
        w("-" * 70)

        # Download
        dl = run.get("download")
        if dl:
            w(f"  DOWNLOAD:")
            w(f"    Granules found:      {dl['granules_found']}")
            w(f"    Already exist (skip): {dl['skipped']}")
            w(f"    Downloaded:           {dl['downloaded']}")
            w(f"    Failed:               {len(dl['failed'])}")
            if dl["failed"]:
                for err in dl["failed"][:50]:
                    w(f"      ✗ {err['file']}")
                    w(f"        → {err['reason']}")
                if len(dl["failed"]) > 50:
                    w(f"      ... and {len(dl['failed']) - 50} more")
        else:
            w("  DOWNLOAD: skipped")
        w("")

        # Process
        pr = run.get("process")
        if pr:
            w(f"  PROCESS:")
            w(f"    Files total:     {pr['files_total']}")
            w(f"    Processed OK:    {pr['files_processed']}")
            w(f"    No data in bbox: {pr['files_no_data']}")
            w(f"    Errors:          {len(pr['files_error'])}")
            if pr["files_error"]:
                for err in pr["files_error"][:50]:
                    w(f"      ✗ {err['file']}")
                    w(f"        → {err['reason']}")
                if len(pr["files_error"]) > 50:
                    w(f"      ... and {len(pr['files_error']) - 50} more")
        else:
            w("  PROCESS: skipped")
        w("")

        # Station data summary
        ss = run.get("station_summary", {})
        if ss:
            w(f"  STATION DATA ({len(ss)} stations with data):")

            # Group by city
            city_station_stats = {}
            for sname, sinfo in ss.items():
                city = stations.get(sname, {}).get("city", "Unknown")
                if city not in city_station_stats:
                    city_station_stats[city] = []
                city_station_stats[city].append((sname, sinfo))

            for city in sorted(city_station_stats):
                members = city_station_stats[city]
                total_records = sum(s["records"] for _, s in members)
                date_min = min(s["date_min"] for _, s in members)
                date_max = max(s["date_max"] for _, s in members)
                w(f"    {city}: {total_records} records ({date_min} → {date_max})")
                for sname, sinfo in members:
                    short_name = sname.split(":")[-1].strip()[:40]
                    w(f"      - {short_name}: {sinfo['records']} records "
                      f"({sinfo['date_min']} → {sinfo['date_max']})")

            # Stations with NO data
            stations_with_data = set(ss.keys())
            stations_without = set(stations.keys()) - stations_with_data
            if stations_without:
                w(f"\n  NO DATA ({len(stations_without)} stations):")
                for sname in sorted(stations_without):
                    w(f"    - {sname}")
        else:
            w("  STATION DATA: no records extracted")

        w("")
        w("=" * 70)
        w("")

    report = "\n".join(lines)
    log_path.write_text(report, encoding="utf-8")
    logger.info("Job report written to %s", log_path)
    return report


# ===========================================================================
# RUN
# ===========================================================================
if __name__ == "__main__":
    city_groups = group_stations_by_city(STATIONS)
    logger.info("Loaded %d stations in %d cities", len(STATIONS), len(city_groups))
    for city, members in city_groups.items():
        logger.info("  %s: %d station(s)", city, len(members))

    all_stats = []
    for product_key in ["deep_blue_snpp"]:
        _, stats = run_viirs_aod(
            product=product_key,
            start="2020-01-01",
            base_dir="/home/slow_data/Air_Quality/VIIRS",
            actions=["download", "process"],
            threshold=25.0,
        )
        all_stats.append(stats)

    write_job_report(all_stats, STATIONS)
