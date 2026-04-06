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
            "aod_variable": "Aerosol_Optical_Thickness_550_Land_Ocean_Best_Estimate",
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
    def read_granule_l2(self, filepath: Path) -> Optional[dict]:
        try:
            ds = nc.Dataset(str(filepath), "r")
        except Exception as e:
            logger.error("Cannot open %s: %s", filepath.name, e)
            return None

        try:
            lat_var = self._read_variable(ds, "Latitude") or self._read_variable(ds, "latitude")
            lon_var = self._read_variable(ds, "Longitude") or self._read_variable(ds, "longitude")
            if lat_var is None or lon_var is None:
                return None

            lat = np.array(lat_var[:], dtype=np.float32)
            lon = np.array(lon_var[:], dtype=np.float32)

            aod_best_var = self._read_variable(ds, self.product["aod_variable"])
            aod_all_var = self._read_variable(ds, self.product["aod_all_variable"])
            aod_best = self._to_float_array(aod_best_var) if aod_best_var else None
            aod_all = self._to_float_array(aod_all_var) if aod_all_var else None

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
                return None

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
            if aod_all is not None:
                result["aod_all"] = aod_all[mask]
                result["n_valid_all"] = int(np.sum(~np.isnan(aod_all[mask])))
            return result

        except Exception as e:
            logger.error("Error %s: %s", filepath.name, e)
            return None
        finally:
            ds.close()

    def extract_station_l2(self, data: dict, radius_km: float = 25.0) -> tuple[list[dict], list[dict]]:
        if data is None:
            return [], []

        lat, lon, qa = data["lat"], data["lon"], data["qa"]
        aod_best = data.get("aod_best")
        aod_all = data.get("aod_all")
        raw_records, filt_records = [], []

        for name, coords in self.stations.items():
            slat, slon = coords["lat"], coords["lon"]
            dist_km = self._haversine(lat, lon, slat, slon)
            in_radius = dist_km <= radius_km
            base = {"datetime": data["datetime"], "station": name,
                    "station_lat": slat, "station_lon": slon}

            # RAW
            if aod_all is not None and np.any(in_radius):
                a = aod_all[in_radius]
                q = qa[in_radius]
                has_valid = np.any(~np.isnan(a))
                qa_u, qa_c = np.unique(q, return_counts=True)
                qa_dist = dict(zip(qa_u.tolist(), qa_c.tolist()))

                raw_records.append({
                    **base,
                    "aod_mean": float(np.nanmean(a)) if has_valid else np.nan,
                    "aod_median": float(np.nanmedian(a)) if has_valid else np.nan,
                    "aod_std": float(np.nanstd(a)) if has_valid else np.nan,
                    "aod_min": float(np.nanmin(a)) if has_valid else np.nan,
                    "aod_max": float(np.nanmax(a)) if has_valid else np.nan,
                    "n_pixels_total": int(np.sum(in_radius)),
                    "n_pixels_valid_aod": int(np.sum(~np.isnan(a))),
                    "n_pixels_nan": int(np.sum(np.isnan(a))),
                    "n_qa0_no_retrieval": int(qa_dist.get(0, 0)),
                    "n_qa1_low": int(qa_dist.get(1, 0)),
                    "n_qa2_medium": int(qa_dist.get(2, 0)),
                    "n_qa3_high": int(qa_dist.get(3, 0)),
                    "qa_distribution": str(qa_dist),
                    "mean_dist_km": float(np.mean(dist_km[in_radius])),
                    "source_sds": "Land_Ocean",
                })

            # FILTERED
            if aod_best is not None and np.any(in_radius):
                ab = aod_best[in_radius]
                qb = qa[in_radius]
                good = (~np.isnan(ab)) & (qb >= self.qa_threshold)
                if np.any(good):
                    af = ab[good]
                    filt_records.append({
                        **base,
                        "aod_mean": float(np.nanmean(af)),
                        "aod_median": float(np.nanmedian(af)),
                        "aod_std": float(np.nanstd(af)),
                        "aod_min": float(np.nanmin(af)),
                        "aod_max": float(np.nanmax(af)),
                        "n_pixels": int(np.sum(good)),
                        "qa_threshold": self.qa_threshold,
                        "mean_dist_km": float(np.mean(dist_km[in_radius][good])),
                        "source_sds": "Best_Estimate",
                    })

        return raw_records, filt_records

    def extract_station_l2_nearest(self, data: dict) -> tuple[list[dict], list[dict]]:
        """Nearest pixel method."""
        if data is None:
            return [], []

        lat, lon, qa = data["lat"], data["lon"], data["qa"]
        aod_best = data.get("aod_best")
        aod_all = data.get("aod_all")
        raw_records, filt_records = [], []

        for name, coords in self.stations.items():
            slat, slon = coords["lat"], coords["lon"]
            dist_km = self._haversine(lat, lon, slat, slon)
            nearest_idx = np.argmin(dist_km)
            nearest_dist = float(dist_km[nearest_idx])

            if nearest_dist > 6.0:
                continue

            base = {
                "datetime": data["datetime"],
                "station": name,
                "station_lat": slat,
                "station_lon": slon,
                "pixel_lat": float(lat[nearest_idx]),
                "pixel_lon": float(lon[nearest_idx]),
                "dist_km": nearest_dist,
            }

            if aod_all is not None:
                aod_val = float(aod_all[nearest_idx])
                qa_val = int(qa[nearest_idx])
                raw_records.append({
                    **base,
                    "aod": aod_val if not np.isnan(aod_val) else np.nan,
                    "qa": qa_val,
                    "source_sds": "Land_Ocean",
                })

            if aod_best is not None:
                aod_val = float(aod_best[nearest_idx])
                qa_val = int(qa[nearest_idx])
                if not np.isnan(aod_val) and qa_val >= self.qa_threshold:
                    filt_records.append({
                        **base,
                        "aod": aod_val,
                        "qa": qa_val,
                        "qa_threshold": self.qa_threshold,
                        "source_sds": "Best_Estimate",
                    })

        return raw_records, filt_records

    def process_l2(self, data_dir: Path, radius_km: float = 25.0) -> dict:
        nc_files = sorted(data_dir.rglob("*.nc"))
        logger.info("L2: processing %d files from %s", len(nc_files), data_dir)

        radius_raw, radius_filt = [], []
        nearest_raw, nearest_filt = [], []

        for i, f in enumerate(nc_files):
            granule = self.read_granule_l2(f)
            if granule is not None:
                r_raw, r_filt = self.extract_station_l2(granule, radius_km)
                radius_raw.extend(r_raw)
                radius_filt.extend(r_filt)

                n_raw, n_filt = self.extract_station_l2_nearest(granule)
                nearest_raw.extend(n_raw)
                nearest_filt.extend(n_filt)

            # --- OOM fix: free memory ---
            del granule
            if (i + 1) % GC_EVERY == 0:
                gc.collect()
                logger.info(
                    "L2: %d/%d files (radius_raw=%d, nearest_raw=%d)",
                    i + 1, len(nc_files), len(radius_raw), len(nearest_raw),
                )

        gc.collect()

        result = {
            "radius_raw": self._to_df(radius_raw),
            "radius_filtered": self._to_df(radius_filt),
            "nearest_raw": self._to_df(nearest_raw),
            "nearest_filtered": self._to_df(nearest_filt),
        }

        logger.info("Radius — raw: %d | filtered: %d",
                     len(result["radius_raw"]), len(result["radius_filtered"]))
        logger.info("Nearest — raw: %d | filtered: %d",
                     len(result["nearest_raw"]), len(result["nearest_filtered"]))

        return result

    # -----------------------------------------------------------------------
    # D3 methods  — OOM-fixed
    # -----------------------------------------------------------------------
    def read_granule_d3(self, filepath: Path) -> Optional[dict]:
        """
        Read a D3 granule, extracting ONLY the bbox subset.

        KEY FIX: Use 'Latitude_1D' / 'Longitude_1D' (shape 180 / 360)
        instead of 'Latitude' / 'Longitude' (shape 180x360).
        Then slice AOD variables with [lat_slice, lon_slice] to read
        only the ~2 cells we need, not the entire global grid.
        """
        try:
            ds = nc.Dataset(str(filepath), "r")
        except Exception as e:
            logger.error("Cannot open %s: %s", filepath.name, e)
            return None

        try:
            # --- FIX: prefer 1D coordinate variables ---
            lat_1d_var = self._read_variable(ds, "Latitude_1D")
            lon_1d_var = self._read_variable(ds, "Longitude_1D")

            # Fallback to 2D if 1D not available (shouldn't happen for D3)
            if lat_1d_var is None or lon_1d_var is None:
                lat_2d_var = self._read_variable(ds, "Latitude") or self._read_variable(ds, "latitude")
                lon_2d_var = self._read_variable(ds, "Longitude") or self._read_variable(ds, "longitude")
                if lat_2d_var is None or lon_2d_var is None:
                    return None
                # Extract 1D from 2D: first column for lat, first row for lon
                lat_1d = np.array(lat_2d_var[:, 0], dtype=np.float32)
                lon_1d = np.array(lon_2d_var[0, :], dtype=np.float32)
            else:
                lat_1d = np.array(lat_1d_var[:], dtype=np.float32)
                lon_1d = np.array(lon_1d_var[:], dtype=np.float32)

            # Filter to bbox indices
            lat_idx = np.where(
                (lat_1d >= self.bbox["lat_min"]) & (lat_1d <= self.bbox["lat_max"])
            )[0]
            lon_idx = np.where(
                (lon_1d >= self.bbox["lon_min"]) & (lon_1d <= self.bbox["lon_max"])
            )[0]

            if len(lat_idx) == 0 or len(lon_idx) == 0:
                return None

            # Build small meshgrid of only the bbox cells
            lon_sub, lat_sub = np.meshgrid(lon_1d[lon_idx], lat_1d[lat_idx])

            # Read ONLY the bbox subset from NetCDF (not the full 180x360!)
            lat_sl = slice(int(lat_idx[0]), int(lat_idx[-1]) + 1)
            lon_sl = slice(int(lon_idx[0]), int(lon_idx[-1]) + 1)

            aod_mean = self._read_subset_2d(ds, self.product["aod_mean"], lat_sl, lon_sl)
            aod_count = self._read_subset_2d(ds, self.product["aod_count"], lat_sl, lon_sl)
            aod_min = self._read_subset_2d(ds, self.product["aod_min"], lat_sl, lon_sl)
            aod_max = self._read_subset_2d(ds, self.product["aod_max"], lat_sl, lon_sl)

            dt = self._parse_datetime_from_filename(filepath)

            # Flatten — extract_station_d3 expects 1-D arrays
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
            }
        except Exception as e:
            logger.error("Error %s: %s", filepath.name, e)
            return None
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
        logger.info("D3: processing %d files from %s", len(nc_files), data_dir)

        all_records = []
        for i, f in enumerate(nc_files):
            granule = self.read_granule_d3(f)
            records = self.extract_station_d3(granule)
            all_records.extend(records)

            # --- OOM fix: free memory ---
            del granule, records
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
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.sort_values(["datetime", "station"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
import yaml

with open("config.yaml", "r") as f:
    data = yaml.safe_load(f)

TOKEN = data['token']


def run_viirs_aod(
    product: str = "deep_blue_noaa20",
    start: str = None,
    end: str = None,
    days: int = 1,
    base_dir: str = "/home/slow_data/Air_Quality/VIIRS",
    output_dir: str = None,
    radius: float = 25.0,
    token: str = TOKEN,
    overwrite: bool = False,
    actions: list = None,
    qa_threshold: int = 2,
):
    """
    Download & process VIIRS AOD.

    Folder structure:
        {base_dir}/L2/NOAA20/2022/*.nc
        {base_dir}/D3/SNPP/2023/*.nc
    """
    if actions is None:
        actions = ["download", "process"]

    level, product_config = get_product_config(product)
    platform = product_config["platform"]

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

    data_dir = Path(base_dir) / level / platform

    # --- DOWNLOAD ---
    if "download" in actions:
        searcher = CMRSearcher(product_key=product, bbox=BBOX)
        granules = searcher.search(start_date, end_date)
        if not granules:
            logger.warning("No granules found.")
        else:
            downloader = VIIRSAODDownloader(
                token=token, output_dir=base_dir,
                level=level, platform=platform,
            )
            downloader.download_all(granules, overwrite=overwrite)

    # --- PROCESS ---
    df_raw, df_filt = pd.DataFrame(), pd.DataFrame()
    prefix = f"viirs_aod_{level}_{platform}"

    if "process" in actions:
        processor = VIIRSAODProcessor(
            product_key=product, bbox=BBOX,
            stations=STATIONS, qa_threshold=qa_threshold,
        )

        if level == "L2":
            results = processor.process_l2(data_dir, radius_km=radius)

            methods = {
                "radius_avg": ("radius_raw", "radius_filtered"),
                "nearest_pixel": ("nearest_raw", "nearest_filtered"),
            }

            for method_name, (raw_key, filt_key) in methods.items():
                df_r = results[raw_key]
                df_f = results[filt_key]

                if df_r.empty and df_f.empty:
                    continue

                stations_in_data = set()
                if not df_r.empty:
                    stations_in_data.update(df_r["station"].unique())
                if not df_f.empty:
                    stations_in_data.update(df_f["station"].unique())

                for station in stations_in_data:
                    folder_name = (
                        station.replace(":", "")
                        .replace("/", "_")
                        .replace("\\", "_")
                        .replace(" ", "_")
                        .strip("_")
                    )
                    station_dir = (Path(output_dir) / f"{level}_{platform}"
                                   / method_name / folder_name)
                    station_dir.mkdir(parents=True, exist_ok=True)

                    if not df_r.empty:
                        s_raw = df_r[df_r["station"] == station].reset_index(drop=True)
                        if not s_raw.empty:
                            s_raw.to_csv(station_dir / "raw.csv", index=False)

                    if not df_f.empty:
                        s_filt = df_f[df_f["station"] == station].reset_index(drop=True)
                        if not s_filt.empty:
                            s_filt.to_csv(station_dir / "filtered.csv", index=False)

                logger.info("Saved %s: %d stations", method_name, len(stations_in_data))

            for key, df in results.items():
                if not df.empty:
                    csv_path = os.path.join(output_dir, f"{prefix}_{key}.csv")
                    df.to_csv(csv_path, index=False)
                    logger.info("Saved: %s (%d records)", csv_path, len(df))

            df_raw = results["radius_raw"]
            df_filt = results["radius_filtered"]

        elif level == "D3":
            df_raw = processor.process_d3(data_dir)
            df_filt = df_raw.copy()

            if not df_raw.empty:
                # Save per station
                for station in df_raw["station"].unique():
                    folder_name = (
                        station.replace(":", "")
                        .replace("/", "_")
                        .replace("\\", "_")
                        .replace(" ", "_")
                        .strip("_")
                    )
                    station_dir = Path(output_dir) / f"{level}_{platform}" / folder_name
                    station_dir.mkdir(parents=True, exist_ok=True)
                    s_df = df_raw[df_raw["station"] == station].reset_index(drop=True)
                    s_df.to_csv(station_dir / "data.csv", index=False)

                # Combined
                d3_csv = os.path.join(output_dir, f"{prefix}.csv")
                df_raw.to_csv(d3_csv, index=False)
                logger.info("Saved: %s (%d records)", d3_csv, len(df_raw))

    return df_raw, df_filt


# ===========================================================================
# RUN
# ===========================================================================

# D3 NOAA20
run_viirs_aod(
    product="deep_blue_noaa20_d3",
    start="2022-01-01",
    base_dir="/home/slow_data/Air_Quality/VIIRS",
    actions=["process"],
)

# D3 SNPP
run_viirs_aod(
    product="deep_blue_snpp_d3",
    start="2022-01-01",
    base_dir="/home/slow_data/Air_Quality/VIIRS",
    actions=["process"],
)