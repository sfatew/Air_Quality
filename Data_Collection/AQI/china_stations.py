"""
CNEMC station coordinates — china_stations.py
=============================================
The quotsoft daily files (china_crawl.py) carry station codes only. The
official list (站点列表-2022.02.13起.xlsx, valid 2022-02-13 → now) is only on
Baidu Pan, so coordinates are rebuilt from two public CC-BY-4.0 Zenodo copies:

  * zenodo 10892980 Station_information.csv — older CNEMC list: code, names,
    city, lon/lat, elevation; ~1667 codes, none of the ~360 added in 2021.
  * zenodo 15680868 (Metair) model_input/metadata_final.csv — lon/lat of the
    2022.02.13 list in the original row order, but codes replaced by
    "chn_<lon>_<lat>" and 26 rows dropped.

Metair rows are matched to the 2022+ header codes (same order) with a monotone
alignment that may skip 26 codes. Cost of pairing a row with a code:
    1 - corr(hourly PM2.5 of code, nearest trusted station to the row)
where trusted = old codes whose two coordinate sources agree within 3 km.
Pure geometric alignment mis-places ~200 new codes (validated on 2025 data:
median neighbour corr 0.47 vs 0.87 here).

Output: <OUTPUT_DIR>/stations.csv
    station_code, lon, lat, coord_source, lon_zenodo, lat_zenodo, shift_km,
    corr_nn, name_cn, name_en, city_cn, city_en, elevation
lon/lat prefer the 2022 list; shift_km > ~5 flags likely relocations, where
pre-2022 data may belong to (lon_zenodo, lat_zenodo). corr_nn is the
alignment QA score (1.0 = the station is its own trusted neighbour).
"""

import csv
import gzip
import io
import logging
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from china_crawl import HEADERS, OUTPUT_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ZENODO_STATIONS_URL = (
    "https://zenodo.org/api/records/10892980/files/Station_information.csv/content")
METAIR_URL = "https://zenodo.org/api/records/15680868/files/model_input.zip/content"
CORR_YEAR = 2023
TRUST_KM = 3.0
NN_MAX_KM = 30.0
NEUTRAL_COST = 0.15


def fetch(url: str, dst: Path) -> Path:
    if dst.exists():
        return dst
    dst.parent.mkdir(parents=True, exist_ok=True)
    logger.info("downloading %s", url)
    r = requests.get(url, headers=HEADERS, timeout=600)
    r.raise_for_status()
    tmp = dst.with_name(dst.name + ".tmp")
    tmp.write_bytes(r.content)
    tmp.replace(dst)
    return dst


def haversine_km(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, (lon1, lat1, lon2, lat2))
    a = (np.sin((lat2 - lat1) / 2) ** 2
         + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2)
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


def latest_header_codes(raw_dir: Path) -> list[str]:
    latest = sorted(raw_dir.glob("*/china_sites_*.csv.gz"))[-1]
    with gzip.open(latest, "rt", encoding="utf-8") as fh:
        return list(dict.fromkeys(next(csv.reader(fh))[3:]))


def pm25_correlation(pm25_csv: Path) -> tuple[np.ndarray, dict[str, int]]:
    """Pairwise corr of log1p hourly PM2.5 (NaN treated as 0 after z-scoring)."""
    d = pd.read_csv(pm25_csv, dtype={"station_code": "category", "pm25": "float32"})
    w = d.pivot(index="time_bjt", columns="station_code", values="pm25")
    x = np.log1p(w.to_numpy(dtype="float64"))
    valid = (~np.isnan(x)).astype("float64")
    z = np.nan_to_num((x - np.nanmean(x, 0)) / np.nanstd(x, 0))
    corr = (z.T @ z) / np.maximum(valid.T @ valid, 1)
    return corr, {c: k for k, c in enumerate(w.columns)}


def monotone_align(n_rows: int, n_codes: int, cost) -> list[tuple[int, int]]:
    """Pair row i with code j, both in order, skipping exactly n_codes-n_rows codes."""
    k = n_codes - n_rows
    dp = np.full((n_rows + 1, k + 1), np.inf)
    dp[0, 0] = 0.0
    took_skip = np.zeros((n_rows + 1, k + 1), dtype=bool)
    for i in range(n_rows + 1):
        for s in range(k + 1):
            if not np.isfinite(dp[i, s]):
                continue
            j = i + s
            if s < k and dp[i, s] < dp[i, s + 1]:
                dp[i, s + 1] = dp[i, s]
                took_skip[i, s + 1] = True
            if i < n_rows and j < n_codes:
                c = dp[i, s] + cost(i, j)
                if c < dp[i + 1, s]:
                    dp[i + 1, s] = c
                    took_skip[i + 1, s] = False
    pairs, i, s = [], n_rows, k
    while i > 0 or s > 0:
        if took_skip[i, s]:
            s -= 1
        else:
            pairs.append((i - 1, i - 1 + s))
            i -= 1
    return pairs[::-1]


def build_stations(output_dir: Path = OUTPUT_DIR) -> pd.DataFrame:
    src = output_dir / "stations_src"
    zen = pd.read_csv(fetch(ZENODO_STATIONS_URL, src / "zenodo_Station_information.csv"),
                      encoding="gbk").drop_duplicates("Code").set_index("Code")
    with zipfile.ZipFile(fetch(METAIR_URL, src / "metair_model_input.zip")) as zf:
        with zf.open("model_input/metadata_final.csv") as f:
            met = pd.read_csv(io.TextIOWrapper(f, encoding="utf-8"),
                              usecols=["unique_id", "longitude", "latitude"])
    met = met[met["unique_id"].str.startswith("chn_")].reset_index(drop=True)
    mlon, mlat = met["longitude"].to_numpy(), met["latitude"].to_numpy()

    codes = latest_header_codes(output_dir / "raw")
    logger.info("header codes %d, Metair rows %d, Zenodo codes %d",
                len(codes), len(met), len(zen))
    in_zen = np.array([c in zen.index for c in codes])
    zlon = np.array([zen["Longitude"].get(c, np.nan) for c in codes])
    zlat = np.array([zen["Latitude"].get(c, np.nan) for c in codes])

    # pass 1: geometry only, to find old codes whose coordinates agree
    def geo_cost(i, j):
        if not in_zen[j]:
            return 0.0
        return min(haversine_km(mlon[i], mlat[i], zlon[j], zlat[j]), 50.0) / 50.0

    geo_pairs = monotone_align(len(met), len(codes), geo_cost)
    corr, cidx = pm25_correlation(output_dir / "pm25" / f"china_pm25_{CORR_YEAR}.csv.gz")
    trusted = [(i, codes[j]) for i, j in geo_pairs
               if in_zen[j] and codes[j] in cidx
               and haversine_km(mlon[i], mlat[i], zlon[j], zlat[j]) < TRUST_KM]
    t_lon = np.array([mlon[i] for i, _ in trusted])
    t_lat = np.array([mlat[i] for i, _ in trusted])
    t_col = np.array([cidx[c] for _, c in trusted])

    nn_col = np.empty(len(met), dtype=int)
    nn_km = np.empty(len(met))
    for i in range(len(met)):
        d = haversine_km(mlon[i], mlat[i], t_lon, t_lat)
        nn_col[i], nn_km[i] = t_col[d.argmin()], d.min()

    # pass 2: PM2.5 co-variation with the nearest trusted station
    def data_cost(i, j):
        if codes[j] in cidx and nn_km[i] < NN_MAX_KM:
            return 1.0 - corr[cidx[codes[j]], nn_col[i]]
        if in_zen[j]:
            return geo_cost(i, j)
        return NEUTRAL_COST

    pairs = monotone_align(len(met), len(codes), data_cost)
    aligned = pd.DataFrame({
        "station_code": [codes[j] for _, j in pairs],
        "lon": [mlon[i] for i, _ in pairs],
        "lat": [mlat[i] for i, _ in pairs],
        "corr_nn": [corr[cidx[codes[j]], nn_col[i]] if codes[j] in cidx else np.nan
                    for i, j in pairs],
    })
    aligned["coord_source"] = "cnemc_2022_list"

    all_codes = sorted(set(codes) | set(zen.index))
    out = pd.DataFrame({"station_code": all_codes}).merge(aligned, how="left")
    z = zen.rename(columns={"Longitude": "lon_zenodo", "Latitude": "lat_zenodo",
                            "Station_name_Chinese": "name_cn",
                            "Station_name_English": "name_en",
                            "City_Chinese": "city_cn", "City_English": "city_en",
                            "Elevation": "elevation"})
    out = out.merge(z, how="left", left_on="station_code", right_index=True)
    fallback = out["lon"].isna() & out["lon_zenodo"].notna()
    out.loc[fallback, ["lon", "lat"]] = out.loc[fallback, ["lon_zenodo", "lat_zenodo"]].values
    out.loc[fallback, "coord_source"] = "zenodo_10892980"
    out["shift_km"] = haversine_km(out["lon"], out["lat"], out["lon_zenodo"], out["lat_zenodo"])

    cols = ["station_code", "lon", "lat", "coord_source", "lon_zenodo", "lat_zenodo",
            "shift_km", "corr_nn", "name_cn", "name_en", "city_cn", "city_en", "elevation"]
    out = out[cols].round({"shift_km": 3, "corr_nn": 4})
    dst = output_dir / "stations.csv"
    out.to_csv(dst, index=False)
    logger.info("%d codes → %s  |  %s  |  no coords: %s", len(out), dst,
                out["coord_source"].value_counts(dropna=False).to_dict(),
                out.loc[out["lon"].isna(), "station_code"].tolist())
    return out


if __name__ == "__main__":
    build_stations()
