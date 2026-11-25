"""Shared helpers for the AQI data-quality notebook.

Imported via `%run _aqi_common.py` from `aqi.ipynb` so the notebook stays focused
on analysis. Provides:

- Paths, region constants, VN bbox / basemap
- Station coordinate lookup (master CSV `stationName` carries a colon that the
  AQI filenames drop — both forms are normalised before matching)
- Per-station loader returning Timestamp + pollutants only
- Network loader that concatenates every station tagged with region/lat/lon
- Time-key annotator (hour / month / season / year)
- Per-station null + coverage summary
- `save_fig(...)` that saves PNG AND keeps the figure open for inline display

Memory note: with ~108 valid stations × ~10 numeric columns the network frame
fits comfortably in RAM (~150 MB), so we do not stream.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# --------------------------------------------------------------------------- paths
AQI_DIR     = Path("/home/slow_data/Air_Quality/AQI/process")

STATION_MAP = Path("/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv")
FIG_DIR     = Path("/home/work1/projects/Air_Quality/pm25_map/features/aqi/figures")

# GADM Vietnam administrative boundaries (level 0 = country, 1 = province).
GADM_VN_L0 = Path("/home/work1/projects/Air_Quality/Data_Collection/GADM_Vietnam/gadm41_VNM_0.shp")
GADM_VN_L1 = Path("/home/work1/projects/Air_Quality/Data_Collection/GADM_Vietnam/gadm41_VNM_1.shp")

# --------------------------------------------------------------------------- pollutants
# Column names follow the process/ CSV schema: lowercase pm2.5 + uppercase
# gas names. PM1 / PM10 are not carried in process/ output.
PRIMARY = "pm2.5"
POLLUTANTS = ["pm2.5", "CO", "NOx", "O3", "SO2"]

# --------------------------------------------------------------------------- geometry
VN_BBOX = (101.5, 110.5, 8.0, 23.5)  # (lon_min, lon_max, lat_min, lat_max)

REGION_ORDER = ["NW", "NE", "RRD", "NCC", "SCC", "CH", "SE"]
REGION_FULL_NAMES = {
    "NW":  "North West",       "NE":  "North East",        "RRD": "Red River Delta",
    "NCC": "North Central Coast", "SCC": "South Central Coast",
    "CH":  "Central Highlands", "SE":  "South East",
}
REGION_COLORS = {
    "NW":  "#1f77b4", "NE":  "#9467bd", "RRD": "#2ca02c", "NCC": "#bcbd22",
    "SCC": "#ff7f0e", "CH":  "#8c564b", "SE":  "#d62728",
}

SEASON_OF_MONTH = {12: "DJF", 1: "DJF", 2: "DJF",
                   3: "MAM",  4: "MAM",  5: "MAM",
                   6: "JJA",  7: "JJA",  8: "JJA",
                   9: "SON", 10: "SON", 11: "SON"}
SEASON_ORDER = ["DJF", "MAM", "JJA", "SON"]


# --------------------------------------------------------------------------- station lookup
def _normalize(name: str) -> str:
    return " ".join(name.replace(":", "").split()).strip().lower()


def load_station_map() -> pd.DataFrame:
    """Master station map with normalised lookup key in `_key`."""
    df = pd.read_csv(STATION_MAP)
    df["_key"] = df["stationName"].map(_normalize)
    return df


def station_lookup() -> dict[str, dict]:
    s = load_station_map()
    return {r["_key"]: dict(stationName=r["stationName"],
                            lat=float(r["latitude"]),
                            lon=float(r["longitude"]),
                            region=r["region"])
            for _, r in s.iterrows()}


# --------------------------------------------------------------------------- file scan
def list_aqi_files(folder: Path = AQI_DIR) -> list[Path]:
    return sorted(p for p in folder.glob("*.csv") if p.is_file())


def load_aqi_station(path: Path,
                     pollutants: list[str] = POLLUTANTS) -> pd.DataFrame:
    """Read one processed station CSV, keep datetime + requested pollutants.

    Process CSV columns are `datetime, pm2.5, CO, NOx, O3, SO2`
    (output of `filter_aqi.py`). Pollutants not present become all-NaN
    columns so the network frame has a stable schema.
    """
    try:
        header = pd.read_csv(path, nrows=0).columns.tolist()
    except Exception:
        return pd.DataFrame()
    if "datetime" not in header:
        return pd.DataFrame()
    keep = ["datetime"] + [c for c in pollutants if c in header]
    df = pd.read_csv(path, usecols=keep, low_memory=False)
    df = df.rename(columns={"datetime": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    for c in pollutants:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["timestamp", *pollutants]].sort_values("timestamp").reset_index(drop=True)


# --------------------------------------------------------------------------- time keys
def add_time_keys(df: pd.DataFrame, time_col: str = "timestamp") -> pd.DataFrame:
    t = pd.to_datetime(df[time_col])
    out = df.copy()
    out["hour"]   = t.dt.hour.astype("int8")
    out["dow"]    = t.dt.dayofweek.astype("int8")
    out["month"]  = t.dt.month.astype("int8")
    out["year"]   = t.dt.year.astype("int16")
    out["season"] = out["month"].map(SEASON_OF_MONTH)
    return out


# --------------------------------------------------------------------------- network loader
def load_all_stations(pollutants: list[str] = POLLUTANTS,
                      *, drop_unknown_region: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (data, meta).

    `data` — one row per (station, timestamp) tagged with station/region/lat/lon
             and time keys; columns = pollutants + meta + time keys.
    `meta` — per-station summary used by the spatial / coverage plots.
    """
    lut = station_lookup()
    files = list_aqi_files()
    frames = []
    meta_rows = []
    for fp in files:
        key = _normalize(fp.stem)
        if key not in lut:
            if drop_unknown_region:
                continue
            info = dict(stationName=fp.stem, lat=np.nan, lon=np.nan, region="UNKNOWN")
        else:
            info = lut[key]
        df = load_aqi_station(fp, pollutants)
        if df.empty:
            continue
        df["station"] = info["stationName"]
        df["region"]  = info["region"]
        df["lat"]     = info["lat"]
        df["lon"]     = info["lon"]
        frames.append(df)

        valid = df[PRIMARY].notna()
        n_valid = int(valid.sum())
        start = df["timestamp"].min()
        end   = df["timestamp"].max()
        span_days = max(1, (end.normalize() - start.normalize()).days + 1)
        n_days = int(df.loc[valid, "timestamp"].dt.normalize().nunique())
        meta_rows.append(dict(
            file_stem=fp.stem, station=info["stationName"], region=info["region"],
            lat=info["lat"], lon=info["lon"],
            rows=len(df), valid_PM25=n_valid,
            start=start, end=end, n_days=n_days, span_days=span_days,
            day_coverage=n_days / span_days,
            mean_PM25=float(df[PRIMARY].dropna().mean())   if n_valid else np.nan,
            median_PM25=float(df[PRIMARY].dropna().median()) if n_valid else np.nan,
            p95_PM25=float(df[PRIMARY].dropna().quantile(0.95)) if n_valid else np.nan,
        ))
    if not frames:
        return pd.DataFrame(), pd.DataFrame()
    data = add_time_keys(pd.concat(frames, ignore_index=True))
    meta = pd.DataFrame(meta_rows)
    meta["region"] = pd.Categorical(meta["region"],
                                    categories=REGION_ORDER + ["UNKNOWN"])
    return data, meta


# --------------------------------------------------------------------------- null summary
def per_station_null_pct(data: pd.DataFrame,
                         pollutants: list[str] = POLLUTANTS) -> pd.DataFrame:
    """Return a station × pollutant null-percentage matrix (0–100)."""
    g = data.groupby("station", observed=True)
    out = pd.DataFrame({p: 100 * (1 - g[p].count() / g[p].size())
                        for p in pollutants})
    return out


# --------------------------------------------------------------------------- VN basemap
_VN_GDF_CACHE = {"l0": None, "l1": None}

def _load_vn_gdf(level: int = 1):
    key = f"l{level}"
    if _VN_GDF_CACHE[key] is None:
        import geopandas as gpd
        shp = GADM_VN_L0 if level == 0 else GADM_VN_L1
        _VN_GDF_CACHE[key] = gpd.read_file(shp)
    return _VN_GDF_CACHE[key]


def vn_basemap(ax=None, *, level: int = 1, figsize=(7, 11),
               edgecolor: str = "0.35", facecolor: str = "0.97",
               linewidth: float = 0.5):
    if ax is None:
        _, ax = plt.subplots(figsize=figsize)
    gdf = _load_vn_gdf(level=level)
    gdf.plot(ax=ax, color=facecolor, edgecolor=edgecolor, linewidth=linewidth)
    ax.set_xlim(VN_BBOX[0], VN_BBOX[1])
    ax.set_ylim(VN_BBOX[2], VN_BBOX[3])
    ax.set_xlabel("Longitude (°E)"); ax.set_ylabel("Latitude (°N)")
    ax.set_aspect("equal", adjustable="box")
    return ax


def plot_stations_on_vn(meta: pd.DataFrame, value_col: str, *, ax=None,
                        size_range=(15, 250), cmap="YlOrRd",
                        title: str | None = None, cbar_label: str | None = None):
    ax = vn_basemap(ax=ax)
    df = meta.dropna(subset=[value_col, "lat", "lon"]).copy()
    if df.empty:
        ax.set_title(title or value_col); return ax
    v = df[value_col].astype(float).values
    vmin, vmax = np.nanmin(v), np.nanmax(v)
    sizes = (np.full_like(v, np.mean(size_range)) if vmax == vmin
             else size_range[0] + (v - vmin) / (vmax - vmin) * (size_range[1] - size_range[0]))
    sc = ax.scatter(df["lon"], df["lat"], s=sizes, c=v, cmap=cmap,
                    edgecolor="black", linewidth=0.4, alpha=0.85)
    cbar = plt.colorbar(sc, ax=ax, shrink=0.6, pad=0.02)
    cbar.set_label(cbar_label or value_col)
    if title: ax.set_title(title)
    return ax


# --------------------------------------------------------------------------- CNEMC (China)
# Output of Data_Collection/AQI/china_crawl.py + china_stations.py. Values are as
# published by CNEMC (no QC here); timestamps are Beijing local time (UTC+8).
CNEMC_DIR = Path("/home/slow_data/Air_Quality/AQI/china")
CNEMC_YEARS = range(2020, 2027)
CNEMC_ZONE_ORDER = ["CN-S", "CN-C", "CN-N"]
CNEMC_ZONE_NAMES = {"CN-S": "Nam TQ (< 26°N)", "CN-C": "Trung TQ (26–34°N)",
                    "CN-N": "Bắc TQ (≥ 34°N)"}


def cnemc_zone(lat: pd.Series) -> pd.Series:
    return pd.Series(np.select([lat < 26, lat < 34], ["CN-S", "CN-C"], "CN-N"),
                     index=lat.index).where(lat.notna())


def load_cnemc_stations() -> pd.DataFrame:
    st = pd.read_csv(CNEMC_DIR / "stations.csv")
    st["zone"] = cnemc_zone(st["lat"])
    return st


def load_cnemc_pm25(years=CNEMC_YEARS) -> pd.DataFrame:
    """Hourly CNEMC PM2.5, long: timestamp (UTC+8), station, zone, pm2.5, hour/month/year.

    ~95M rows for 2020–2026 (~2 GB); zone is NaN for the 16 codes without coordinates.
    """
    st = load_cnemc_stations()
    codes = pd.CategoricalDtype(st["station_code"])
    frames = []
    for y in years:
        df = pd.read_csv(CNEMC_DIR / "pm25" / f"china_pm25_{y}.csv.gz",
                         dtype={"station_code": codes, "pm25": "float32"})
        df["time_bjt"] = pd.to_datetime(df["time_bjt"], format="%Y-%m-%d %H:%M:%S")
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    df = df.rename(columns={"time_bjt": "timestamp", "station_code": "station",
                            "pm25": PRIMARY})
    # map through category codes: a plain .map would build a 95M-element object column
    zone_codes = pd.Categorical(st["zone"], categories=CNEMC_ZONE_ORDER).codes
    df["zone"] = pd.Categorical.from_codes(zone_codes[df["station"].cat.codes],
                                           categories=CNEMC_ZONE_ORDER)
    t = df["timestamp"].dt
    df["hour"] = t.hour.astype("int8")
    df["month"] = t.month.astype("int8")
    df["year"] = t.year.astype("int16")
    return df


# --------------------------------------------------------------------------- figure I/O
def ensure_fig_dir() -> Path:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    return FIG_DIR


def save_fig(name: str, fig=None, *, dpi: int = 140) -> Path:
    """Save PNG to FIG_DIR/<name>.png. Does NOT close — lets the figure render inline."""
    ensure_fig_dir()
    f = fig if fig is not None else plt.gcf()
    out = FIG_DIR / f"{name}.png"
    f.savefig(out, dpi=dpi, bbox_inches="tight")
    return out


# --------------------------------------------------------------------------- defaults
sns.set_theme(style="whitegrid", context="notebook")
plt.rcParams["figure.dpi"] = 110
plt.rcParams["savefig.bbox"] = "tight"
