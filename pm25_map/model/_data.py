"""Khung ô–giờ cho mô hình PM2.5 trên lưới 0,05° — dùng chung cho global LightGBM và GTW-Tree (chuyển sang
theo giờ 29/10/2026; bản theo ngày đã bỏ).

    y   pm25           PM2.5 1 giờ của ô = TB các trạm trong ô (AQI/process/aqi_pm25_grid_hourly.pkl,
                       `map_aqi_grid.py --hourly`). Nhãn Envisoft là giờ KẾT THÚC: 10:00 = TB [09:00, 10:00).
    x   aod            Stage B RF (Stage_B_hourly/output/rf/YYYY/MM/DD/aod_YYYYMMDD_HHMM.nc), `aod_550nm`
                       của ô: Stage A ở ô có quan trắc, RF lấp ở ô còn lại. Kèm `aod_observed` (0/1) và
                       `aod_unc` (SD giữa các cây, NaN ở ô quan trắc) để phân tích — không nằm trong X_VARS.
        ERA5 (11)      ERA5/_monthly_raw/era5_YYYYMM.nc tại HH:00 UTC, bilinear 0,25° → tâm ô.
                       SolarRad, Precip là tích luỹ 1 giờ [HH−1, HH) UTC.
        elev, elev_std DEM Copernicus GLO-30 (tĩnh)
        cams_pm25      CAMS PM2.5 3 giờ/lần: bilinear từng mốc rồi nội suy tuyến tính theo thời gian về HH:00
                       UTC. `cams_src` 1 = EAC4 (0,75°), 2 = forecast (0,4°, từ 2026-01), 3 = hai mốc kề khác nguồn.
    st  9 biến         chỉ cho bản global "-st": 6 khoảng cách haversine tới góc / giữa cạnh VN_BBOX,
                       pt1/pt2 = cos/sin ngày trong năm, pt3 = năm chuẩn hoá.

Ghép giờ (chọn 29/10/2026): slot AOD HH:00 UTC (Stage A gom quan trắc ±30′ quanh HH:00) ↔ PM nhãn
(HH+7):00 giờ địa phương, tức khoảng đo [HH−1, HH) UTC. Chỉ giữ giờ có file RF (ban ngày, UTC 00–10 ≈
07–17 giờ địa phương) và có PM2.5; bỏ dòng thiếu ERA5 hoặc CAMS. Khoá thời gian: `t_hour` = số giờ từ DAY0
(UTC), `day` = ngày địa phương kể từ DAY0.

Hai kiểu kiểm định (dùng chung cho mọi mô hình, `outer_folds(df, cv)`):
    site    5 fold theo ô (StratifiedGroupKFold theo vùng, data/cv_folds_hourly.csv) — mỗi ô vào test đúng 1 lần
    sample  5 fold ngẫu nhiên theo ô–giờ (KFold xáo trộn, seed RANDOM_STATE, theo thứ tự khung)

    python _data.py              # dựng data/frame_hourly.pkl + fold — cần bản đồ RF của Stage B đã chạy xong
    python _data.py --check      # + đối chiếu PM2.5 giờ với bảng ngày của map_aqi_grid, thống kê nhanh
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pandas as pd
import xarray as xr
from sklearn.model_selection import KFold, StratifiedGroupKFold

MODEL_DIR = Path(__file__).resolve().parent
DATA_DIR = MODEL_DIR / "data"          # dữ liệu DÙNG CHUNG cho mọi mô hình (khung + fold)
FRAME_PKL = DATA_DIR / "frame_hourly.pkl"
FOLDS_CSV = DATA_DIR / "cv_folds_hourly.csv"

SLOW = Path("/home/slow_data/Air_Quality")
PM_HOURLY = SLOW / "AQI/process/aqi_pm25_grid_hourly.pkl"
PM_DAILY = SLOW / "AQI/process/aqi_pm25_grid_daily.pkl"          # chỉ để đối chiếu trong check()
CELLS_FILE = SLOW / "AQI/process/aqi_pm25_grid_cells.csv"
RF_DIR = SLOW / "Stage_B_hourly/output/rf"
ERA5_RAW = SLOW / "ERA5/_monthly_raw"
CAMS_RAW = SLOW / "CAM/pm25/_monthly_raw"
DEM_GRID = SLOW / "DEM/grid_005/dem_005.nc"

PM = "pm25"
ERA5_VARS = ["T2m", "RH", "Psfc", "U10", "V10", "PBLH",          # gió ở dạng U10/V10 (chọn 24/10/2026)
             "CloudCover", "TCWV", "SolarRad", "Precip", "CAPE"]
DEM_VARS = ["elev", "elev_std"]
X_VARS = ["aod", *ERA5_VARS, *DEM_VARS, "cams_pm25"]
SPATIAL_VARS = ["d_corner_SW", "d_corner_NW", "d_corner_SE", "d_corner_NE", "d_mid_W", "d_mid_E"]
TEMPORAL_VARS = ["pt1", "pt2", "pt3"]
ST_VARS = SPATIAL_VARS + TEMPORAL_VARS
HOUR_VARS = ["hour_pt1", "hour_pt2"]
REGION_ORDER = ["NW", "NE", "RRD", "NCC", "SCC", "CH", "SE"]
SEASON_OF_MONTH = {12: "DJF", 1: "DJF", 2: "DJF", 3: "MAM", 4: "MAM", 5: "MAM",
                   6: "JJA", 7: "JJA", 8: "JJA", 9: "SON", 10: "SON", 11: "SON"}
CV_KINDS = ("site", "sample")
N_OUTER = 5
RANDOM_STATE = 0
DAY0 = pd.Timestamp("2020-01-01")

TZ = pd.Timedelta(hours=7)
CAMS_STEP = pd.Timedelta(hours=3)
CAMS_SRC = {"cams-global-reanalysis-eac4": 1, "cams-global-atmospheric-composition-forecasts": 2}
_RF_NAME = re.compile(r"aod_(\d{8})_(\d{4})\.nc$")

# Giống train_v3: khung (lon_min, lon_max, lat_min, lat_max) và các điểm tham chiếu (lat, lon)
VN_BBOX = (101.5, 110.5, 8.0, 23.5)
_LAT_MID = 0.5 * (VN_BBOX[2] + VN_BBOX[3])
REF_POINTS = {
    "d_corner_SW": (VN_BBOX[2], VN_BBOX[0]), "d_corner_NW": (VN_BBOX[3], VN_BBOX[0]),
    "d_corner_SE": (VN_BBOX[2], VN_BBOX[1]), "d_corner_NE": (VN_BBOX[3], VN_BBOX[1]),
    "d_mid_W": (_LAT_MID, VN_BBOX[0]), "d_mid_E": (_LAT_MID, VN_BBOX[1]),
}
YEAR_MIN, YEAR_MAX = 2020, 2026


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = (np.sin((p2 - p1) / 2) ** 2
         + np.cos(p1) * np.cos(p2) * np.sin(np.radians(np.asarray(lon2) - np.asarray(lon1)) / 2) ** 2)
    return 2 * R * np.arcsin(np.sqrt(a))


def _add_st(df: pd.DataFrame) -> pd.DataFrame:
    for col, (rlat, rlon) in REF_POINTS.items():
        df[col] = haversine_km(df.cell_lat.values, df.cell_lon.values, rlat, rlon).astype("float32")
    doy = df.date.dt.dayofyear.values.astype("float32")
    T = np.where(df.date.dt.is_leap_year.values, 366.0, 365.0)
    df["pt1"] = np.cos(2 * np.pi * doy / T).astype("float32")
    df["pt2"] = np.sin(2 * np.pi * doy / T).astype("float32")
    df["pt3"] = ((df.year.values - YEAR_MIN) / (YEAR_MAX - YEAR_MIN)).astype("float32")
    return df


def _add_hour(df: pd.DataFrame) -> pd.DataFrame:
    """hour_pt1/hour_pt2 = cos/sin(2π·hour_lt/24); hour_lt is the local hour ENDING the 1-h mean."""
    a = 2 * np.pi * df.hour_lt.to_numpy(np.float32) / 24
    df["hour_pt1"], df["hour_pt2"] = np.cos(a).astype("float32"), np.sin(a).astype("float32")
    return df


# ── PM2.5 và danh sách giờ ──────────────────────────────────────────────────────
def rf_slots(rf_dir: Path = RF_DIR) -> pd.DatetimeIndex:
    """Các slot UTC có file bản đồ AOD RF."""
    t = [pd.Timestamp(f"{m[1]} {m[2]}") for p in rf_dir.rglob("aod_*.nc") if (m := _RF_NAME.search(p.name))]
    return pd.DatetimeIndex(sorted(t), name="time_utc")


def load_pm_hourly(slots: pd.DatetimeIndex) -> pd.DataFrame:
    """PM2.5 ô–giờ tại các slot `slots` (UTC) + thuộc tính ô và khoá thời gian."""
    pm = pd.read_pickle(PM_HOURLY)
    pm["time_utc"] = pm.time_lt - TZ                     # nhãn (HH+7):00 LT ↔ slot HH:00 UTC
    pm = pm[pm.time_utc.isin(slots)]
    cells = pd.read_csv(CELLS_FILE)[["cell_id", "region", "province", "sensor_mix", "n_stations_total"]]
    pm = pm.merge(cells, on="cell_id", how="left")
    pm["date"] = pm.time_lt.dt.normalize()               # ngày địa phương; slot 00–10 UTC luôn cùng ngày
    pm["hour_lt"] = pm.time_lt.dt.hour.astype("int8")
    pm["year"], pm["month"] = pm.date.dt.year, pm.date.dt.month
    pm["season"] = pm.month.map(SEASON_OF_MONTH)
    return pm.sort_values(["time_utc", "cell_id"]).reset_index(drop=True)


# ── AOD Stage B RF ──────────────────────────────────────────────────────────────
def _rf_path(t: pd.Timestamp, rf_dir: Path = RF_DIR) -> Path:
    return rf_dir / f"{t:%Y/%m/%d}" / f"aod_{t:%Y%m%d_%H%M}.nc"


def add_aod(df: pd.DataFrame, rf_dir: Path = RF_DIR) -> pd.DataFrame:
    """`aod`, `aod_observed`, `aod_unc` của ô tại slot, đọc từ bản đồ RF (một file mỗi slot)."""
    out = df.copy()
    aod = np.full(len(df), np.nan)
    obs = np.zeros(len(df), np.int8)
    unc = np.full(len(df), np.nan)
    for t, idx in df.groupby("time_utc").indices.items():
        with nc.Dataset(_rf_path(t, rf_dir)) as ds:
            ds.set_auto_mask(False)
            r, c = df.row.values[idx], df.col.values[idx]
            a = ds["aod_550nm"][:][r, c].astype(np.float64)
            u = ds["uncertainty"][:][r, c].astype(np.float64)
            aod[idx] = np.where(a == ds["aod_550nm"]._FillValue, np.nan, a)
            unc[idx] = np.where(u == ds["uncertainty"]._FillValue, np.nan, u)
            obs[idx] = ds["is_observed"][:][r, c]
    out["aod"], out["aod_observed"], out["aod_unc"] = aod, obs, unc
    return out


# ── nội suy tại tâm ô ───────────────────────────────────────────────────────────
def bilinear_weights(src: np.ndarray, dst: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Chỉ số dưới i0 và trọng số w cho nội suy tuyến tính 1-D trên lưới đều (src tăng hay giảm dần):
    value(dst) = (1-w)·src_val[i0] + w·src_val[i0+1]. Không ngoại suy."""
    step = src[1] - src[0]
    if not np.allclose(np.diff(src), step):
        raise ValueError("lưới nguồn không đều — bilinear trọng số cố định không áp được")
    pos = (dst - src[0]) / step
    if pos.min() < 0 or pos.max() > len(src) - 1:
        raise ValueError(f"điểm đích nằm ngoài lưới nguồn [{src.min()}, {src.max()}]")
    i0 = np.clip(np.floor(pos).astype(np.int64), 0, len(src) - 2)
    return i0, pos - i0


def _at_points(field: np.ndarray, wy, wx) -> np.ndarray:
    """(nt, ny, nx) → (nt, nđiểm)."""
    (i0, fy), (j0, fx) = wy, wx
    return (field[:, i0, j0] * (1 - fy) * (1 - fx) + field[:, i0 + 1, j0] * fy * (1 - fx)
            + field[:, i0, j0 + 1] * (1 - fy) * fx + field[:, i0 + 1, j0 + 1] * fy * fx)


def _cells(df: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray]:
    """Bảng ô duy nhất (cell_id, row, col, cell_lat, cell_lon) và vị trí ô của từng dòng."""
    cells = df[["cell_id", "row", "col", "cell_lat", "cell_lon"]].drop_duplicates("cell_id").reset_index(drop=True)
    return cells, pd.Index(cells.cell_id).get_indexer(df.cell_id)


def _weights(ds: xr.Dataset, cells: pd.DataFrame):
    return (bilinear_weights(ds.latitude.values.astype(float), cells.cell_lat.values),
            bilinear_weights(ds.longitude.values.astype(float), cells.cell_lon.values))


# ── ERA5 theo giờ ───────────────────────────────────────────────────────────────
def era5_points(months, cells: pd.DataFrame, variables=ERA5_VARS) -> dict:
    """{biến: DataFrame(index = time_utc, cột = vị trí ô)} cho các tháng `months` ('YYYYMM')."""
    parts = {v: [] for v in variables}
    for ym in months:
        p = ERA5_RAW / f"era5_{ym}.nc"
        if not p.exists():
            continue
        with xr.open_dataset(p, engine="netcdf4") as ds:
            wy, wx = _weights(ds, cells)
            t = pd.DatetimeIndex(ds.time.values)
            for v in variables:
                parts[v].append(pd.DataFrame(_at_points(ds[v].values.astype(np.float64), wy, wx), index=t))
    return {v: pd.concat(x).sort_index() for v, x in parts.items() if x}


def add_era5_hourly(df: pd.DataFrame, variables=ERA5_VARS) -> pd.DataFrame:
    out = df.copy()
    cells, ci = _cells(df)
    series = era5_points(sorted(set(df.time_utc.dt.strftime("%Y%m"))), cells, variables)
    for v in variables:
        s = series.get(v)
        ti = s.index.get_indexer(df.time_utc) if s is not None else np.full(len(df), -1)
        vals = np.full(len(df), np.nan)
        ok = ti >= 0
        vals[ok] = s.to_numpy()[ti[ok], ci[ok]]
        out[v] = vals
    return out


# ── CAMS PM2.5 3 giờ/lần → từng giờ ────────────────────────────────────────────
def cams_points(months, cells: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Mốc CAMS (3 giờ/lần) tại tâm ô: (DataFrame index = mốc UTC, cột = vị trí ô; mã nguồn theo mốc).
    Nội suy theo không gian ở TỪNG mốc vì lưới đổi theo sản phẩm (EAC4 0,75° → forecast 0,4° từ 2026-01)."""
    vals, src = [], []
    for ym in months:
        p = CAMS_RAW / f"cams_pm25_{ym}.nc"
        if not p.exists():
            continue
        with xr.open_dataset(p) as ds:
            wy, wx = _weights(ds, cells)
            t = pd.DatetimeIndex(ds.time.values)
            vals.append(pd.DataFrame(_at_points(ds.PM25.values.astype(np.float64), wy, wx), index=t))
            src.append(pd.Series(CAMS_SRC.get(ds.attrs.get("source_dataset"), 0), index=t))
    return pd.concat(vals).sort_index(), pd.concat(src).sort_index()


def add_cams_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """`cams_pm25` = (1 − w)·C(t0) + w·C(t0 + 3 h), t0 = mốc 3 giờ ngay trước slot, w = (t − t0) / 3 h."""
    out = df.copy()
    cells, ci = _cells(df)
    lo, hi = df.time_utc.min() - pd.DateOffset(months=1), df.time_utc.max() + pd.DateOffset(months=1)
    C, src = cams_points(pd.period_range(lo, hi, freq="M").strftime("%Y%m"), cells)
    t0 = df.time_utc.dt.floor("3h")
    w = ((df.time_utc - t0) / CAMS_STEP).to_numpy()
    i0, i1 = C.index.get_indexer(t0), C.index.get_indexer(t0 + CAMS_STEP)
    A = C.to_numpy()
    c0 = np.where(i0 >= 0, A[np.maximum(i0, 0), ci], np.nan)
    c1 = np.where(i1 >= 0, A[np.maximum(i1, 0), ci], np.nan)
    out["cams_pm25"] = np.where(w == 0, c0, (1 - w) * c0 + w * c1)
    s0 = np.where(i0 >= 0, src.to_numpy()[np.maximum(i0, 0)], 0)
    s1 = np.where(i1 >= 0, src.to_numpy()[np.maximum(i1, 0)], 0)
    out["cams_src"] = np.where((w == 0) | (s0 == s1), s0, 3).astype("int8")
    return out


def add_dem(df: pd.DataFrame, variables=DEM_VARS) -> pd.DataFrame:
    """Các lớp DEM tĩnh của ô."""
    out = df.copy()
    with xr.open_dataset(DEM_GRID) as d:
        for v in variables:
            out[v] = d[v].values[df.row.values, df.col.values].astype(np.float64)
    return out


# ── khung ───────────────────────────────────────────────────────────────────────
def build_frame(refresh: bool = False, verbose: bool = True) -> pd.DataFrame:
    """y + x + st cho mọi ô–giờ có PM2.5 và có bản đồ AOD RF (cache ở data/frame_hourly.pkl)."""
    if FRAME_PKL.exists() and not refresh:
        return _add_hour(pd.read_pickle(FRAME_PKL))
    slots = rf_slots()
    if not len(slots):
        raise FileNotFoundError(f"chưa có file bản đồ AOD RF nào trong {RF_DIR}")
    df = load_pm_hourly(slots)
    df = add_cams_hourly(add_era5_hourly(add_dem(add_aod(df))))
    n0 = len(df)
    df = df[df[ERA5_VARS].notna().all(axis=1) & df.cams_pm25.notna()].reset_index(drop=True)
    df["day"] = (df.date - DAY0).dt.days.astype("int32")
    df["t_hour"] = ((df.time_utc - DAY0) / pd.Timedelta(hours=1)).astype("int32")   # giờ kể từ DAY0 (UTC)
    df = _add_st(df)
    keep = ["cell_id", "row", "col", "cell_lat", "cell_lon", "region", "province", "sensor_mix",
            "time_utc", "time_lt", "date", "day", "t_hour", "hour_lt", "year", "month", "season",
            PM, "n_stations", "pm25_sd_stations", *X_VARS, *ST_VARS, "aod_observed", "aod_unc", "cams_src"]
    df = df[keep].sort_values(["time_utc", "cell_id"]).reset_index(drop=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_pickle(FRAME_PKL)
    if verbose:
        print(f"khung giờ: {len(df):,} ô–giờ / {n0:,} có PM2.5 tại {len(slots):,} slot RF "
              f"(bỏ {n0 - len(df):,} thiếu ERA5/CAMS) · {df.cell_id.nunique()} ô · "
              f"{df.time_utc.min():%Y-%m-%d %H} → {df.time_utc.max():%Y-%m-%d %H} UTC · "
              f"AOD có {df.aod.notna().mean() * 100:.1f}% (quan trắc Stage A {df.aod_observed.mean() * 100:.1f}%)")
    return _add_hour(df)


# ── fold ────────────────────────────────────────────────────────────────────────
def make_folds(df: pd.DataFrame, refresh: bool = False) -> pd.DataFrame:
    """Fold site-based: cell_id → outer_fold (0..N_OUTER-1). Đọc lại file nếu đã có để mọi tiến trình dùng chung."""
    if FOLDS_CSV.exists() and not refresh:
        return pd.read_csv(FOLDS_CSV)
    sgkf = StratifiedGroupKFold(n_splits=N_OUTER, shuffle=True, random_state=RANDOM_STATE)
    fold_of = {}
    for k, (_, te) in enumerate(sgkf.split(df, df.region, groups=df.cell_id)):
        fold_of.update({c: k for c in df.cell_id.values[te]})
    cells = df.groupby("cell_id").agg(region=("region", "first"), province=("province", "first"),
                                      n_hours=(PM, "size")).reset_index()
    cells["outer_fold"] = cells.cell_id.map(fold_of).astype(int)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cells.to_csv(FOLDS_CSV, index=False)
    return cells


def outer_folds(df: pd.DataFrame, cv: str) -> np.ndarray:
    """Fold ngoài của từng dòng: `site` theo ô (make_folds), `sample` ngẫu nhiên theo ô–giờ."""
    if cv == "site":
        return df.cell_id.map(make_folds(df).set_index("cell_id").outer_fold).to_numpy()
    if cv == "sample":
        out = np.empty(len(df), np.int64)
        for k, (_, te) in enumerate(KFold(N_OUTER, shuffle=True, random_state=RANDOM_STATE).split(df)):
            out[te] = k
        return out
    raise ValueError(f"cv phải thuộc {CV_KINDS}: {cv!r}")


def inner_val_cells(df_train: pd.DataFrame, n_splits: int = 4, seed: int = RANDOM_STATE) -> np.ndarray:
    """Ô validation của vòng trong: 1 phần trong StratifiedGroupKFold(n_splits) trên các ô train của fold ngoài."""
    sgkf = StratifiedGroupKFold(n_splits=n_splits, shuffle=True, random_state=seed)
    _, va = next(iter(sgkf.split(df_train, df_train.region, groups=df_train.cell_id)))
    return np.unique(df_train.cell_id.values[va])


def metrics(obs, est) -> dict:
    """R2 = 1 − SSE/SST (sklearn); R2_paper = bình phương hệ số tương quan Pearson (Li et al. 2024,
    Supplementary Note 1); RPE = RMSE / TB quan trắc (%)."""
    obs, est = np.asarray(obs, float), np.asarray(est, float)
    m = np.isfinite(obs) & np.isfinite(est)
    obs, est = obs[m], est[m]
    n = len(obs)
    if n < 3:
        return dict(N=n, R2=np.nan, R2_paper=np.nan, RMSE=np.nan, MAE=np.nan, RPE=np.nan,
                    slope=np.nan, intercept=np.nan, bias=np.nan)
    rmse = float(np.sqrt(np.mean((est - obs) ** 2)))
    slope, intercept = np.polyfit(obs, est, 1)
    return dict(N=n, R2=float(1 - np.sum((obs - est) ** 2) / np.sum((obs - obs.mean()) ** 2)),
                R2_paper=float(np.corrcoef(obs, est)[0, 1] ** 2), RMSE=rmse,
                MAE=float(np.mean(np.abs(est - obs))), RPE=float(100 * rmse / obs.mean()),
                slope=float(slope), intercept=float(intercept), bias=float(np.mean(est - obs)))


# ── kiểm tra ────────────────────────────────────────────────────────────────────
def check(df: pd.DataFrame) -> None:
    """PM2.5: ô một trạm, ngày đủ 24 giờ → TB các giờ = bảng ngày của map_aqi_grid (bắt lỗi gộp/nhãn giờ);
    cộng thống kê nhanh của khung. (ERA5/CAMS đã đối chiếu với lưới ngày lúc dựng lần đầu, 29/10/2026.)"""
    pmh = pd.read_pickle(PM_HOURLY)
    pmd = pd.read_pickle(PM_DAILY)
    one = pd.read_csv(CELLS_FILE).query("n_stations_total == 1").cell_id
    h = pmh[pmh.cell_id.isin(one)].assign(date=lambda x: x.time_lt.dt.normalize())   # bảng ngày gộp nhãn 00–23
    g = h.groupby(["cell_id", "date"]).pm25.agg(["mean", "size"]).reset_index()
    m = g[g["size"] == 24].merge(pmd[["cell_id", "date", PM]], on=["cell_id", "date"])
    print(f"[check PM] {len(m):,} ô–ngày một trạm đủ 24 giờ · max |TB giờ − bảng ngày| = "
          f"{(m['mean'] - m[PM]).abs().max():.2e} µg/m³")
    print(f"[khung] {len(df):,} dòng · {df.cell_id.nunique()} ô · theo giờ địa phương:",
          df.hour_lt.value_counts().sort_index().to_dict())
    print(f"[khung] theo năm: {df.year.value_counts().sort_index().to_dict()} · cams_src: "
          f"{df.cams_src.value_counts().sort_index().to_dict()}")
    print("[khung] tương quan Pearson với PM2.5:",
          {v: round(df[[PM, v]].corr().iloc[0, 1], 3) for v in ["aod", "cams_pm25", "PBLH", "RH", "T2m"]})


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--refresh", action="store_true", help="dựng lại cả khung lẫn fold")
    ap.add_argument("--check", action="store_true", help="đối chiếu PM2.5 giờ với bảng ngày + thống kê")
    a = ap.parse_args()
    d = build_frame(refresh=a.refresh)
    f = make_folds(d, refresh=a.refresh)
    print(f.groupby("outer_fold").agg(n_cells=("cell_id", "size"), n_hours=("n_hours", "sum"),
                                      regions=("region", lambda s: s.value_counts().to_dict())).to_string())
    if a.check:
        check(d)
