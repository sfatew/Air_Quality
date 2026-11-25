"""Đọc kết quả theo giờ của mọi mô hình và các hàm vẽ dùng chung — cho compare_models.ipynb và notebook
kết quả riêng của từng mô hình.

Mỗi mô hình ghi dự đoán ngoài mẫu (out-of-fold) trên CÙNG fold của `_data.outer_folds(df, cv)`:
    global_model/hourly/<cv>/{original,st}/oof.pkl                (train_global.py — LightGBM tune bằng Optuna)
    local_model/gtw_tree/output_hourly/<cv>/<thuật toán>/test_fold*.pkl   (run_gtw.py)
    local_model/ASTMNN/output_hourly/<cv>/test_fold*.pkl          (chưa chuyển sang giờ)
`load_all(cv)` trả {tên hiển thị: DataFrame(cell_id, time_utc, outer_fold, pm25, pred, …)}. So sánh nên
dùng `joined(...)`: chỉ những ô–giờ mà mọi mô hình được so đều đã có dự đoán (fold chưa chạy xong của một
mô hình sẽ không làm lệch so sánh). Kết quả theo ngày cũ (global_model/{original,st}/, gtw_tree/output/)
vẫn nằm trên đĩa nhưng không đọc ở đây.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import _data as D

MODEL_DIR = D.MODEL_DIR
GLOBAL_DIR = MODEL_DIR / "global_model" / "hourly"
GTW_DIR = MODEL_DIR / "local_model" / "gtw_tree" / "output_hourly"
ASTMNN_DIR = MODEL_DIR / "local_model" / "ASTMNN" / "output_hourly"
ALGO = {"rf": "RF", "xgb": "XGBoost", "lgbm": "LightGBM"}
GLOBAL_NAME = {"original": "Global", "st": "Global -st"}     # LightGBM tune bằng Optuna
KEYS = ["cell_id", "time_utc"]


def load_global(cv: str = "site") -> dict:
    return {name: pd.read_pickle(GLOBAL_DIR / cv / v / "oof.pkl")
            for v, name in GLOBAL_NAME.items() if (GLOBAL_DIR / cv / v / "oof.pkl").exists()}


def load_gtw(cv: str = "site") -> dict:
    out = {}
    for algo, label in ALGO.items():
        parts = sorted((GTW_DIR / cv / algo).glob("test_fold*.pkl"))
        if parts:
            out[f"GTW-{label}"] = pd.concat([pd.read_pickle(p) for p in parts], ignore_index=True)
    return out


def load_astmnn(cv: str = "site") -> dict:
    parts = sorted((ASTMNN_DIR / cv).glob("test_fold*.pkl"))
    return {"ASTMNN": pd.concat([pd.read_pickle(p) for p in parts], ignore_index=True)} if parts else {}


def load_all(cv: str = "site") -> dict:
    return {**load_global(cv), **load_gtw(cv), **load_astmnn(cv)}


def joined(preds: dict, frame: pd.DataFrame, names=None, cv: str = "site") -> pd.DataFrame:
    """Bảng rộng trên các ô–giờ chung: cột quan trắc + một cột dự đoán mỗi mô hình + fold ngoài của `cv`."""
    names = list(names or preds)
    keys = None
    for n in names:
        k = preds[n].loc[preds[n].pred.notna(), KEYS]
        keys = k if keys is None else keys.merge(k, on=KEYS)
    cols = [*KEYS, "date", "hour_lt", "region", "province", "season", "year", "month", D.PM]
    base = frame.assign(outer_fold=D.outer_folds(frame, cv))[cols + ["outer_fold"]].merge(keys, on=KEYS)
    for n in names:
        base = base.merge(preds[n][[*KEYS, "pred"]].rename(columns={"pred": n}), on=KEYS)
    return base


def metrics_table(J: pd.DataFrame, names) -> pd.DataFrame:
    rows = []
    for n in names:
        m = D.metrics(J[D.PM], J[n])
        per = [D.metrics(g[D.PM], g[n])["R2"] for _, g in J.groupby("outer_fold")]
        rows.append(dict(model=n, **{k: m[k] for k in ("N", "R2", "R2_paper", "RMSE", "MAE", "RPE", "slope", "bias")},
                         R2_fold_mean=np.mean(per), R2_fold_sd=np.std(per)))
    return pd.DataFrame(rows).set_index("model")


def per_cell(J: pd.DataFrame, name: str, cells: pd.DataFrame, min_hours: int = 200) -> pd.DataFrame:
    """Chỉ số theo ô (≥ min_hours ô–giờ), kèm toạ độ / vùng của ô."""
    rows = {c: D.metrics(g[D.PM], g[name]) for c, g in J.groupby("cell_id") if len(g) >= min_hours}
    return pd.DataFrame(rows).T.join(cells.set_index("cell_id"))


def cell_table(frame: pd.DataFrame) -> pd.DataFrame:
    return (frame.groupby("cell_id")
            .agg(lat=("cell_lat", "first"), lon=("cell_lon", "first"), region=("region", "first"),
                 province=("province", "first"), n_hours=(D.PM, "size")).reset_index())


# ── vẽ ──────────────────────────────────────────────────────────────────────────
def density(ax, obs, est, title: str) -> dict:
    """Hexbin quan trắc ↔ ước lượng (thang log), đường 1:1 và đường hồi quy, hộp chỉ số."""
    obs, est = np.asarray(obs, float), np.asarray(est, float)
    m = np.isfinite(obs) & np.isfinite(est)
    obs, est = obs[m], est[m]
    hi = np.nanpercentile(np.r_[obs, est], 99.5)
    ax.hexbin(obs, est, gridsize=60, extent=(0, hi, 0, hi), mincnt=1, bins="log", cmap="viridis")
    ax.plot([0, hi], [0, hi], "k--", lw=0.8)
    r = D.metrics(obs, est)
    xs = np.array([0, hi])
    ax.plot(xs, r["slope"] * xs + r["intercept"], color="red", lw=1)
    ax.text(0.03, 0.97, f"N = {r['N']:,}\nR² = {r['R2']:.3f}\nRMSE = {r['RMSE']:.2f}\n"
            f"y = {r['slope']:.2f}x + {r['intercept']:.1f}", transform=ax.transAxes, va="top",
            fontsize=8, family="monospace", bbox=dict(facecolor="white", alpha=0.8, edgecolor="0.7"))
    ax.set_xlim(0, hi); ax.set_ylim(0, hi); ax.set_title(title, fontsize=10)
    ax.set_xlabel("PM2.5 quan trắc (µg/m³)"); ax.set_ylabel("PM2.5 ước lượng (µg/m³)")
    return r


_VN = None


def vn_axes(ax, region: str = "all") -> None:
    """Nền ranh giới tỉnh VN; region='north' phóng to đồng bằng sông Hồng – Đông Bắc."""
    global _VN
    if _VN is None:
        import geopandas as gpd
        _VN = gpd.read_file(MODEL_DIR.parents[1] / "Data_Collection/GADM_Vietnam/gadm41_VNM_1.shp")
    _VN.boundary.plot(ax=ax, color="0.5", lw=0.3)
    if region == "north":
        ax.set_xlim(104.5, 108.2); ax.set_ylim(20.0, 22.6)
    else:
        ax.set_xlim(102.0, 110.0); ax.set_ylim(8.3, 23.5)
    ax.set_xticks([]); ax.set_yticks([])
