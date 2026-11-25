#!/usr/bin/env python3
"""Nested CV cho GTW-Tree (RF, XGBoost, LightGBM) — PM2.5 theo giờ trên lưới 0,05°.

Vòng ngoài (`_data.outer_folds`, dùng chung với global_model): `site` = 5 fold theo ô, `sample` = 5 fold ngẫu
nhiên theo ô–giờ. Vòng trong — vòng lặp lồng như paper (Supplementary Note 3): trong phần train của fold ngoài,
tách validation cùng kiểu với vòng ngoài (site: 1/4 số ô, StratifiedGroupKFold theo vùng; sample: 1/4 số
ô–giờ ngẫu nhiên), lấy ngẫu nhiên `--inner-targets` điểm đích validation, rồi

    for λ in LAMBDAS:            # km²/ngày² — 1 ngày tương đương √λ km
        for N0 in N0S:           # băng thông thích ứng = khoảng cách tới mẫu gần thứ N0
            dựng mô hình cục bộ cho các điểm đích validation

và chọn (λ, N0) có R² validation cao nhất. Sau đó dùng TẤT CẢ mẫu train của fold ngoài làm pool, dự đoán
MỌI ô–giờ test (mỗi ô–giờ một mô hình cục bộ, như paper).

Chạy tiếp được: bước nào đã có kết quả trên đĩa thì bỏ qua.
    output_hourly/<cv>/<algo>/inner_grid.csv     fold, λ, N0, chỉ số validation, số mẫu cục bộ, thời gian
    output_hourly/<cv>/<algo>/test_fold<k>.pkl   dự đoán test fold k + n mẫu, h, độ quan trọng biến cục bộ
    output_hourly/<cv>/<algo>/metrics.json       chỉ số gộp mọi fold + từng fold + (λ, N0) đã chọn
    output_hourly/<cv>/nonlocal/                 mẫu non-local đã tính (dùng chung cho cả 3 thuật toán)
    (kết quả theo ngày cũ ở output/<algo>/ giữ nguyên)

    python run_gtw.py                                  # lgbm → xgb → rf, mỗi thuật toán site rồi sample
    python run_gtw.py --algos lgbm --cv site --folds 0 # thử nhanh
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1]))           # pm25_map/model
import _data as D  # noqa: E402
import gtw_tree as GT  # noqa: E402

LAMBDAS = [1e2, 1e3, 1e4, 1e5]          # km²/ngày²; paper: 1e4
N0S = [8, 32, 128, 512, 2048]           # paper: 8. Mạng trạm VN thưa nên dò rộng (ở bản ngày, tối ưu chạm biên 512)
OUT = HERE / "output_hourly"


def log(msg: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S}  {msg}", flush=True)


def _arrays(df: pd.DataFrame, cell_pos: dict):
    """X, y, giờ (t_hour), chỉ số ô."""
    return (df[D.X_VARS].to_numpy(np.float32), df[D.PM].to_numpy(np.float64), df.t_hour.to_numpy(np.int64),
            df.cell_id.map(cell_pos).to_numpy(np.int64))


def cell_geometry(df: pd.DataFrame):
    """Chỉ số ô và ma trận khoảng cách haversine (km) giữa các ô có trạm."""
    cells = df.groupby("cell_id")[["cell_lat", "cell_lon"]].first().sort_index()
    cell_pos = {c: i for i, c in enumerate(cells.index)}
    dist = D.haversine_km(cells.cell_lat.values[:, None], cells.cell_lon.values[:, None],
                          cells.cell_lat.values[None, :], cells.cell_lon.values[None, :])
    return cell_pos, dist


def inner_split(train: pd.DataFrame, cv: str, k: int, n_targets: int):
    """(pool vòng trong, điểm đích validation, mô tả) — validation cùng kiểu với vòng ngoài."""
    rng = np.random.default_rng(D.RANDOM_STATE + k)
    if cv == "site":
        val_cells = D.inner_val_cells(train, seed=D.RANDOM_STATE + k)
        is_val = train.cell_id.isin(val_cells).to_numpy()
        desc = f"{len(val_cells)} ô val / {train.cell_id.nunique() - len(val_cells)} ô train"
    else:
        is_val = np.zeros(len(train), bool)
        is_val[rng.permutation(len(train))[: len(train) // 4]] = True
        desc = f"{is_val.sum():,} ô–giờ val ngẫu nhiên"
    va = np.where(is_val)[0]
    if n_targets and len(va) > n_targets:
        va = np.sort(rng.choice(va, n_targets, replace=False))
    return train[~is_val], train.iloc[va], desc


def cached_nonlocal(path: Path, pool: GT.Pool, Xt, tt, ct, n_jobs: int) -> np.ndarray:
    """Mẫu non-local của một (pool, tập đích); lưu ra đĩa vì không phụ thuộc thuật toán, λ, N0."""
    if path.exists():
        z = np.load(path)
        if len(z["tt"]) == len(tt) and (z["tt"] == tt).all() and (z["ct"] == ct).all():
            return z["nl"]
    nl = GT.nonlocal_indices(pool, Xt, tt, n_jobs=n_jobs)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(path, nl=nl, tt=tt, ct=ct)
    return nl


def run_algo(algo: str, df: pd.DataFrame, cv: str, dist: np.ndarray, cell_pos: dict, out_root: Path, *,
             outer_folds, lambdas, n0s, n_targets: int, n_jobs: int) -> None:
    """Nested CV một thuật toán với kiểu kiểm định `cv`."""
    out_dir = out_root / cv / algo
    nl_dir = out_root / cv / "nonlocal"
    out_dir.mkdir(parents=True, exist_ok=True)
    grid_csv = out_dir / "inner_grid.csv"
    grid = pd.read_csv(grid_csv) if grid_csv.exists() else pd.DataFrame()
    df = df.assign(outer_fold=D.outer_folds(df, cv))
    tag = f"{algo}/{cv}"

    for k in outer_folds:
        test_pkl = out_dir / f"test_fold{k}.pkl"
        train = df[df.outer_fold != k].reset_index(drop=True)
        test = df[df.outer_fold == k].reset_index(drop=True)

        # ── vòng trong: λ × N0 trên điểm đích validation tách từ train ─────────────
        todo = [(lam, n0) for lam in lambdas for n0 in n0s
                if grid.empty or not ((grid.fold == k) & np.isclose(grid["lambda"], lam) & (grid.N0 == n0)).any()]
        if todo:
            in_tr, in_va, desc = inner_split(train, cv, k, n_targets)
            pool = GT.make_pool(*_arrays(in_tr, cell_pos))
            Xv, yv, tv, cv_idx = _arrays(in_va, cell_pos)
            t0 = time.time()
            nl = cached_nonlocal(nl_dir / f"fold{k}_inner{n_targets}.npz", pool, Xv, tv, cv_idx, n_jobs)
            log(f"[{tag}] fold {k}: vòng trong {desc} · {len(yv):,} điểm đích · non-local {time.time() - t0:.0f}s · "
                f"còn {len(todo)} tổ hợp")
            for lam, n0 in todo:
                t0 = time.time()
                r = GT.predict_targets(pool, Xv, tv, cv_idx, nl, dist, lam, n0, algo, n_jobs=n_jobs)
                m = D.metrics(yv, r["pred"])
                row = dict(fold=k, algo=algo, cv=cv, **{"lambda": lam}, N0=n0, n_val=len(yv), **m,
                           n_local_median=float(np.median(r["n_local"])),
                           n_local_p10=float(np.percentile(r["n_local"], 10)),
                           h_median=float(np.nanmedian(r["h"])), seconds=time.time() - t0)
                grid = pd.concat([grid, pd.DataFrame([row])], ignore_index=True)
                grid.to_csv(grid_csv, index=False)
                log(f"[{tag}] fold {k}  λ={lam:>8g}  N0={n0:>4}  R²={m['R2']:.3f}  RMSE={m['RMSE']:.2f}  "
                    f"n_cục_bộ≈{row['n_local_median']:.0f}  ({row['seconds']:.0f}s)")

        g = grid[grid.fold == k]
        best = g.loc[g.R2.idxmax()]
        lam_b, n0_b = float(best["lambda"]), int(best.N0)

        # ── vòng ngoài: dự đoán mọi ô–giờ test với (λ, N0) đã chọn, pool = mọi mẫu train ──
        if test_pkl.exists():
            log(f"[{tag}] fold {k}: đã có {test_pkl.name} — bỏ qua")
            continue
        pool = GT.make_pool(*_arrays(train, cell_pos))
        Xt, yt, tt, ct = _arrays(test, cell_pos)
        t0 = time.time()
        nl = cached_nonlocal(nl_dir / f"fold{k}_test.npz", pool, Xt, tt, ct, n_jobs)
        log(f"[{tag}] fold {k}: TEST {len(yt):,} điểm đích · λ={lam_b:g} N0={n0_b} · non-local {time.time() - t0:.0f}s")
        t0 = time.time()
        r = GT.predict_targets(pool, Xt, tt, ct, nl, dist, lam_b, n0_b, algo, importance=True, n_jobs=n_jobs)
        res = test[["cell_id", "time_utc", "t_hour", "date", "hour_lt", "region", "outer_fold", D.PM]].copy()
        res["pred"] = r["pred"]
        res["n_local"], res["n_total"], res["h"] = r["n_local"], r["n_total"], r["h"]
        res["lambda"], res["N0"] = lam_b, n0_b
        imp = pd.DataFrame(r["importance"], columns=[f"imp_{v}" for v in D.X_VARS])
        pd.concat([res, imp], axis=1).to_pickle(test_pkl)
        m = D.metrics(yt, res["pred"])
        log(f"[{tag}] fold {k}: TEST λ={lam_b:g} N0={n0_b}  R²={m['R2']:.3f}  RMSE={m['RMSE']:.2f}  "
            f"({len(yt):,} điểm, {time.time() - t0:.0f}s)")

    # ── tổng hợp các fold đã xong ─────────────────────────────────────────────────
    parts = [pd.read_pickle(p) for p in sorted(out_dir.glob("test_fold*.pkl"))]
    if parts:
        allp = pd.concat(parts, ignore_index=True)
        per_fold = {int(f): {**D.metrics(g[D.PM], g.pred), "lambda": float(g["lambda"].iloc[0]),
                             "N0": int(g.N0.iloc[0])} for f, g in allp.groupby("outer_fold")}
        summary = dict(algo=algo, cv=cv, folds_done=sorted(per_fold), pooled=D.metrics(allp[D.PM], allp.pred),
                       per_fold=per_fold, lambdas=lambdas, n0s=n0s, inner_targets=n_targets, x_vars=D.X_VARS,
                       window_hours=GT.WINDOW_HOURS, created=datetime.now().isoformat(timespec="seconds"))
        (out_dir / "metrics.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
        p = summary["pooled"]
        log(f"[{tag}] GỘP {len(per_fold)} fold: R²={p['R2']:.3f}  R²_paper={p['R2_paper']:.3f}  "
            f"RMSE={p['RMSE']:.2f}  RPE={p['RPE']:.1f}%")


def main() -> int:
    ap = argparse.ArgumentParser(description="GTW-Tree nested CV (RF / XGBoost / LightGBM), PM2.5 theo giờ")
    ap.add_argument("--algos", nargs="+", default=["lgbm", "xgb", "rf"], choices=GT.ALGOS)
    ap.add_argument("--cv", nargs="+", default=list(D.CV_KINDS), choices=D.CV_KINDS)
    ap.add_argument("--folds", nargs="+", type=int, default=list(range(D.N_OUTER)))
    ap.add_argument("--lambdas", nargs="+", type=float, default=LAMBDAS)
    ap.add_argument("--n0s", nargs="+", type=int, default=N0S)
    ap.add_argument("--inner-targets", type=int, default=3000,
                    help="số điểm đích validation (ngẫu nhiên) mỗi fold ở vòng trong")
    ap.add_argument("--n-jobs", type=int, default=8)
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args()

    df = D.build_frame()
    cell_pos, dist = cell_geometry(df)
    log(f"GTW-Tree: {len(df):,} ô–giờ, {len(cell_pos)} ô · thuật toán {a.algos} · cv {a.cv} · fold {a.folds} · "
        f"λ {a.lambdas} · N0 {a.n0s} · {a.inner_targets} điểm đích vòng trong · {a.n_jobs} tiến trình")
    for algo in a.algos:
        for cv in a.cv:
            run_algo(algo, df, cv, dist, cell_pos, a.out, outer_folds=a.folds, lambdas=a.lambdas, n0s=a.n0s,
                     n_targets=a.inner_targets, n_jobs=a.n_jobs)
    log("xong")
    return 0


if __name__ == "__main__":
    sys.exit(main())
