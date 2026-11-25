#!/usr/bin/env python3
"""Global model PM2.5 theo giờ trên lưới 0,05° — LightGBM tune bằng Optuna, nested CV (theo khung train_v3.py cũ).

    y  PM2.5 1 giờ của ô;  x  `_data.X_VARS` (AOD Stage B RF, ERA5 ×11 theo giờ, DEM ×2, CAMS PM2.5)

Hai biến thể (Li et al. 2024, Supplementary Table 1):
    original    chỉ x
    st          x + 9 biến không gian/thời gian của train_v3 (d_corner_*, d_mid_*, pt1–pt3) — bản "-st"

Hai kiểu kiểm định (`_data.outer_folds`):
    site    vòng ngoài 5 fold theo ô; vòng trong Optuna TPE, mỗi trial chấm bằng GroupKFold(5) theo ô trên phần
            train — cùng kiểu tổng quát hoá không gian mà vòng ngoài đo. Sau CV: tune lại trên toàn bộ dữ liệu và
            refit → model.txt (mô hình triển khai để vẽ bản đồ; chỉ dựng ở lượt site).
    sample  vòng ngoài 5 fold ngẫu nhiên theo ô–giờ; vòng trong KFold(5) ngẫu nhiên.

Kết quả: global_model/hourly/<cv>/<biến thể>/  (kết quả theo ngày cũ ở global_model/{original,st}/ giữ nguyên)
    oof.pkl                 dự đoán OOF (cell_id, time_utc, region, outer_fold, pm25, pred)
    metrics.json            chỉ số gộp + từng fold + tham số Optuna từng fold (+ mô hình triển khai)
    feature_importance.csv  độ quan trọng biến (% gain) TB ± độ lệch chuẩn qua các fold
    model.txt               booster triển khai (chỉ cv=site)
    folds/fold_<k>.{json,npz}   kết quả từng fold — chạy lại thì bỏ qua fold đã xong

    python train_global.py                                   # site rồi sample, cả hai biến thể
    python train_global.py --cv site --variants st --n-trials 50
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore", message="X does not have valid feature names", category=UserWarning)

import lightgbm as lgb  # noqa: E402
import numpy as np  # noqa: E402
import optuna  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.metrics import r2_score  # noqa: E402
from sklearn.model_selection import GroupKFold, KFold  # noqa: E402

HERE = Path(__file__).resolve().parent                     # model/global_model
sys.path.insert(0, str(HERE.parent))                        # model/ → _data
import _data as D  # noqa: E402

optuna.logging.set_verbosity(optuna.logging.WARNING)

VARIANTS = {"original": D.X_VARS, "st": D.X_VARS + D.ST_VARS,
            "st_hour": D.X_VARS + D.ST_VARS + D.HOUR_VARS}
OUT = HERE / "hourly"

# Không gian tìm kiếm Optuna — giữ nguyên train_v3 (5 tham số quan trọng nhất của LightGBM)
BASE_PARAMS = dict(n_estimators=500, objective="regression", metric="rmse", verbosity=-1,
                   random_state=D.RANDOM_STATE)


def suggest_params(trial: optuna.Trial, n_jobs: int) -> dict:
    return dict(
        BASE_PARAMS, n_jobs=n_jobs, bagging_freq=5,
        num_leaves       = trial.suggest_int  ("num_leaves",       16,    512,  log=True),
        min_data_in_leaf = trial.suggest_int  ("min_data_in_leaf", 20,    500,  log=True),
        learning_rate    = trial.suggest_float("learning_rate",    0.005, 0.1,  log=True),
        feature_fraction = trial.suggest_float("feature_fraction", 0.6,   1.0),
        bagging_fraction = trial.suggest_float("bagging_fraction", 0.6,   1.0),
    )


def log(msg: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S}  {msg}", flush=True)


def inner_splits(X, y, groups, inner_k: int, cv: str, seed: int) -> list:
    """Vòng trong cùng kiểu với vòng ngoài: theo ô (site) hoặc ngẫu nhiên theo ô–giờ (sample)."""
    if cv == "site":
        return list(GroupKFold(n_splits=min(inner_k, len(np.unique(groups)))).split(X, y, groups))
    return list(KFold(n_splits=inner_k, shuffle=True, random_state=seed).split(X, y))


def tune_params(X, y, groups, *, cv: str, n_trials: int, inner_k: int, n_jobs: int, seed: int) -> tuple[dict, float]:
    """Optuna TPE chỉ trên phần dữ liệu được đưa vào (phần train của fold ngoài), chấm bằng R² vòng trong."""
    splits = inner_splits(X, y, groups, inner_k, cv, seed)

    def objective(trial):
        params = suggest_params(trial, n_jobs)
        return float(np.mean([r2_score(y[va], lgb.LGBMRegressor(**params).fit(X[tr], y[tr]).predict(X[va]))
                              for tr, va in splits]))

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=seed))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    return {**BASE_PARAMS, "n_jobs": n_jobs, "bagging_freq": 5, **study.best_params}, float(study.best_value)


def gain_pct(model: lgb.LGBMRegressor, n: int) -> np.ndarray:
    imp = model.booster_.feature_importance(importance_type="gain").astype(float)
    return 100 * imp / imp.sum() if imp.sum() > 0 else np.zeros(n)


def run(variant: str, df: pd.DataFrame, cv: str, *, n_trials: int, inner_k: int, n_jobs: int,
        out_root: Path = OUT, final: bool = True) -> None:
    """Nested CV một biến thể với kiểu kiểm định `cv`; `final` dựng thêm mô hình triển khai."""
    feats = VARIANTS[variant]
    out = out_root / cv / variant
    (out / "folds").mkdir(parents=True, exist_ok=True)
    X = df[feats].to_numpy(np.float32)
    y = df[D.PM].to_numpy(np.float64)
    groups = df.cell_id.to_numpy()
    outer = D.outer_folds(df, cv)
    oof = np.full(len(y), np.nan, np.float64)
    per_fold, imps = {}, []
    tag = f"{cv}/{variant}"
    log(f"[{tag}] {len(feats)} biến · {len(y):,} ô–giờ · {len(np.unique(groups))} ô")
    t_all = time.time()
    for k in range(D.N_OUTER):
        t0 = time.time()
        tr, te = np.where(outer != k)[0], np.where(outer == k)[0]
        fj, fz = out / "folds" / f"fold_{k}.json", out / "folds" / f"fold_{k}.npz"
        if fj.exists() and fz.exists():                       # fold đã xong ở lượt trước
            z = np.load(fz)
            oof[z["te"]] = z["pred"]
            imps.append(z["imp"])
            per_fold[k] = json.loads(fj.read_text())
            log(f"[{tag}] fold {k}: đã có — R²={per_fold[k]['R2']:.3f}")
            continue
        bp, inner_r2 = tune_params(X[tr], y[tr], groups[tr], cv=cv, n_trials=n_trials, inner_k=inner_k,
                                   n_jobs=n_jobs, seed=D.RANDOM_STATE + k)
        m = lgb.LGBMRegressor(**bp).fit(X[tr], y[tr])
        oof[te] = m.predict(X[te])
        imps.append(gain_pct(m, len(feats)))
        per_fold[k] = {**D.metrics(y[te], oof[te]), "inner_cv_r2": inner_r2,
                       "best_params": {kk: vv for kk, vv in bp.items() if kk not in BASE_PARAMS and kk != "n_jobs"},
                       "n_train_cells": int(len(np.unique(groups[tr])))}
        np.savez(fz, te=te, pred=oof[te], imp=imps[-1])
        fj.write_text(json.dumps(per_fold[k], indent=2))
        log(f"[{tag}] fold {k}: R²={per_fold[k]['R2']:.3f}  RMSE={per_fold[k]['RMSE']:.2f}  "
            f"inner-R²={inner_r2:.3f}  ({time.time() - t0:.0f}s)")

    res = df[["cell_id", "time_utc", "t_hour", "date", "hour_lt", "region", D.PM]].copy()
    res["outer_fold"], res["pred"] = outer, oof
    res.to_pickle(out / "oof.pkl")
    imp = np.array(imps)
    pd.DataFrame({"feature": feats, "importance_pct": imp.mean(0), "importance_std": imp.std(0)}) \
        .sort_values("importance_pct", ascending=False).to_csv(out / "feature_importance.csv", index=False)
    summary = dict(variant=variant, cv=cv, model="LightGBM (Optuna)", features=feats,
                   pooled=D.metrics(y, oof), per_fold=per_fold, seconds=time.time() - t_all,
                   created=datetime.now().isoformat(timespec="seconds"))

    if final:                                      # mô hình triển khai: tune lại trên toàn bộ, refit
        fm = out / "final_model.json"
        if fm.exists() and (out / "model.txt").exists():
            summary["final_model"] = json.loads(fm.read_text())
        else:
            t0 = time.time()
            bp, full_r2 = tune_params(X, y, groups, cv=cv, n_trials=n_trials, inner_k=inner_k,
                                      n_jobs=n_jobs, seed=D.RANDOM_STATE)
            lgb.LGBMRegressor(**bp).fit(X, y).booster_.save_model(str(out / "model.txt"))
            summary["final_model"] = {"cv_r2_all": full_r2, "params": {kk: vv for kk, vv in bp.items() if kk != "n_jobs"}}
            fm.write_text(json.dumps(summary["final_model"], indent=2))
            log(f"[{tag}] mô hình cuối (tune trên toàn bộ, R² CV={full_r2:.3f}) → model.txt ({time.time() - t0:.0f}s)")
    (out / "metrics.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    p = summary["pooled"]
    log(f"[{tag}] GỘP: R²={p['R2']:.3f}  R²_paper={p['R2_paper']:.3f}  RMSE={p['RMSE']:.2f}  RPE={p['RPE']:.1f}%")


def main() -> int:
    ap = argparse.ArgumentParser(description="Global LightGBM tune bằng Optuna (nested CV site / sample)")
    ap.add_argument("--cv", nargs="+", default=list(D.CV_KINDS), choices=D.CV_KINDS)
    ap.add_argument("--variants", nargs="+", default=list(VARIANTS), choices=list(VARIANTS))
    ap.add_argument("--n-trials", type=int, default=30, help="số trial Optuna mỗi fold ngoài (như train_v3)")
    ap.add_argument("--inner-k", type=int, default=5)
    ap.add_argument("--n-jobs", type=int, default=4)
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args()
    df = D.build_frame()
    log(f"global: cv {a.cv} · biến thể {a.variants} · Optuna {a.n_trials} trial · {a.n_jobs} luồng")
    for cv in a.cv:
        for v in a.variants:
            run(v, df, cv, n_trials=a.n_trials, inner_k=a.inner_k, n_jobs=a.n_jobs, out_root=a.out,
                final=(cv == "site"))
    log("xong")
    return 0


if __name__ == "__main__":
    sys.exit(main())
