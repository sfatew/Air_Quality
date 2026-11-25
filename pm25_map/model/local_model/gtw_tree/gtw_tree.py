"""GTW-Tree — mô hình cây có trọng số không gian–thời gian (Li, Wang & Wu 2024, npj Clim. Atmos.
Sci. 7:138; Supplementary Notes 3–4, Table 3), áp cho PM2.5 theo giờ.

Với MỖI điểm đích (ô j, giờ t) dựng riêng một mô hình cây từ mẫu train:

1. Mẫu cục bộ — mẫu train trong 30 ngày (720 giờ) trước giờ đích, chỉ quá khứ (t ≤ t_j, gồm cả cùng giờ ở ô khác):
       d² = ds² + λ·dt²     ds: km (haversine giữa tâm ô), dt: NGÀY (= số giờ / 24) → λ có đơn vị km²/ngày²
                            như paper và bản ngày trước, nên cùng một lưới λ giữ nguyên ý nghĩa
       h² = d² của mẫu gần thứ N0 (kernel thích ứng — mật độ trạm không đều)
       w  = exp(−d² / h²), giữ mẫu có w > 1e-4 (số mẫu đổi liên tục theo không gian → bản đồ mượt)
2. Mẫu non-local — Ns = 3 mẫu train có giờ ≤ t giống nhất trong không gian biến:
       S = Σ_k (z_jk − z_mk)²  (biến chuẩn hoá z theo tập train; NaN bỏ qua, chuẩn theo số biến có)
   trọng số = max(w). Mẫu trùng với mẫu cục bộ vẫn giữ (paper: nên có từ cả hai góc nhìn).
3. MODEL.fit(X, y, sample_weight=w) với tham số mặc định của thư viện (paper: 100 cây, LightGBM
   lr 0,1; RF không giới hạn độ sâu).
4. Dự đoán ô j giờ t; ghi số mẫu, băng thông h và độ quan trọng biến của mô hình cục bộ.

λ và N0 được chọn bằng vòng lặp lồng trên tập validation (run_gtw.py).
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass

import numpy as np
from joblib import Parallel, delayed

W_MIN = 1e-4          # ngưỡng trọng số giữ mẫu (Supplementary Note 3)
HOURS_PER_DAY = 24
WINDOW_HOURS = 30 * HOURS_PER_DAY    # chỉ lấy 30 ngày trước giờ đích
N_NONLOCAL = 3        # Ns
ALGOS = ("rf", "xgb", "lgbm")


def make_model(algo: str, seed: int = 0):
    """Tham số mặc định như paper (Supplementary Table 3); n_jobs=1 vì song song hoá theo điểm đích."""
    if algo == "rf":
        from sklearn.ensemble import RandomForestRegressor
        return RandomForestRegressor(n_estimators=100, n_jobs=1, random_state=seed)
    if algo == "lgbm":
        import lightgbm as lgb
        return lgb.LGBMRegressor(n_estimators=100, learning_rate=0.1, n_jobs=1, verbosity=-1,
                                 random_state=seed)
    if algo == "xgb":
        import xgboost as xgb
        return xgb.XGBRegressor(n_estimators=100, n_jobs=1, random_state=seed, tree_method="hist")
    raise ValueError(f"algo phải thuộc {ALGOS}: {algo!r}")


def _importance(model, algo: str, p: int) -> np.ndarray:
    """Độ quan trọng biến của mô hình cục bộ, chuẩn hoá tổng = 1 (RF: impurity; XGB/LGBM: gain)."""
    if algo == "lgbm":
        imp = model.booster_.feature_importance(importance_type="gain").astype(np.float64)
    else:
        imp = np.asarray(model.feature_importances_, dtype=np.float64)
    s = imp.sum()
    return (imp / s if s > 0 else np.zeros(p)).astype(np.float32)


@dataclass
class Pool:
    """Mẫu train, sắp theo giờ tăng dần."""
    X: np.ndarray        # (n, p) float32, có thể có NaN
    Z: np.ndarray        # (n, p) X chuẩn hoá z theo chính tập này — chỉ dùng tìm mẫu non-local
    y: np.ndarray        # (n,)
    t: np.ndarray        # (n,) int, giờ kể từ DAY0, tăng dần
    cell: np.ndarray     # (n,) chỉ số ô (hàng của ma trận khoảng cách)
    mu: np.ndarray       # (p,) TB / độ lệch chuẩn dùng chuẩn hoá điểm đích
    sd: np.ndarray


def make_pool(X, y, t, cell) -> Pool:
    o = np.argsort(t, kind="stable")
    X = np.asarray(X, np.float32)[o]
    mu, sd = np.nanmean(X, axis=0), np.nanstd(X, axis=0)
    sd = np.where(sd > 0, sd, 1.0)
    return Pool(X=X, Z=((X - mu) / sd).astype(np.float32), y=np.asarray(y, np.float64)[o],
                t=np.asarray(t)[o], cell=np.asarray(cell)[o], mu=mu, sd=sd)


def _nonlocal_chunk(Zpool, tpool, Zt, tt, ns):
    out = np.full((len(tt), ns), -1, dtype=np.int64)
    ends = np.searchsorted(tpool, tt, side="right")
    p = Zpool.shape[1]
    has_nan = bool(np.isnan(Zpool).any() or np.isnan(Zt).any())
    for i, (z, e) in enumerate(zip(Zt, ends)):
        if e == 0:
            continue
        diff = Zpool[:e] - z
        if has_nan:
            valid = np.isfinite(diff)
            nv = valid.sum(axis=1)
            S = np.where(nv > 0, (np.where(valid, diff, 0.0) ** 2).sum(axis=1) * p / np.maximum(nv, 1), np.inf)
        else:
            S = np.einsum("ij,ij->i", diff, diff)
        k = min(ns, int(e))
        out[i, :k] = np.argpartition(S, k - 1)[:k]
    return out


def nonlocal_indices(pool: Pool, Xt: np.ndarray, tt: np.ndarray, ns: int = N_NONLOCAL,
                     n_jobs: int = 8, chunk: int = 2000) -> np.ndarray:
    """(n_đích, ns) chỉ số trong pool của Ns mẫu giống nhất có giờ ≤ t (−1 nếu thiếu).

    Không phụ thuộc λ, N0 hay thuật toán → tính một lần cho mỗi (pool, tập đích) rồi dùng lại (run_gtw lưu ra đĩa).
    """
    Zt = ((np.asarray(Xt, np.float32) - pool.mu) / pool.sd).astype(np.float32)
    tt = np.asarray(tt)
    blocks = [slice(i, i + chunk) for i in range(0, len(tt), chunk)]
    res = Parallel(n_jobs=n_jobs, backend="loky")(
        delayed(_nonlocal_chunk)(pool.Z, pool.t, Zt[b], tt[b], ns) for b in blocks)
    return np.concatenate(res) if res else np.full((0, ns), -1, dtype=np.int64)


def predict_one(pool: Pool, x: np.ndarray, t: int, ds_row: np.ndarray, idx_nl: np.ndarray,
                lam: float, n0: int, algo: str, window: int = WINDOW_HOURS,
                importance: bool = False, seed: int = 0) -> tuple:
    """Dựng mô hình cục bộ cho một điểm đích; trả (dự đoán, n_cục_bộ, n_tổng, h, độ quan trọng|None).

    `t`, `window` tính bằng giờ; `ds_row[c]` = khoảng cách (km) từ ô đích tới ô train c;
    `idx_nl` = mẫu non-local (nonlocal_indices).
    """
    s = np.searchsorted(pool.t, t - window, side="left")
    e = np.searchsorted(pool.t, t, side="right")
    p = pool.X.shape[1]
    idx_loc = np.arange(s, e)
    if len(idx_loc):
        dt_days = (t - pool.t[s:e]).astype(np.float64) / HOURS_PER_DAY
        d2 = ds_row[pool.cell[s:e]] ** 2 + lam * dt_days ** 2
        kth = min(n0, len(d2)) - 1
        h2 = np.partition(d2, kth)[kth]
        w = np.exp(-d2 / h2) if h2 > 0 else (d2 == 0).astype(np.float64)
        keep = w > W_MIN
        idx_loc, w_loc = idx_loc[keep], w[keep]
    else:
        h2, w_loc = np.nan, np.empty(0)
    idx_nl = idx_nl[idx_nl >= 0]
    w_nl = np.full(len(idx_nl), w_loc.max() if len(w_loc) else 1.0)
    idx = np.concatenate([idx_loc, idx_nl])
    w_all = np.concatenate([w_loc, w_nl])
    if len(idx) < 3:                                   # quá ít mẫu: TB có trọng số
        pred = float(np.average(pool.y[idx], weights=w_all)) if len(idx) else np.nan
        return pred, len(idx_loc), len(idx), float(np.sqrt(h2)) if np.isfinite(h2) else np.nan, None
    m = make_model(algo, seed)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        m.fit(pool.X[idx], pool.y[idx], sample_weight=w_all)
        pred = float(m.predict(x[None, :].astype(np.float32))[0])
    imp = _importance(m, algo, p) if importance else None
    return pred, len(idx_loc), len(idx), float(np.sqrt(h2)), imp


def _run_chunk(pool, Xt, tt, ct, nl, ds, lam, n0, algo, window, importance):
    return [predict_one(pool, x, int(t), ds[c], n, lam, n0, algo, window, importance)
            for x, t, c, n in zip(Xt, tt, ct, nl)]


def predict_targets(pool: Pool, Xt: np.ndarray, tt: np.ndarray, ct: np.ndarray, nl: np.ndarray,
                    ds: np.ndarray, lam: float, n0: int, algo: str, *, window: int = WINDOW_HOURS,
                    importance: bool = False, n_jobs: int = 8, chunk: int = 200) -> dict:
    """Dự đoán mọi điểm đích (Xt[i], giờ tt[i], ô ct[i]); `ds[ct[i]]` = hàng khoảng cách tới các ô
    train, `nl[i]` = mẫu non-local. Song song hoá theo khối điểm đích (mỗi mô hình cục bộ 1 luồng).
    """
    Xt = np.asarray(Xt, np.float32)
    blocks = [slice(i, i + chunk) for i in range(0, len(tt), chunk)]
    res = Parallel(n_jobs=n_jobs, backend="loky")(
        delayed(_run_chunk)(pool, Xt[b], tt[b], ct[b], nl[b], ds, lam, n0, algo, window, importance)
        for b in blocks)
    flat = [r for block in res for r in block]
    out = {"pred": np.array([r[0] for r in flat]), "n_local": np.array([r[1] for r in flat]),
           "n_total": np.array([r[2] for r in flat]), "h": np.array([r[3] for r in flat])}
    if importance:
        p = Xt.shape[1]
        out["importance"] = np.stack([r[4] if r[4] is not None else np.full(p, np.nan, np.float32)
                                      for r in flat])
    return out
