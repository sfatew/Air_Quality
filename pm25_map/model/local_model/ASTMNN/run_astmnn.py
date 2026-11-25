#!/usr/bin/env python3
"""Nested CV cho ASTMNN — PM2.5 ngày trên lưới 0,05° (cùng fold, cùng ô validation với GTW-Tree).

Vòng ngoài: 5 fold theo ô (`model/data/cv_folds.csv`, dùng chung mọi mô hình).
Vòng trong: ô validation = `_data.inner_val_cells(train, seed=RANDOM_STATE + k)` (trùng GTW-Tree);
vòng lặp lồng như Table S4/S5 của paper:

    for w in W_GRID:          # kích thước patch SaM (ô 0,05°)
        for t in T_GRID:      # số ngày của chuỗi TaM
            huấn luyện trên ô inner-train, early stopping theo RMSE ô validation

chọn (w, t) có R² validation cao nhất; dự đoán test của fold là của mô hình đó (không huấn luyện
lại). Dự đoán test của các tổ hợp khác vẫn lưu để xem độ nhạy, nhưng KHÔNG dùng để chọn.

Chuẩn hoá theo ô inner-train của từng fold: log1p cho biến lệch phải (LOG_VARS), z, cắt ±5
(khác paper — mạng nơ-ron ngoại suy tuyến tính: ô núi Lào Cai lệch +83 µg/m³ khi không cắt);
AOD thiếu → 0 sau chuẩn hoá, kênh `aod_valid` giữ 0/1. GM nhận: 15 biến (z) + aod_valid + (x, y) Descartes + sin/cos ngày trong
năm (paper chỉ dùng sin(2π·giờ/24); với ngày, sin một mình lẫn tháng 1 với tháng 6 nên thêm cos).

Kết quả (cạnh file này, chạy tiếp được):
    output/inner_grid.csv              fold, w, t, chỉ số validation, epoch, λ ba nhánh, thời gian
    output/preds/fold<k>_w<w>_t<t>.npz dự đoán val + test, lịch sử loss, λ
    output/test_fold<k>.pkl            dự đoán test của (w, t) đã chọn
    output/models/fold<k>.pt           trọng số mô hình đã chọn của fold
    output/metrics.json                chỉ số gộp + từng fold + (w, t), λ đã chọn

    python run_astmnn.py                     # 5 fold × 9 tổ hợp
    python run_astmnn.py --folds 0 --w 9 --t 3 --max-epochs 5     # thử nhanh
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
import torch
import torch.nn.functional as F

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1]))                     # model/
import _data as D  # noqa: E402
import data_astmnn as DA  # noqa: E402
from astmnn import ASTMNN  # noqa: E402

W_GRID = (5, 7, 9)            # paper Table S4 (tốt nhất w = 9)
T_GRID = (3, 4, 5)            # paper Table S5 (tốt nhất t = 3)
R_EARTH = 6371.0


def log(msg: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S}  {msg}", flush=True)


def load_inputs():
    df = D.build_frame()
    idx = pd.read_pickle(DA.DATA / "index.pkl")
    if not (np.array_equal(idx.cell_id.values, df.cell_id.values) and np.array_equal(idx.day.values, df.day.values)):
        raise ValueError("cache ASTMNN lệch thứ tự với khung — chạy lại data_astmnn.py --refresh")
    P = np.load(DA.DATA / "patches_w9.npy", mmap_mode="r")
    S = np.load(DA.DATA / "seq_t5.npy")
    lat, lon = np.radians(df.cell_lat.values), np.radians(df.cell_lon.values)
    doy = df.date.dt.dayofyear.values
    T = np.where(df.date.dt.is_leap_year.values, 366.0, 365.0)
    extra = np.column_stack([R_EARTH * np.cos(lat) * np.cos(lon), R_EARTH * np.cos(lat) * np.sin(lon),
                             np.sin(2 * np.pi * doy / T), np.cos(2 * np.pi * doy / T)]).astype(np.float32)
    return df, P, S, extra


LOG_VARS = ("aod", "Precip", "CAPE", "cams_pm25", "elev", "elev_std")   # lệch phải → log1p trước khi chuẩn hoá
Z_CLIP = 5.0          # cắt z: mạng nơ-ron ngoại suy tuyến tính, khác cây (thử fold 0: ô núi Lào Cai có
                      # elev_std z ≈ 13 so với ô train đồng bằng → lệch +83 µg/m³)


class FoldTensors:
    """Đầu vào đã chuẩn hoá theo ô inner-train của một fold, đặt sẵn trên GPU.

    Mỗi biến: log1p nếu thuộc LOG_VARS → z theo inner-train → cắt ±Z_CLIP → NaN thành 0 (= TB).
    Kênh `aod_valid` giữ nguyên 0/1.
    """

    def __init__(self, df, P, S, extra, tr_rows, device):
        nv = len(D.X_VARS)
        log_idx = [D.X_VARS.index(v) for v in LOG_VARS]
        X = df[D.X_VARS].to_numpy(np.float32)
        mask = np.isfinite(X[:, D.X_VARS.index("aod")]).astype(np.float32)[:, None]
        Xt = X.copy()
        Xt[:, log_idx] = np.log1p(np.maximum(Xt[:, log_idx], 0))
        mu, sd = np.nanmean(Xt[tr_rows], 0), np.nanstd(Xt[tr_rows], 0)
        sd = np.where(sd > 0, sd, 1.0)
        emu, esd = extra[tr_rows, :2].mean(0), extra[tr_rows, :2].std(0)
        y = df[D.PM].to_numpy(np.float32)
        self.ymu, self.ysd = float(y[tr_rows].mean()), float(y[tr_rows].std())

        def prep(a, axis_c):
            out = np.moveaxis(np.array(a, dtype=np.float32, copy=True), axis_c, -1)   # kênh ra cuối (view)
            out[..., log_idx] = np.log1p(np.maximum(out[..., log_idx], 0))
            out[..., :nv] = np.clip((out[..., :nv] - mu) / sd, -Z_CLIP, Z_CLIP)
            np.nan_to_num(out, nan=0.0, copy=False)
            return np.ascontiguousarray(np.moveaxis(out, -1, axis_c))

        g = np.column_stack([prep(np.column_stack([X, mask]), 1),
                             (extra[:, :2] - emu) / esd, extra[:, 2:]]).astype(np.float32)
        self.P = torch.from_numpy(prep(P, 1)).to(device)                 # (N, C, 9, 9)
        self.S = torch.from_numpy(prep(S, 2)).to(device)                 # (N, 5, C)
        self.G = torch.from_numpy(g).to(device)
        self.Y = torch.from_numpy((y - self.ymu) / self.ysd).to(device)
        self.c_in, self.g_in = self.P.shape[1], self.G.shape[1]

    def batch(self, i, w, t):
        h, c = w // 2, DA.W_MAX // 2
        return (self.P[i, :, c - h:c + h + 1, c - h:c + h + 1], self.S[i, DA.T_MAX - t:], self.G[i])


@torch.no_grad()
def predict(model, ft: FoldTensors, rows: torch.Tensor, w: int, t: int, bs: int = 2048) -> np.ndarray:
    model.eval()
    out = [model(*ft.batch(rows[b:b + bs], w, t)) for b in range(0, len(rows), bs)]
    return (torch.cat(out).float().cpu().numpy() * ft.ysd + ft.ymu) if out else np.empty(0)


def train_one(ft, tr, va, w, t, *, seed, max_epochs, patience, bs, lr, device):
    torch.manual_seed(seed)
    model = ASTMNN(ft.c_in, ft.g_in).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    sched = torch.optim.lr_scheduler.ReduceLROnPlateau(opt, factor=0.5, patience=3)
    y_va = ft.Y[va].cpu().numpy() * ft.ysd + ft.ymu
    best, best_state, best_ep, hist = np.inf, None, -1, []
    g = torch.Generator(device=device).manual_seed(seed)
    for ep in range(max_epochs):
        model.train()
        perm = tr[torch.randperm(len(tr), device=device, generator=g)]
        tot = 0.0
        for b in range(0, len(perm) - 1, bs):                     # bỏ lô 1 mẫu (BatchNorm)
            i = perm[b:b + bs]
            if len(i) < 2:
                continue
            loss = F.mse_loss(model(*ft.batch(i, w, t)), ft.Y[i])
            opt.zero_grad(set_to_none=True)
            loss.backward()
            opt.step()
            tot += loss.item() * len(i)
        rmse = float(np.sqrt(np.mean((predict(model, ft, va, w, t) - y_va) ** 2)))
        sched.step(rmse)
        hist.append((tot / len(perm), rmse))
        if rmse < best:
            best, best_ep = rmse, ep
            best_state = {k: v.detach().clone() for k, v in model.state_dict().items()}
        elif ep - best_ep >= patience:
            break
    model.load_state_dict(best_state)
    return model, best_ep, np.array(hist)


def main() -> int:
    ap = argparse.ArgumentParser(description="ASTMNN nested CV (vòng trong dò w × t)")
    ap.add_argument("--folds", nargs="+", type=int, default=list(range(D.N_OUTER)))
    ap.add_argument("--w", nargs="+", type=int, default=list(W_GRID))
    ap.add_argument("--t", nargs="+", type=int, default=list(T_GRID))
    ap.add_argument("--max-epochs", type=int, default=80)
    ap.add_argument("--patience", type=int, default=10)
    ap.add_argument("--batch-size", type=int, default=512)
    ap.add_argument("--lr", type=float, default=1e-3)
    ap.add_argument("--out", type=Path, default=HERE / "output")
    a = ap.parse_args()
    device = "cuda" if torch.cuda.is_available() else "cpu"
    out = a.out
    (out / "preds").mkdir(parents=True, exist_ok=True)
    (out / "models").mkdir(parents=True, exist_ok=True)

    df, P, S, extra = load_inputs()
    folds = D.make_folds(df)
    fold_of = df.cell_id.map(folds.set_index("cell_id").outer_fold).to_numpy()
    grid_csv = out / "inner_grid.csv"
    grid = pd.read_csv(grid_csv) if grid_csv.exists() else pd.DataFrame()
    log(f"ASTMNN: {len(df):,} ô–ngày · thiết bị {device} · fold {a.folds} · w {a.w} · t {a.t} · "
        f"tối đa {a.max_epochs} epoch, patience {a.patience}, lô {a.batch_size}")

    for k in a.folds:
        train = df[fold_of != k]
        val_cells = D.inner_val_cells(train, seed=D.RANDOM_STATE + k)
        va_rows = np.where((fold_of != k) & df.cell_id.isin(val_cells).values)[0]
        tr_rows = np.where((fold_of != k) & ~df.cell_id.isin(val_cells).values)[0]
        te_rows = np.where(fold_of == k)[0]
        ft = FoldTensors(df, P, S, extra, tr_rows, device)
        tr, va, te = (torch.as_tensor(r, device=device) for r in (tr_rows, va_rows, te_rows))
        log(f"fold {k}: train {len(tr_rows):,} ({np.unique(df.cell_id.values[tr_rows]).size} ô) · "
            f"val {len(va_rows):,} ({len(val_cells)} ô) · test {len(te_rows):,}")
        for w in a.w:
            for t in a.t:
                npz = out / "preds" / f"fold{k}_w{w}_t{t}.npz"
                if npz.exists():
                    continue
                t0 = time.time()
                model, best_ep, hist = train_one(ft, tr, va, w, t, seed=1000 * k + 10 * w + t,
                                                 max_epochs=a.max_epochs, patience=a.patience,
                                                 bs=a.batch_size, lr=a.lr, device=device)
                pv, pt = predict(model, ft, va, w, t), predict(model, ft, te, w, t)
                lam = model.branch_weights().detach().cpu().numpy()
                m = D.metrics(df[D.PM].values[va_rows], pv)
                torch.save(model.state_dict(), out / "models" / f"fold{k}_w{w}_t{t}.pt")
                np.savez(npz, val=pv, test=pt, hist=hist, lam=lam)
                row = dict(fold=k, w=w, t=t, n_train=len(tr_rows), n_val=len(va_rows), **m,
                           best_epoch=best_ep, epochs_run=len(hist), lam_s=lam[0], lam_t=lam[1], lam_g=lam[2],
                           seconds=time.time() - t0)
                grid = pd.concat([grid, pd.DataFrame([row])], ignore_index=True)
                grid.to_csv(grid_csv, index=False)
                log(f"fold {k}  w={w} t={t}: R²_val={m['R2']:.3f}  RMSE={m['RMSE']:.2f}  epoch tốt {best_ep}/{len(hist)}  "
                    f"λ(S,T,G)=({lam[0]:.2f},{lam[1]:.2f},{lam[2]:.2f})  ({time.time() - t0:.0f}s)")
                del model
                torch.cuda.empty_cache()

        g = grid[(grid.fold == k) & grid.w.isin(a.w) & grid.t.isin(a.t)]
        if len(g) < len(a.w) * len(a.t):
            continue
        b = g.loc[g.R2.idxmax()]
        wb, tb = int(b.w), int(b.t)
        z = np.load(out / "preds" / f"fold{k}_w{wb}_t{tb}.npz")
        res = df.iloc[te_rows][["cell_id", "date", "day", "region", D.PM]].copy()
        res["outer_fold"], res["pred"], res["w"], res["t"] = k, z["test"], wb, tb
        res["lam_s"], res["lam_t"], res["lam_g"] = z["lam"]
        res.to_pickle(out / f"test_fold{k}.pkl")
        sel = out / "models" / f"fold{k}_w{wb}_t{tb}.pt"
        if sel.exists():
            sel.replace(out / "models" / f"fold{k}.pt")
        for p in (out / "models").glob(f"fold{k}_w*_t*.pt"):      # chỉ giữ mô hình đã chọn
            p.unlink()
        m = D.metrics(res[D.PM], res.pred)
        log(f"fold {k}: CHỌN w={wb} t={tb} (R²_val={b.R2:.3f}) → TEST R²={m['R2']:.3f}  RMSE={m['RMSE']:.2f}")

    parts = [pd.read_pickle(p) for p in sorted(out.glob("test_fold*.pkl"))]
    if parts:
        allp = pd.concat(parts, ignore_index=True)
        per_fold = {int(f): {**D.metrics(g[D.PM], g.pred), "w": int(g.w.iloc[0]), "t": int(g.t.iloc[0]),
                             "lam": [float(g.lam_s.iloc[0]), float(g.lam_t.iloc[0]), float(g.lam_g.iloc[0])]}
                    for f, g in allp.groupby("outer_fold")}
        summary = dict(model="ASTMNN", folds_done=sorted(per_fold), pooled=D.metrics(allp[D.PM], allp.pred),
                       per_fold=per_fold, w_grid=a.w, t_grid=a.t, channels=DA.CHANNELS,
                       train=dict(max_epochs=a.max_epochs, patience=a.patience, batch_size=a.batch_size, lr=a.lr),
                       created=datetime.now().isoformat(timespec="seconds"))
        (out / "metrics.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
        p = summary["pooled"]
        log(f"GỘP {len(per_fold)} fold: R²={p['R2']:.3f}  R²_paper={p['R2_paper']:.3f}  RMSE={p['RMSE']:.2f}  RPE={p['RPE']:.1f}%")
    log("xong")
    return 0


if __name__ == "__main__":
    sys.exit(main())
