#!/usr/bin/env python3
"""Đầu vào cho ASTMNN (Wang et al. 2024, JAG 134:104145) từ lưới 0,05° theo ngày.

Bài gốc làm theo GIỜ với AOD Himawari 1 km; ở đây đổi sang NGÀY trên lưới 0,05°:
    SaM  patch w×w ô quanh ô đích (paper: w×w pixel)       → lưu w = 9, cắt 5/7 khi dò
    TaM  chuỗi t ngày gần nhất của ô đích (paper: t giờ)    → lưu t = 5, cắt 3/4 khi dò
    GM   véc-tơ biến của ô đích (+ mã hoá vị trí/thời gian, tính khi huấn luyện)
Kênh = 15 biến của `_data.X_VARS` + `aod_valid` (1 nếu ô có AOD). AOD thiếu (ngày mây) để NaN
trong cache; khi huấn luyện được điền 0 sau chuẩn hoá z, và kênh mặt nạ báo cho mạng biết
(spatial attention của SaM vốn để "nhấn thông tin hợp lệ"). DEM NaN = biển khơi → 0 m.
Chuỗi TaM ở đầu chuỗi số liệu (2020-01-01) lặp lại ngày đầu, như paper thay AOD 8:00 cho các
giờ trước đó.

Ghi ra (thư mục data/ cạnh file này), cùng thứ tự dòng với `_data.build_frame()`:
    patches_w9.npy   (N, C, 9, 9) float32
    seq_t5.npy       (N, 5, C)    float32   — bước cuối là chính ngày đích
    index.pkl        cell_id, day của từng dòng (để kiểm khớp với khung)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))            # model/
import _data as D  # noqa: E402

G = D.G
DATA = HERE / "data"
W_MAX, T_MAX = 9, 5
CHANNELS = [*D.X_VARS, "aod_valid"]
AOD_C, MASK_C = CHANNELS.index("aod"), CHANNELS.index("aod_valid")


def _load(var: str, year: int, dates: pd.DatetimeIndex) -> np.ndarray:
    """(ngày, lat, lon) float32 của MỘT biến trong một năm, theo trục ngày `dates` (của ERA5).

    Đọc từng biến một để bộ nhớ đỉnh chỉ ~1 mảng năm (~75 MB) — máy đang chạy song song GTW/global.
    """
    if var in D.ERA5_VARS:
        with xr.open_dataset(G.ERA5_GRID.format(year=year)) as e:
            return e[f"{var}_24h"].values.astype(np.float32)
    path, name = {"aod": (G.MODIS_GRID, "aod"), "cams_pm25": (G.CAMS_GRID, "PM25")}[var]
    with xr.open_dataset(path.format(year=year)) as f:
        idx = pd.DatetimeIndex(f.date.values).get_indexer(dates)
        a = f[name].values.astype(np.float32)
    out = np.full((len(dates),) + a.shape[1:], np.nan, np.float32)
    out[idx >= 0] = a[idx[idx >= 0]]
    return out


def build(refresh: bool = False) -> None:
    if (DATA / "patches_w9.npy").exists() and not refresh:
        print("đã có cache trong", DATA)
        return
    df = D.build_frame()
    n, C, h = len(df), len(CHANNELS), W_MAX // 2
    P = np.full((n, C, W_MAX, W_MAX), np.nan, np.float32)
    off = np.arange(-h, h + 1)
    cells = np.sort(df.cell_id.unique())
    cpos = {c: i for i, c in enumerate(cells)}
    crow = df.groupby("cell_id").row.first().loc[cells].values
    ccol = df.groupby("cell_id").col.first().loc[cells].values
    n_days = int(df.day.max()) + 1
    table = np.full((len(cells), n_days, C), np.nan, np.float32)     # chuỗi ngày đủ của 80 ô

    with xr.open_dataset(G.DEM_GRID) as d:              # lớp tĩnh: đọc một lần
        dem = {v: np.nan_to_num(d[v].values.astype(np.float32), nan=0.0) for v in D.DEM_VARS}
    rr = (df.row.values + h)[:, None, None] + off[None, :, None]
    cc = (df.col.values + h)[:, None, None] + off[None, None, :]
    for v, g in dem.items():
        j = CHANNELS.index(v)
        P[:, j] = np.pad(g, h, constant_values=0.0)[rr, cc]
        table[:, :, j] = g[crow, ccol][:, None]

    for y in sorted(df.year.unique()):
        sub = np.where(df.year.values == y)[0]
        with xr.open_dataset(G.ERA5_GRID.format(year=int(y))) as e:
            dates = pd.DatetimeIndex(e.date.values)
        di = dates.get_indexer(df.date.values[sub])
        dayno = (dates - D.DAY0).days.values
        ok = (dayno >= 0) & (dayno < n_days)
        for v in [x for x in D.X_VARS if x not in D.DEM_VARS]:
            j = CHANNELS.index(v)
            g = _load(v, int(y), dates)
            table[:, dayno[ok], j] = g[ok][:, crow, ccol].T
            g = np.pad(g, ((0, 0), (h, h), (h, h)), constant_values=np.nan)
            P[sub, j] = g[di[:, None, None], rr[sub], cc[sub]]
            del g
        print(f"  {y}: {len(sub):,} patch", flush=True)
    P[:, MASK_C] = np.isfinite(P[:, AOD_C]).astype(np.float32)
    table[..., MASK_C] = np.isfinite(table[..., AOD_C]).astype(np.float32)

    steps = np.arange(-(T_MAX - 1), 1)
    dd = np.clip(df.day.values[:, None] + steps[None, :], 0, n_days - 1)
    S = table[df.cell_id.map(cpos).values[:, None], dd]                # (n, T_MAX, C)

    # kiểm: tâm patch và bước cuối của chuỗi phải trùng khung đã ghép bằng _grid_common
    X = df[D.X_VARS].to_numpy(np.float32)
    for name, A in (("tâm patch", P[:, :len(D.X_VARS), h, h]), ("bước cuối chuỗi", S[:, -1, :len(D.X_VARS)])):
        bad = ~np.isclose(A, X, rtol=1e-5, atol=1e-5, equal_nan=True)
        if bad.any():
            raise ValueError(f"{name} lệch khung ở {bad.sum()} giá trị (biến {np.array(D.X_VARS)[bad.any(0)]})")
    DATA.mkdir(parents=True, exist_ok=True)
    np.save(DATA / "patches_w9.npy", P)
    np.save(DATA / "seq_t5.npy", S)
    df[["cell_id", "day"]].to_pickle(DATA / "index.pkl")
    print(f"patch {P.shape} ({P.nbytes / 1e6:.0f} MB) · chuỗi {S.shape} · AOD có ở tâm {P[:, MASK_C, h, h].mean():.1%}"
          f" · trong patch {P[:, MASK_C].mean():.1%} → {DATA}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Dựng patch/chuỗi đầu vào ASTMNN")
    ap.add_argument("--refresh", action="store_true")
    build(ap.parse_args().refresh)
