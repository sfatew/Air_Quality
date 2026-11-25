"""Vẽ bản đồ lưới 0.05° ra PNG để kiểm tra bằng mắt.

Đọc GeoPackage do ``divide_grid.py`` sinh ra và dựng một hình hai khung:

  Khung trái  — toàn miền 102–110°E / 8–23.5°N, trục chia theo độ.
  Khung phải  — phóng to đồng bằng sông Hồng, vẽ rõ từng cạnh ô 0.05°.

Ô được tô theo ba trạng thái phân loại của lưới (xem divide_grid.py):
    trong lãnh thổ   tâm ô nằm trong một tỉnh
    chỉ chạm         ô cắt lãnh thổ nhưng tâm rơi ra biển
    ngoài            để trống, chỉ còn nền

Phần tô dùng ``imshow`` trên mảng 310×160 thay vì vẽ 49 600 polygon: ảnh ra
đúng từng ô, sắc nét, và nhanh hơn hai bậc độ lớn.

Thang màu: một tông lam, bậc 250 → 450 của ramp tuần tự, đã qua
``validate_palette.js --ordinal`` (đơn sắc, độ sáng đơn điệu, đầu nhạt đạt
2.06:1 so với nền).

    python plot_grid.py
    python plot_grid.py --zoom 108.0 110.0 15.5 17.0 --out output/zoom_mien_trung.png
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib as mpl
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D
from matplotlib.colors import BoundaryNorm, ListedColormap
from matplotlib.patches import Patch, Rectangle

sys.path.insert(0, str(Path(__file__).resolve().parent))
import divide_grid as DG  # noqa: E402  — nguồn duy nhất của REPO + lưới

REPO = DG.find_repo()
GPKG = REPO / "pm25_map" / "output" / "vn_grid_005deg.gpkg"
GADM_L1 = REPO / "Data_Collection" / "GADM_Vietnam" / "gadm41_VNM_1.shp"
DEFAULT_OUT = REPO / "pm25_map" / "output" / "vn_grid_005deg.png"

# Bảng màu (dataviz reference palette — ramp lam, bậc ordinal).
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
INK_MUTED = "#8c8b85"
C_TOUCH = "#86b6ef"   # blue 250 — ô chỉ chạm lãnh thổ
C_INSIDE = "#2a78d6"  # blue 450 — ô có tâm trong lãnh thổ
GRIDLINE = "#dcdbd6"

# Vùng phóng to mặc định: ven biển Hải Phòng – Hạ Long. Chọn chỗ có bờ biển và
# đảo nhỏ thay vì giữa đồng bằng, vì chỉ ở đây mới thấy được cả ba trạng thái
# phân loại ô cùng lúc — giữa đất liền thì ô nào cũng "trong lãnh thổ".
DEFAULT_ZOOM = (106.20, 107.30, 20.35, 21.15)


def load() -> tuple[gpd.GeoDataFrame, gpd.GeoSeries, gpd.GeoSeries, dict]:
    g = gpd.read_file(GPKG, layer="grid")
    prov = gpd.read_file(GADM_L1).to_crs("EPSG:4326").geometry
    # Biên quốc gia = hợp của 63 tỉnh; vẽ đậm hơn ranh giới tỉnh để mắt tách
    # được "đâu là lãnh thổ" (thứ quyết định màu ô) khỏi "đâu là ranh giới
    # hành chính" (chỉ là thông tin nền).
    country = gpd.GeoSeries([prov.union_all()], crs="EPSG:4326")
    res = float(round(g["lon_max"].iloc[0] - g["lon_min"].iloc[0], 10))
    spec = {
        "res": res,
        "lon_min": float(g["lon_min"].min()), "lon_max": float(g["lon_max"].max()),
        "lat_min": float(g["lat_min"].min()), "lat_max": float(g["lat_max"].max()),
        "nlat": int(g["row"].max()) + 1, "nlon": int(g["col"].max()) + 1,
    }
    return g, prov, country, spec


def classify_raster(g: gpd.GeoDataFrame, spec: dict) -> np.ndarray:
    """Mảng (nlat, nlon): 0 ngoài, 1 chỉ chạm, 2 trong lãnh thổ. Hàng 0 ở cực bắc."""
    arr = np.zeros((spec["nlat"], spec["nlon"]), dtype=np.int8)
    r = g["row"].to_numpy().astype(int)
    c = g["col"].to_numpy().astype(int)
    arr[r, c] = np.where(g["in_vietnam"].to_numpy(), 2,
                         np.where(g["touches_vietnam"].to_numpy(), 1, 0))
    return arr


def _degree_axes(ax, lon0, lon1, lat0, lat1, step_lon, step_lat, labelsize=9):
    """Trục chia theo độ, có hậu tố °E / °N — 'thang độ' để đối chiếu toạ độ."""
    xt = np.arange(np.ceil(lon0 / step_lon) * step_lon, lon1 + 1e-9, step_lon)
    yt = np.arange(np.ceil(lat0 / step_lat) * step_lat, lat1 + 1e-9, step_lat)
    ax.set_xticks(xt); ax.set_yticks(yt)
    fmt = "{:g}°E" if step_lon >= 1 else "{:.2f}°E"
    fmty = "{:g}°N" if step_lat >= 1 else "{:.2f}°N"
    ax.set_xticklabels([fmt.format(v) for v in xt], fontsize=labelsize, color=INK_2)
    ax.set_yticklabels([fmty.format(v) for v in yt], fontsize=labelsize, color=INK_2)
    ax.tick_params(length=3, width=0.6, color=INK_MUTED)
    for s in ax.spines.values():
        s.set_color(INK_MUTED); s.set_linewidth(0.6)


def _cell_lines(ax, spec, lon0, lon1, lat0, lat1, color=GRIDLINE, lw=0.4):
    """Vẽ cạnh ô 0.05° trong khung nhìn (chỉ dùng khi phóng to đủ gần)."""
    r = spec["res"]
    xs = np.arange(np.ceil(lon0 / r) * r, lon1 + 1e-9, r)
    ys = np.arange(np.ceil(lat0 / r) * r, lat1 + 1e-9, r)
    segs = [[(x, lat0), (x, lat1)] for x in xs] + [[(lon0, y), (lon1, y)] for y in ys]
    ax.add_collection(LineCollection(segs, colors=color, linewidths=lw, zorder=3))


def draw(out: Path, zoom: tuple[float, float, float, float]) -> dict:
    g, prov, country, spec = load()
    arr = classify_raster(g, spec)
    extent = [spec["lon_min"], spec["lon_max"], spec["lat_min"], spec["lat_max"]]

    cmap = ListedColormap([C_TOUCH, C_INSIDE])
    norm = BoundaryNorm([0.5, 1.5, 2.5], cmap.N)
    masked = np.ma.masked_where(arr == 0, arr)
    aspect = 1.0 / np.cos(np.radians(0.5 * (spec["lat_min"] + spec["lat_max"])))

    fig = plt.figure(figsize=(12.5, 9.6), facecolor=SURFACE)
    gs = fig.add_gridspec(2, 2, width_ratios=[1.0, 1.45], height_ratios=[1.0, 0.1],
                          wspace=0.17, hspace=0.05)
    axA = fig.add_subplot(gs[:, 0])
    axB = fig.add_subplot(gs[0, 1])
    axL = fig.add_subplot(gs[1, 1]); axL.axis("off")

    # ── Khung trái: toàn miền ────────────────────────────────────────────────
    for ax in (axA, axB):
        ax.set_facecolor(SURFACE)
    axA.imshow(masked, cmap=cmap, norm=norm, extent=extent,
               origin="upper", interpolation="nearest", zorder=2)
    prov.boundary.plot(ax=axA, color=INK_2, linewidth=0.22, alpha=0.55, zorder=4)
    country.boundary.plot(ax=axA, color=INK, linewidth=0.55, zorder=5)
    axA.set_xlim(extent[0], extent[1]); axA.set_ylim(extent[2], extent[3])
    axA.set_aspect(aspect)
    _degree_axes(axA, *extent, step_lon=2, step_lat=2)
    axA.set_title(f"Toàn miền · {spec['nlat']}×{spec['nlon']} ô",
                  fontsize=11, color=INK, pad=8, loc="left")

    # Khung chỉ vùng phóng to
    z0, z1, z2, z3 = zoom
    axA.add_patch(Rectangle((z0, z2), z1 - z0, z3 - z2, fill=False,
                            edgecolor="#eb6834", linewidth=1.3, zorder=5))

    # ── Khung phải: phóng to ─────────────────────────────────────────────────
    axB.imshow(masked, cmap=cmap, norm=norm, extent=extent,
               origin="upper", interpolation="nearest", zorder=2)
    _cell_lines(axB, spec, z0, z1, z2, z3)
    prov.boundary.plot(ax=axB, color=INK_2, linewidth=0.5, alpha=0.6, zorder=4)
    country.boundary.plot(ax=axB, color=INK, linewidth=1.1, zorder=5)
    axB.set_xlim(z0, z1); axB.set_ylim(z2, z3)
    axB.set_aspect(aspect)
    _degree_axes(axB, z0, z1, z2, z3, step_lon=0.25, step_lat=0.25)
    for s in axB.spines.values():
        s.set_color("#eb6834"); s.set_linewidth(1.3)
    km_lat = spec["res"] * 111.195
    km_lon = km_lat * np.cos(np.radians(0.5 * (z2 + z3)))
    axB.set_title(f"Phóng to · 1 ô = {spec['res']}° × {spec['res']}° "
                  f"≈ {km_lon:.1f} × {km_lat:.1f} km",
                  fontsize=11, color=INK, pad=8, loc="left")

    # ── Chú giải ─────────────────────────────────────────────────────────────
    n_in = int((arr == 2).sum()); n_touch = int((arr == 1).sum()); n_all = arr.size
    handles = [
        Patch(facecolor=C_INSIDE, edgecolor="none",
              label=f"Tâm ô trong lãnh thổ — {n_in:,} ô ({100*n_in/n_all:.1f}%)"),
        Patch(facecolor=C_TOUCH, edgecolor="none",
              label=f"Chỉ chạm lãnh thổ — {n_touch:,} ô"),
        Patch(facecolor=SURFACE, edgecolor=INK_MUTED, linewidth=0.6,
              label=f"Ngoài lãnh thổ — {n_all - n_in - n_touch:,} ô"),
        Line2D([0], [0], color=INK, linewidth=1.1, label="Biên quốc gia"),
        Line2D([0], [0], color=INK_2, linewidth=0.5, alpha=0.6,
               label="Ranh giới tỉnh"),
    ]
    axL.legend(handles=handles, loc="upper left", frameon=False, ncol=1,
               fontsize=9.5, labelcolor=INK_2, handlelength=1.4,
               handleheight=1.0, borderpad=0, labelspacing=0.55)

    fig.suptitle("Lưới WGS-84 (EPSG:4326) 0,05° × 0,05° trên lãnh thổ Việt Nam",
                 fontsize=14.5, color=INK, x=0.065, ha="left", y=0.975)
    fig.text(0.065, 0.938,
             f"{n_all:,} ô · hộp bao {extent[0]:g}–{extent[1]:g}°E, "
             f"{extent[2]:g}–{extent[3]:g}°N · khớp lưới Stage A · "
             f"ranh giới GADM 4.1 (không gồm Hoàng Sa / Trường Sa)",
             fontsize=9.5, color=INK_2, ha="left")

    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=170, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    return {"path": out, "n_in": n_in, "n_touch": n_touch, "n_all": n_all}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--zoom", nargs=4, type=float, default=list(DEFAULT_ZOOM),
                    metavar=("LON0", "LON1", "LAT0", "LAT1"))
    args = ap.parse_args(argv)
    mpl.rcParams["font.family"] = "DejaVu Sans"
    info = draw(Path(args.out), tuple(args.zoom))
    print(f"[plot] {info['n_all']:,} ô | trong lãnh thổ {info['n_in']:,} | "
          f"chỉ chạm {info['n_touch']:,}")
    print(f"[plot] → {info['path']}  "
          f"({Path(info['path']).stat().st_size/1e6:.2f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
