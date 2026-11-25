#!/usr/bin/env python3
"""Copernicus DEM GLO-30 → lưới 0,05° WGS-84 (lớp tĩnh, không theo ngày).

Nguồn: các ô 1°×1° gốc `DEM/raw/Copernicus_DSM_COG_10_*_DEM.tif` do
`Data_Collection/DEM/collect_copernicus_dem.py` tải — 1″ (≈ 30 m), WGS-84, độ
cao so với geoid EGM2008 (tức độ cao trên mực nước biển).

Gộp 1″ → 0,05°: thống kê theo khối, không nội suy
--------------------------------------------------
0,05° = 180″, nên mỗi ô lưới gồm đúng 180 × 180 = 32.400 pixel, và cạnh ô 1°
của Copernicus trùng cạnh ô lưới (1° = 20 ô). Mỗi ô 1° vì vậy cắt gọn thành
20 × 20 khối, không pixel nào bị chia giữa hai ô lưới và không cần trọng số.
Tâm pixel Copernicus nằm trên giây nguyên, nên cạnh ô lưới đi qua TÂM một hàng/
cột pixel. Pixel nằm trên cạnh được gán theo đúng cách bộ ô 1° chia (cột cạnh tây
và hàng cạnh bắc thuộc về ô). So với chia theo diện tích, cách này lệch nửa pixel
(~15 m), không đáng kể ở ô 5,5 km.

Đầu ra
------
    {output_dir}/dem_005.nc — chiều (lat, lon), cùng tâm ô với divide_grid.py
        elev                 TB độ cao trong ô (m)
        elev_std             độ lệch chuẩn trong ô (m) — độ gồ ghề địa hình
        elev_min, elev_max   thấp / cao nhất trong ô (m)
        n_pix                số pixel hợp lệ (đủ = 32.400)
        cell_id(lat, lon)    int32, khớp cell_id của divide_grid.py
    Ô không thuộc ô 1° nào (biển khơi, bucket không có ô): NaN, n_pix = 0.
"""

import argparse
import concurrent.futures
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import netCDF4 as nc
import numpy as np
import rasterio

sys.path.insert(0, str(Path(__file__).resolve().parent))
import divide_grid as DG  # noqa: E402  — nguồn duy nhất định nghĩa lưới

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_RAW_DIR    = Path("/home/slow_data/Air_Quality/DEM/raw")
DEFAULT_OUTPUT_DIR = Path("/home/slow_data/Air_Quality/DEM/grid_005")
TILE_GLOB = "Copernicus_DSM_COG_10_*_DEM.tif"
TILE_RE   = re.compile(r"_([NS])(\d{2})_00_([EW])(\d{3})_00_DEM")
PIX = 1 / 3600                                    # 1″
STATS = ("elev", "elev_std", "elev_min", "elev_max")


def tile_stats(path: Path, res: float) -> tuple[int, int, dict]:
    """Thống kê 20×20 khối của một ô 1°; trả (lat nam, lon tây, {stat: (k, k)})."""
    ns, la, ew, lo = TILE_RE.search(path.name).groups()
    lat_s = int(la) * (1 if ns == "N" else -1)
    lon_w = int(lo) * (1 if ew == "E" else -1)
    k = int(round(1 / res))                       # ô lưới trên mỗi cạnh ô 1°
    with rasterio.open(path) as src:
        t = src.transform
        # Kiểm hình học: tâm pixel đầu = (lon_w, lat_s + 1), bước 1″, 3600×3600
        if (src.width, src.height) != (3600, 3600) or not (
                np.isclose(t.a, PIX) and np.isclose(t.e, -PIX)
                and np.isclose(t.c + PIX / 2, lon_w) and np.isclose(t.f - PIX / 2, lat_s + 1)):
            raise ValueError(f"{path.name}: lưới pixel khác chuẩn GLO-30 ({src.width}x{src.height}, {t})")
        arr = src.read(1).astype(np.float64)
        if src.nodata is not None:
            arr[arr == src.nodata] = np.nan
    n = 3600 // k
    b = arr.reshape(k, n, k, n)
    valid = np.isfinite(b)
    cnt = valid.sum(axis=(1, 3))
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = np.where(valid, b, 0).sum(axis=(1, 3)) / cnt
        var = np.where(valid, (b - mean[:, None, :, None]) ** 2, 0).sum(axis=(1, 3)) / cnt
    out = {
        "elev": mean,
        "elev_std": np.sqrt(var),
        "elev_min": np.where(cnt > 0, np.nanmin(np.where(valid, b, np.inf), axis=(1, 3)), np.nan),
        "elev_max": np.where(cnt > 0, np.nanmax(np.where(valid, b, -np.inf), axis=(1, 3)), np.nan),
        "n_pix": cnt,
    }
    return lat_s, lon_w, out


def _worker(args):
    return tile_stats(*args)


def build(raw_dir: Path, output_dir: Path, workers: int = 3, overwrite: bool = False) -> Path:
    out = output_dir / "dem_005.nc"
    if out.exists() and not overwrite:
        logger.info(f"đã có {out} — bỏ qua (--overwrite để dựng lại)")
        return out
    tiles = sorted(raw_dir.glob(TILE_GLOB))
    if not tiles:
        raise SystemExit(f"Không có ô DEM nào ({TILE_GLOB}) trong {raw_dir}")

    spec = DG.grid_spec()
    nlat, nlon, res = spec["nlat"], spec["nlon"], spec["res"]
    k = int(round(1 / res))
    grids = {s: np.full((nlat, nlon), np.nan) for s in STATS}
    grids["n_pix"] = np.zeros((nlat, nlon), dtype=np.int64)

    logger.info(f"{len(tiles)} ô 1° trong {raw_dir}")
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as ex:
        for lat_s, lon_w, st in ex.map(_worker, [(p, res) for p in tiles]):
            # khối (i, j) của ô 1° ↔ hàng/cột lưới; cắt phần nằm ngoài hộp bao
            r0 = int(round((spec["lat_max"] - (lat_s + 1)) / res))
            c0 = int(round((lon_w - spec["lon_min"]) / res))
            i = np.arange(k); rows, cols = r0 + i, c0 + i
            ri, ci = (rows >= 0) & (rows < nlat), (cols >= 0) & (cols < nlon)
            if not ri.any() or not ci.any():
                continue
            sel = np.ix_(rows[ri], cols[ci])
            if (grids["n_pix"][sel] > 0).any():
                raise ValueError(f"ô 1° N{lat_s} E{lon_w} chồng lên ô đã ghi")
            for s, v in st.items():
                grids[s][sel] = v[np.ix_(ri, ci)]

    dlat = spec["lat_max"] - (np.arange(nlat) + 0.5) * res     # bắc → nam, như divide_grid
    dlon = spec["lon_min"] + (np.arange(nlon) + 0.5) * res
    output_dir.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".nc.tmp")
    meta = {
        "elev":     ("mean elevation above EGM2008 geoid in cell", "m"),
        "elev_std": ("standard deviation of 1-arcsec elevation in cell (terrain roughness)", "m"),
        "elev_min": ("minimum 1-arcsec elevation in cell", "m"),
        "elev_max": ("maximum 1-arcsec elevation in cell", "m"),
    }
    with nc.Dataset(tmp, "w", format="NETCDF4") as dst:
        dst.createDimension("lat", nlat)
        dst.createDimension("lon", nlon)
        vla = dst.createVariable("lat", "f8", ("lat",)); vla.units = "degrees_north"; vla[:] = dlat
        vlo = dst.createVariable("lon", "f8", ("lon",)); vlo.units = "degrees_east"; vlo[:] = dlon
        vc = dst.createVariable("cell_id", "i4", ("lat", "lon"), zlib=True, complevel=4)
        vc.long_name = "cell_id = row * nlon + col, matches divide_grid.py"
        vc[:] = np.arange(nlat * nlon, dtype=np.int32).reshape(nlat, nlon)
        for s, (long_name, units) in meta.items():
            v = dst.createVariable(s, "f4", ("lat", "lon"), zlib=True, complevel=4,
                                   fill_value=np.float32(np.nan))
            v.long_name, v.units = long_name, units
            v[:] = grids[s].astype(np.float32)
        vn = dst.createVariable("n_pix", "i4", ("lat", "lon"), zlib=True, complevel=4)
        vn.long_name = f"valid 1-arcsec pixels in cell (full = {(3600 // k) ** 2})"
        vn[:] = grids["n_pix"].astype(np.int32)

        dst.title = "Copernicus DEM GLO-30 on 0.05 deg WGS-84 grid"
        dst.grid = (f"{spec['lon_min']}-{spec['lon_max']}E, {spec['lat_min']}-{spec['lat_max']}N, "
                    f"{res} deg, {nlat}x{nlon}; matches divide_grid.py / Stage A")
        dst.aggregation = (f"block statistics of the {3600 // k}x{3600 // k} native 1-arcsec pixels "
                           "of each cell (cells align with the 1-deg tiles); no interpolation")
        dst.source = f"{raw_dir}/{TILE_GLOB} (collect_copernicus_dem.py, AWS copernicus-dem-30m)"
        dst.missing = "NaN / n_pix = 0: no GLO-30 tile (open sea)"
        dst.created = datetime.now().isoformat(timespec="seconds")
    tmp.replace(out)

    full = int((grids["n_pix"] == (3600 // k) ** 2).sum())
    logger.info(f"ô đủ pixel {full:,}/{nlat * nlon:,} · không có dữ liệu "
                f"{int((grids['n_pix'] == 0).sum()):,} · elev {np.nanmin(grids['elev']):.0f}–"
                f"{np.nanmax(grids['elev']):.0f} m · {out.stat().st_size / 1e6:.1f} MB → {out}")
    return out


def parse_args():
    p = argparse.ArgumentParser(description="Copernicus DEM GLO-30 → lưới 0,05°")
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR,
                   help="thư mục ô 1° gốc do collect_copernicus_dem.py tải")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    a = parse_args()
    build(a.raw_dir, a.output_dir, a.workers, a.overwrite)
