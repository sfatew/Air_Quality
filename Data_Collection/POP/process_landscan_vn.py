"""
LandScan VN 30″ (download_landscan_vn.py) → dân số trên lưới 0.05° của dự án.

Lưới đọc từ ``AOD_map/stage_a/config.py`` (như divide_grid.py / dem_005.nc):
(row, col, cell_id) trùng khít lưới AOD. 1 ô 0.05° = đúng 6×6 pixel LandScan.

Mỗi ô, mỗi năm:
    pop          tổng người trong ô (pixel nodata = biển tính là 0)
    pop_density  pop / diện tích ô (người/km²), diện tích ô trên cầu
                 R = 6371.0088 km: R² · Δλ · (sin φ_bắc − sin φ_nam)
    n_pix        số pixel LandScan có dữ liệu (0..36); 0 = biển khơi → pop = NaN

In : /home/slow_data/Air_Quality/LandScan/vietnam/raw/landscan_vn_<năm>.tif
Out: /home/slow_data/Air_Quality/LandScan/vietnam/grid_005/landscan_005.nc
     dims (year, lat, lon) = (n, 310, 160), lat Bắc → Nam như dem_005.nc

Chạy:  python process_landscan_vn.py [--years ...]
"""
import argparse
import importlib.util
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import rasterio
import xarray as xr

REPO           = Path(__file__).resolve().parents[2]
STAGE_A_CONFIG = REPO / "AOD_map" / "stage_a" / "config.py"
IN_DIR  = Path("/home/slow_data/Air_Quality/LandScan/vietnam/raw")
OUT_NC  = Path("/home/slow_data/Air_Quality/LandScan/vietnam/grid_005/landscan_005.nc")
PIX = 1 / 120
R_KM = 6371.0088

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)


def load_cfg():
    spec = importlib.util.spec_from_file_location("_stage_a_config", STAGE_A_CONFIG)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def block_sum(a: np.ndarray, k: int) -> np.ndarray:
    ny, nx = a.shape
    return a.reshape(ny // k, k, nx // k, k).sum(axis=(1, 3))


def cell_area_km2(cfg) -> np.ndarray:
    """(nlat, nlon) diện tích ô trên cầu."""
    north = np.deg2rad(cfg.LATS + cfg.GRID_RES / 2)
    south = np.deg2rad(cfg.LATS - cfg.GRID_RES / 2)
    a = R_KM ** 2 * np.deg2rad(cfg.GRID_RES) * (np.sin(north) - np.sin(south))
    return np.repeat(a[:, None], cfg.NLON, axis=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, nargs="*", help="mặc định: mọi file trong IN_DIR")
    ap.add_argument("--out", type=Path, default=OUT_NC)
    args = ap.parse_args()

    cfg = load_cfg()
    k = int(round(cfg.GRID_RES / PIX))                              # 6
    if not np.isclose(k * PIX, cfg.GRID_RES):
        sys.exit(f"GRID_RES {cfg.GRID_RES} không là bội của 30″")

    files = {int(re.search(r"(\d{4})", p.stem).group(1)): p
             for p in sorted(IN_DIR.glob("landscan_vn_*.tif"))}
    years = sorted(args.years or files)
    missing = [y for y in years if y not in files]
    if missing:
        sys.exit(f"Thiếu file năm {missing} trong {IN_DIR} — chạy download_landscan_vn.py trước")

    area = cell_area_km2(cfg)
    pop, npix = [], []
    for y in years:
        with rasterio.open(files[y]) as src:
            t = src.transform
            want = (PIX, 0.0, cfg.LON_MIN, 0.0, -PIX, cfg.LAT_MAX)
            if src.crs is None or src.crs.to_epsg() != 4326:
                raise ValueError(f"{files[y].name}: CRS {src.crs} ≠ EPSG:4326")
            # rtol=0: isclose mặc định chấp nhận lệch gốc ~1e-3° (~110 m)
            if (src.height, src.width) != (cfg.NLAT * k, cfg.NLON * k) or not np.allclose(
                    [t.a, t.b, t.c, t.d, t.e, t.f], want, rtol=0, atol=1e-9):
                raise ValueError(f"{files[y].name}: lưới khác 30″ khớp Stage A ({t})")
            if src.nodata is None:
                raise ValueError(f"{files[y].name}: thiếu nodata — không tách được biển")
            m = src.read(1, masked=True)                  # đúng cả khi nodata là NaN
            a, valid = m.filled(0), ~np.ma.getmaskarray(m)
        n = block_sum(valid.astype(np.int32), k)
        p = block_sum(np.where(valid, a, 0).astype(np.float64), k)
        pop.append(np.where(n > 0, p, np.nan))
        npix.append(n)
        log.info("%d: tổng %.2f triệu người, %d ô có dữ liệu", y, np.nansum(pop[-1]) / 1e6, int((n > 0).sum()))

    pop = np.stack(pop)
    nlat, nlon = cfg.NLAT, cfg.NLON
    ds = xr.Dataset(
        {"pop": (("year", "lat", "lon"), pop.astype(np.float32)),
         "pop_density": (("year", "lat", "lon"), (pop / area).astype(np.float32)),
         "n_pix": (("year", "lat", "lon"), np.stack(npix).astype(np.int16)),
         "cell_area_km2": (("lat", "lon"), area.astype(np.float32)),
         "cell_id": (("lat", "lon"), np.arange(nlat * nlon, dtype=np.int32).reshape(nlat, nlon))},
        coords={"year": years, "lat": cfg.LATS, "lon": cfg.LONS},
    )
    ds["pop"].attrs.update(units="persons", long_name="LandScan ambient population summed over cell")
    ds["pop_density"].attrs.update(units="persons km-2", long_name="pop / cell_area_km2")
    ds["n_pix"].attrs.update(long_name=f"valid 30-arcsec LandScan pixels in cell (max {k * k})")
    ds["cell_id"].attrs["long_name"] = "cell_id = row * nlon + col, matches divide_grid.py"
    ds.attrs.update(
        title="LandScan Global ambient population on 0.05 deg WGS-84 grid",
        grid=f"{cfg.LON_MIN}-{cfg.LON_MAX}E, {cfg.LAT_MIN}-{cfg.LAT_MAX}N, {cfg.GRID_RES} deg, "
             f"{nlat}x{nlon}; matches divide_grid.py / AOD_map/stage_a/config.py",
        aggregation=f"sum of the {k}x{k} native 30-arcsec pixels; nodata (sea) counted as 0",
        source=str(IN_DIR), missing="NaN where n_pix = 0 (open sea)",
        created=datetime.now().isoformat(timespec="seconds"),
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.out.with_suffix(".nc.part")
    ds.to_netcdf(tmp, encoding={v: {"zlib": True, "complevel": 4} for v in ds.data_vars})
    tmp.rename(args.out)
    log.info("Ghi %s", args.out)


if __name__ == "__main__":
    main()
