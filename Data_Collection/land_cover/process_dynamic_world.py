"""
Dynamic World số pixel 0.01° (download_dynamic_world.py) → % lớp phủ đất trên lưới 0.05°.

Lưới KHÔNG định nghĩa lại: đọc LAT/LON/GRID_RES từ ``AOD_map/stage_a/config.py``
(giống ``pm25_map/features/build_grid/divide_grid.py``), nên (row, col, cell_id)
trùng khít lưới AOD / DEM grid_005. Mỗi ô 0.05° = 5×5 ô 0.01° = 250 000 pixel nền ≈10 m.

Mỗi ô, mỗi năm:
    pct_<lớp>   = 100 · (số pixel nền có nhãn mode = lớp) / (số pixel nền hợp lệ)
                  9 lớp cộng lại = 100 %. NaN khi ô không có pixel hợp lệ nào.
    valid_frac  = pixel hợp lệ / 250 000  (biển khơi, ngoài vệt quét S2 → thấp / 0)
    obs_mean    = số cảnh Dynamic World trung bình trên mỗi pixel hợp lệ (QC mây)
Ô ven biển: Dynamic World vẫn gán nhãn vùng biển gần bờ trong vệt S2 là `water`,
nên pct_water ô ven biển bao gồm cả biển. Lọc theo valid_frac / in_vietnam ở bước sau.
Vùng tính: chỉ trong ranh giới VN (GADM) nới 0.1° (xem download_dynamic_world.py);
ô ngoài vùng → valid_frac = 0, pct_* = NaN; ô cắt mép vùng → valid_frac < 1.

In : /home/slow_data/Air_Quality/land_cover/dynamic_world/counts_001deg/dw_counts_<năm>.tif
Out: /home/slow_data/Air_Quality/land_cover/dynamic_world/grid_005/dw_landcover_005.nc
     dims (year, lat, lon) = (n, 310, 160), lat Bắc → Nam như dem_005.nc

Chạy:  python process_dynamic_world.py   [--years 2020 2021 ...]
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
IN_DIR  = Path("/home/slow_data/Air_Quality/land_cover/dynamic_world/counts_001deg")
OUT_NC  = Path("/home/slow_data/Air_Quality/land_cover/dynamic_world/grid_005/dw_landcover_005.nc")
FINE_RES = 0.01
BASE_PER_FINE = 10_000          # pixel nền 1e-4° trong mỗi ô 0.01°

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)


def load_cfg():
    spec = importlib.util.spec_from_file_location("_stage_a_config", STAGE_A_CONFIG)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def read_year(path: Path, cfg) -> tuple[dict[str, np.ndarray], dict]:
    """Đọc GeoTIFF 0.01°, kiểm đúng lưới, trả {band: (ny, nx)} và tags."""
    with rasterio.open(path) as src:
        t = src.transform
        ny = int(round((cfg.LAT_MAX - cfg.LAT_MIN) / FINE_RES))
        nx = int(round((cfg.LON_MAX - cfg.LON_MIN) / FINE_RES))
        want = (FINE_RES, 0.0, cfg.LON_MIN, 0.0, -FINE_RES, cfg.LAT_MAX)
        if src.crs is None or src.crs.to_epsg() != 4326:
            raise ValueError(f"{path.name}: CRS {src.crs} ≠ EPSG:4326")
        # rtol=0: isclose mặc định chấp nhận lệch gốc ~1e-3° (~110 m)
        if (src.height, src.width) != (ny, nx) or not np.allclose(
                [t.a, t.b, t.c, t.d, t.e, t.f], want, rtol=0, atol=1e-9):
            raise ValueError(f"{path.name}: lưới khác 0.01° của Stage A ({src.width}x{src.height}, {t})")
        if list(src.descriptions[-2:]) != ["valid", "n_obs"]:
            raise ValueError(f"{path.name}: thiếu band valid/n_obs ({src.descriptions})")
        bands = {d: src.read(i + 1).astype(np.float64) for i, d in enumerate(src.descriptions)}
        return bands, src.tags()


def block_sum(a: np.ndarray, k: int) -> np.ndarray:
    ny, nx = a.shape
    return a.reshape(ny // k, k, nx // k, k).sum(axis=(1, 3))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, nargs="*", help="mặc định: mọi file trong IN_DIR")
    ap.add_argument("--out", type=Path, default=OUT_NC)
    args = ap.parse_args()

    cfg = load_cfg()
    k = int(round(cfg.GRID_RES / FINE_RES))                       # 5
    if not np.isclose(k * FINE_RES, cfg.GRID_RES):
        sys.exit(f"GRID_RES {cfg.GRID_RES} không chia hết cho {FINE_RES}")
    full = BASE_PER_FINE * k * k                                   # 250 000

    files = {int(re.search(r"(\d{4})", p.stem).group(1)): p
             for p in sorted(IN_DIR.glob("dw_counts_*.tif"))}
    years = sorted(args.years or files)
    missing = [y for y in years if y not in files]
    if missing:
        sys.exit(f"Thiếu file năm {missing} trong {IN_DIR} — chạy download_dynamic_world.py trước")

    classes = None
    out = {}
    meta = {}
    for y in years:
        bands, tags = read_year(files[y], cfg)
        cls_y = tags["classes"].split(",")
        if classes is None:
            classes = cls_y
        elif cls_y != classes:
            sys.exit(f"{y}: thứ tự lớp khác các năm trước")
        cnt = {b: block_sum(a, k) for b, a in bands.items()}
        valid = cnt["valid"]
        with np.errstate(invalid="ignore", divide="ignore"):
            for c in classes:
                out.setdefault(f"pct_{c}", []).append(np.where(valid > 0, 100 * cnt[c] / valid, np.nan))
            out.setdefault("obs_mean", []).append(np.where(valid > 0, cnt["n_obs"] / valid, np.nan))
        out.setdefault("valid_frac", []).append(valid / full)
        meta[y] = (tags.get("date_start"), tags.get("date_end_exclusive"), tags.get("n_images"))
        log.info("%d: ô có dữ liệu %d/%d, kỳ %s→%s, %s cảnh", y, int((valid > 0).sum()),
                 valid.size, *meta[y])

    nlat, nlon = cfg.NLAT, cfg.NLON
    coords = {"year": years, "lat": cfg.LATS, "lon": cfg.LONS}
    ds = xr.Dataset(
        {v: (("year", "lat", "lon"), np.stack(a).astype(np.float32)) for v, a in out.items()},
        coords=coords,
    )
    ds["cell_id"] = (("lat", "lon"), np.arange(nlat * nlon, dtype=np.int32).reshape(nlat, nlon))
    ds["cell_id"].attrs["long_name"] = "cell_id = row * nlon + col, matches divide_grid.py"
    for c in classes:
        ds[f"pct_{c}"].attrs.update(units="%", long_name=f"share of valid ~10 m pixels whose "
                                                         f"annual mode label is {c}")
    ds["valid_frac"].attrs.update(units="1", long_name="valid ~10 m pixels / 250000")
    ds["obs_mean"].attrs.update(units="scenes", long_name="mean # Dynamic World scenes per valid pixel")
    ds["date_start"] = ("year", [meta[y][0] or "" for y in years])
    ds["date_end_exclusive"] = ("year", [meta[y][1] or "" for y in years])
    ds["n_images"] = ("year", [int(meta[y][2]) if meta[y][2] else -1 for y in years])
    ds.attrs.update(
        title="Google Dynamic World V1 land-cover fractions on 0.05 deg WGS-84 grid",
        grid=f"{cfg.LON_MIN}-{cfg.LON_MAX}E, {cfg.LAT_MIN}-{cfg.LAT_MAX}N, {cfg.GRID_RES} deg, "
             f"{nlat}x{nlon}; matches divide_grid.py / AOD_map/stage_a/config.py",
        aggregation="per ~1e-4 deg pixel: mode of `label` over the year; per cell: class share "
                    "of valid pixels",
        source=str(IN_DIR),
        region="computed only inside GADM Vietnam level-0 buffered 0.1 deg",
        missing="NaN where valid_frac = 0 (outside region or no Dynamic World pixel)",
        created=datetime.now().isoformat(timespec="seconds"),
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.out.with_suffix(".nc.part")
    ds.to_netcdf(tmp, encoding={v: {"zlib": True, "complevel": 4} for v in ds.data_vars
                                if ds[v].dtype != object and ds[v].dtype.kind != "U"})
    tmp.rename(args.out)

    s = sum(ds[f"pct_{c}"] for c in classes)
    ok = np.isfinite(s)
    log.info("Ghi %s · tổng %% lớp ∈ [%.3f, %.3f]", args.out, float(s.where(ok).min()), float(s.where(ok).max()))


if __name__ == "__main__":
    main()
