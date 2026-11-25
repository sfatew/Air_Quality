"""
Tải LandScan Global (dân số ambient 24h, người/pixel) cắt theo hộp bao Việt Nam.

Nguồn : projects/sat-io/open-datasets/ORNL/LANDSCAN_GLOBAL (GEE Community Catalog)
        Pixel 30″ = 1/120° (~0,9 km), gốc (-180, 90); int32, nodata = biển.
Năm   : mặc định 2020..2024 (2024 là năm mới nhất trên GEE tính tới 11/2026).
Hộp   : 102–110°E, 8–23,5°N = lưới Stage A. 102 và 23,5 là bội của 1/120 nên
        ảnh cắt KHỚP pixel gốc (không resample); 1 ô 0.05° = đúng 6×6 pixel.
Lưu   : /home/slow_data/Air_Quality/LandScan/vietnam/raw/landscan_vn_<năm>.tif
        (giữ nguyên giá trị gốc, nodata = -2147483647 như file ORNL).
        KHÔNG đụng tới bản global LandScan/landscan-global-*.tif mà Stage B đang đọc.

Chỉ cần quyền đọc GEE (interactive getPixels), mỗi năm ~7 MB, vài giây.
Chạy:  python download_landscan_vn.py [--years 2020 2021 ...] [--force]
"""
import argparse
import logging
from pathlib import Path

import ee
import numpy as np
import rasterio
from rasterio.transform import from_origin

PROJECT   = "thesis-498612"
ASSET_DIR = "projects/sat-io/open-datasets/ORNL/LANDSCAN_GLOBAL"
OUT_DIR   = Path("/home/slow_data/Air_Quality/LandScan/vietnam/raw")

LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 102.0, 110.0, 8.0, 23.5
PIX = 1 / 120
NX = int(round((LON_MAX - LON_MIN) / PIX))    # 960
NY = int(round((LAT_MAX - LAT_MIN) / PIX))    # 1860
NODATA = -2147483647

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)


def fetch(year: int) -> np.ndarray:
    a = ee.data.getPixels({
        "assetId": f"{ASSET_DIR}/landscan-global-{year}",
        "fileFormat": "NUMPY_NDARRAY", "bandIds": ["b1"],
        "grid": {"dimensions": {"width": NX, "height": NY},
                 "affineTransform": {"scaleX": PIX, "shearX": 0, "translateX": LON_MIN,
                                     "shearY": 0, "scaleY": -PIX, "translateY": LAT_MAX},
                 "crsCode": "EPSG:4326"},
    })
    return np.asarray(a["b1"], dtype=np.int32)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, nargs="+", default=list(range(2020, 2025)))
    ap.add_argument("--force", action="store_true", help="tải lại cả năm đã có file")
    args = ap.parse_args()

    ee.Initialize(project=PROJECT)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for y in args.years:
        out = OUT_DIR / f"landscan_vn_{y}.tif"
        if out.exists() and not args.force:
            log.info("%d: đã có %s, bỏ qua", y, out.name)
            continue
        # getPixels trên asset gốc giữ nguyên nodata -2147483647 ở biển (đã đối chiếu
        # 2024 với landscan-global-2024.tif local: trùng 100 % từng pixel).
        arr = fetch(y)
        tmp = out.with_suffix(".tif.part")
        with rasterio.open(tmp, "w", driver="GTiff", width=NX, height=NY, count=1, dtype="int32",
                           crs="EPSG:4326", transform=from_origin(LON_MIN, LAT_MAX, PIX, PIX),
                           nodata=NODATA, compress="deflate", tiled=True) as dst:
            dst.write(arr, 1)
            dst.update_tags(source=f"{ASSET_DIR}/landscan-global-{y}", units="persons per pixel",
                            year=str(y))
        tmp.rename(out)
        v = arr[arr != NODATA]
        log.info("%d → %s · tổng %.2f triệu người trong hộp bao, max %d/pixel",
                 y, out.name, v[v > 0].sum() / 1e6, v.max())


if __name__ == "__main__":
    main()
