"""
Tải Copernicus DEM GLO-30 (1″ ≈ 30 m) dạng ô 1°×1° gốc cho hộp bao Việt Nam.

Nguồn: bucket công khai AWS Open Data `copernicus-dem-30m` (không cần tài khoản).
Ba mosaic cũ `DEM/output_{hn,hh,hcm}.tif` (Stage B đang đọc) là cắt từ chính
nguồn này — trùng 100% từng pixel với ô N21 E105 — nhưng chỉ phủ phần đất liền
Việt Nam (thiếu phía tây 104,3°E ở miền Nam như Phú Quốc, và vùng Lào / Campuchia
/ Thái Lan / Trung Quốc trong hộp bao). Script này tải đủ mọi ô giao hộp bao.

  Hộp bao : 102–110°E, 8–23,5°N (giống ERA5 / lưới Stage A)
  Ô cần   : N08…N23 × E102…E109 = 128 ô; ô toàn biển không có trên bucket
            (tileList.txt là danh sách chính thức) → bỏ qua, không phải lỗi.
  Lưu     : /home/slow_data/Air_Quality/DEM/raw/<tên ô>.tif (giữ nguyên tên gốc).
            Thư mục con riêng, nên không lọt vào glob `DEM/output_*.tif` của Stage B.

Ghi vào `<tên>.part`, kiểm chữ ký TIFF + đúng Content-Length rồi mới đổi tên thật;
chạy lại thì bỏ qua file đã đủ và hợp lệ.
"""

import argparse
import concurrent.futures
import os
import sys

import requests

BUCKET = "https://copernicus-dem-30m.s3.amazonaws.com"
# Ô theo góc tây-nam. Trung Quốc: hộp bao GADM 73,6–134,8°E, 18,2–53,6°N.
REGIONS = {
    "vietnam": dict(out_dir="/home/slow_data/Air_Quality/DEM/raw",
                    lat=range(8, 24), lon=range(102, 110)),      # 8–24°N, 102–110°E
    "china":   dict(out_dir="/home/slow_data/Air_Quality/DEM/china/raw",
                    lat=range(18, 54), lon=range(73, 135)),      # 18–54°N, 73–135°E
}
OUT_DIR = REGIONS["vietnam"]["out_dir"]
MAX_WORKERS = 4

TIFF_MAGIC = (b"II*\x00", b"MM\x00*", b"II+\x00", b"MM\x00+")   # TIFF / BigTIFF


def tile_name(lat: int, lon: int) -> str:
    return f"Copernicus_DSM_COG_10_N{lat:02d}_00_E{lon:03d}_00_DEM"


def is_valid_tif(path: str, expected_size: int | None = None) -> bool:
    try:
        if expected_size is not None and os.path.getsize(path) != expected_size:
            return False
        with open(path, "rb") as fh:
            return fh.read(4) in TIFF_MAGIC
    except OSError:
        return False


def download(name: str, session: requests.Session) -> str:
    url = f"{BUCKET}/{name}/{name}.tif"
    path = os.path.join(OUT_DIR, f"{name}.tif")
    size = int(session.head(url, timeout=30).headers["Content-Length"])
    if is_valid_tif(path, size):
        return "skip"
    tmp = path + ".part"
    try:
        with session.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fh.write(chunk)
        if not is_valid_tif(tmp, size):
            raise RuntimeError("không phải TIFF hoặc sai kích thước (tải dở?)")
        os.replace(tmp, path)
        return "ok"
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def main() -> int:
    global OUT_DIR
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", choices=list(REGIONS), default="vietnam")
    region = REGIONS[ap.parse_args().region]
    OUT_DIR = region["out_dir"]
    os.makedirs(OUT_DIR, exist_ok=True)
    session = requests.Session()
    available = set(session.get(f"{BUCKET}/tileList.txt", timeout=60).text.split())
    wanted = [tile_name(la, lo) for la in region["lat"] for lo in region["lon"]]
    todo = [n for n in wanted if n in available]
    print(f"📦 {len(wanted)} ô giao hộp bao · {len(todo)} có trên bucket · "
          f"{len(wanted) - len(todo)} ô toàn biển (không có dữ liệu)")

    status = {"ok": 0, "skip": 0, "fail": 0}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(download, n, session): n for n in todo}
        for fut in concurrent.futures.as_completed(futures):
            name = futures[fut]
            try:
                status[fut.result()] += 1
            except Exception as e:
                status["fail"] += 1
                print(f"  ❌ {name}: {e}", file=sys.stderr)
    print(f"✅ tải mới {status['ok']} · đã có {status['skip']} · lỗi {status['fail']} → {OUT_DIR}")
    return 1 if status["fail"] else 0


if __name__ == "__main__":
    sys.exit(main())
