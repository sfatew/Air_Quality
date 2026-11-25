"""
Google Dynamic World V1 → số pixel từng lớp phủ đất trong mỗi ô 0.01°, theo năm.

Nguồn : GOOGLE/DYNAMICWORLD/V1 (Earth Engine), 10 m, mỗi cảnh Sentinel-2 một ảnh,
        band `label` = lớp có xác suất lớn nhất trong 9 lớp.
Kỳ    : 2020 → nay, mỗi năm một composite. Năm hiện tại là năm DỞ DANG (tới
        hôm submit) — ngày kết thúc ghi trong tags; tính lại bằng --refresh <năm>.
Vùng  : CHỈ trong ranh giới Việt Nam (GADM level-0) nới REGION_BUFFER_DEG = 0.1°
        (~11 km); file vẫn phủ trọn hộp bao 800×1550 ô 0.01°, ngoài vùng valid = 0.
        Lý do: đo thật ~1 900 EECU-giây / độ² đất liền / năm; cả hộp bao (64 % đất
        liền là Lào/Campuchia/Thái/TQ) ≈ 46 EECU-giờ/năm, chỉ VN ≈ 16.
Shard : shardSize mặc định 32 ô 0.01° (= 3 200×3 200 pixel nền). 256 (mặc định GEE)
        lỗi "Reprojection output too large (25600x25600 pixels)"; 32 rẻ hơn 64.

Cách tính (toàn bộ trên GEE, không tải ảnh 10 m về):
  1. Mỗi pixel nền 1e-4° (≈11 m, lưới khít lưới dự án): MODE của `label` trên
     mọi cảnh trong năm → "lớp của pixel".
  2. One-hot 9 lớp + `valid` (pixel có ≥1 quan sát) + `n_obs` (số cảnh), cộng dồn
     (sum, không trọng số) lên ô 0.01° = đúng 100×100 pixel nền.
  3. Export batch → Google Drive của tài khoản GEE (thư mục DRIVE_FOLDER), tải về
     bằng Drive API rồi ghi lại GeoTIFF có tên band + metadata.
     (Không dùng asset: tài khoản không có quyền ghi asset trong project.)
  Gộp 0.01° → 0.05° và tính %: process_dynamic_world.py (reduceResolution của GEE
  giới hạn 65 535 pixel vào/ô ra, nên không đi thẳng 10 m → 0.05° = 250 000 pixel).

Lưu : /home/slow_data/Air_Quality/land_cover/dynamic_world/counts_001deg/dw_counts_<năm>.tif
      .../counts_001deg/_tasks/dw_counts_<năm>.json   (task ID + kỳ + tên file Drive)

Chạy (Airqua_env, trong tmux — mỗi năm GEE tính vài giờ):
    python download_dynamic_world.py                      # 2020..năm nay: submit + chờ + tải
    python download_dynamic_world.py --years 2023 2024
    python download_dynamic_world.py --refresh 2026       # tính lại năm dở dang
    python download_dynamic_world.py --no-wait            # chỉ submit, tải ở lần chạy sau
    python download_dynamic_world.py --shard-size 64      # nếu task lỗi hết bộ nhớ
Chạy lại an toàn: năm đã có file thì bỏ qua; task đang chạy (theo ID trong _tasks/)
thì chỉ chờ tiếp; task FAILED/CANCELLED từ lần trước thì submit lại một lần.
"""
import argparse
import datetime as dt
import fcntl
import io
import json
import logging
import socket
import sys
import time
from pathlib import Path

import ee
import numpy as np
import rasterio
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

REPO         = Path(__file__).resolve().parents[2]
GADM_L0      = REPO / "Data_Collection" / "GADM_Vietnam" / "gadm41_VNM_0.shp"
REGION_BUFFER_DEG   = 0.1
REGION_SIMPLIFY_DEG = 0.01        # sau khi nới 0.1°, bỏ chi tiết < ~1 km của đường biên
DEFAULT_SHARD = 32

PROJECT      = "thesis-498612"
OUT_DIR      = Path("/home/slow_data/Air_Quality/land_cover/dynamic_world/counts_001deg")
TASK_DIR     = OUT_DIR / "_tasks"
DRIVE_FOLDER = "gee_dynamic_world"
COLLECTION   = "GOOGLE/DYNAMICWORLD/V1"
FIRST_YEAR   = 2020

# Hộp bao = lưới Stage A (AOD_map/stage_a/config.py); 0.01° lồng khít 5×5 trong 0.05°
LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 102.0, 110.0, 8.0, 23.5
MID_RES  = 0.01
BASE_RES = 1e-4
NX = int(round((LON_MAX - LON_MIN) / MID_RES))   # 800
NY = int(round((LAT_MAX - LAT_MIN) / MID_RES))   # 1550
TRANSFORM = (MID_RES, 0.0, LON_MIN, 0.0, -MID_RES, LAT_MAX)

CLASSES = ["water", "trees", "grass", "flooded_vegetation", "crops",
           "shrub_and_scrub", "built", "bare", "snow_and_ice"]   # thứ tự = giá trị label 0..8
BANDS = CLASSES + ["valid", "n_obs"]

OUTSTANDING = {"UNSUBMITTED", "READY", "RUNNING", "CANCEL_REQUESTED"}
TERMINAL_BAD = {"FAILED", "CANCELLED"}
POLL_S = 120
MISSING_FILE_POLLS = 10          # COMPLETED mà Drive chưa thấy file: chờ tối đa 10×POLL_S
MAX_STATUS_ERRORS = 15           # lỗi hỏi trạng thái liên tiếp (~30 phút) trước khi bỏ năm đó
CANCEL_WAIT_S = 1800

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)


# ── Earth Engine ───────────────────────────────────────────────────────────────
def bbox() -> ee.Geometry:
    return ee.Geometry.Rectangle([LON_MIN, LAT_MIN, LON_MAX, LAT_MAX], None, False)


_REGION: dict = {}


def region() -> ee.Geometry:
    """Ranh giới VN (GADM L0) nới REGION_BUFFER_DEG, đơn giản hoá; chỉ tính DW trong vùng này."""
    key = (str(GADM_L0), REGION_BUFFER_DEG, REGION_SIMPLIFY_DEG)   # đổi cấu hình → tính lại
    if key not in _REGION:
        import geopandas as gpd
        from shapely.ops import unary_union
        g = gpd.read_file(GADM_L0).to_crs("EPSG:4326").geometry
        shape = unary_union(list(g)).buffer(REGION_BUFFER_DEG).simplify(REGION_SIMPLIFY_DEG)
        _REGION[key] = ee.Geometry(shape.__geo_interface__, None, False)
    return _REGION[key]


def period(year: int) -> tuple[str, str]:
    """[start, end) của năm; năm hiện tại cắt tại ngày mai (lấy trọn hôm nay)."""
    end = min(dt.date(year + 1, 1, 1), dt.date.today() + dt.timedelta(days=1))
    return f"{year}-01-01", end.isoformat()


def collection(start: str, end: str) -> ee.ImageCollection:
    return (ee.ImageCollection(COLLECTION).filterBounds(region())
            .filterDate(start, end).select("label"))


def counts_image(start: str, end: str, parallel_scale: int) -> ee.Image:
    base = ee.Projection("EPSG:4326", [BASE_RES, 0, LON_MIN, 0, -BASE_RES, LAT_MAX])
    dw = collection(start, end)
    # clip trên composite: tile nằm ngoài vùng không phải tính mode → không tốn EECU
    lab = dw.reduce(ee.Reducer.mode(), parallel_scale).clip(region()).setDefaultProjection(base)
    # sameFootprint=False: bỏ footprint của clip, nếu không Export cắt ảnh theo vùng
    # (test thật ra 21×12 thay vì 20×20) → file lệch lưới hộp bao
    onehot = (lab.eq(ee.Image.constant(list(range(len(CLASSES))))).rename(CLASSES)
              .unmask(0, False))
    valid = lab.mask().gt(0).rename("valid").unmask(0, False)
    n_obs = (dw.count().clip(region()).rename("n_obs").unmask(0, False)
             .setDefaultProjection(base))
    per_pix = ee.Image.cat([onehot, valid, n_obs]).toFloat()
    # bestEffort mặc định False → vượt maxPixels thì task FAIL, không âm thầm hạ phân giải
    return (per_pix.reduceResolution(ee.Reducer.sum().unweighted(), maxPixels=12000)
            .select(BANDS))


def task_status(task_id: str) -> dict:
    return ee.data.getTaskStatus(task_id)[0]


def outstanding_by_description(desc: str) -> list[dict]:
    return [t.status() for t in ee.batch.Task.list()
            if t.config.get("description") == desc and t.state in OUTSTANDING]


def cancel_and_wait(task_ids: list[str]):
    for tid in task_ids:
        if task_status(tid)["state"] in OUTSTANDING - {"CANCEL_REQUESTED"}:
            ee.data.cancelTask(tid)
            log.info("Huỷ task %s", tid)
    t0 = time.time()
    while any(task_status(tid)["state"] in OUTSTANDING for tid in task_ids):
        if time.time() - t0 > CANCEL_WAIT_S:
            raise RuntimeError(f"Task {task_ids} chưa dừng sau {CANCEL_WAIT_S}s — huỷ tay rồi chạy lại")
        time.sleep(30)


# ── Sidecar task record ───────────────────────────────────────────────────────
def record_path(year: int) -> Path:
    return TASK_DIR / f"dw_counts_{year}.json"


def load_record(year: int) -> dict | None:
    p = record_path(year)
    return json.loads(p.read_text()) if p.exists() else None


def submit(year: int, args) -> dict:
    start, end = period(year)
    n_images = collection(start, end).size().getInfo()
    prefix = f"dw_counts_001deg_{year}_{dt.datetime.now():%Y%m%d%H%M%S}"   # duy nhất mỗi lần submit
    task = ee.batch.Export.image.toDrive(
        image=counts_image(start, end, args.parallel_scale), description=f"dw_counts_{year}",
        folder=DRIVE_FOLDER, fileNamePrefix=prefix, region=bbox(), crs="EPSG:4326",
        crsTransform=list(TRANSFORM), maxPixels=int(1e10), shardSize=args.shard_size,
        fileFormat="GeoTIFF",
    )
    task.start()
    rec = {"year": year, "task_id": task.id, "drive_name": f"{prefix}.tif",
           "date_start": start, "date_end_exclusive": end, "n_images": n_images,
           "region": f"GADM VNM_0 buffered {REGION_BUFFER_DEG} deg, simplified {REGION_SIMPLIFY_DEG} deg",
           "shard_size": args.shard_size, "parallel_scale": args.parallel_scale,
           "submitted": dt.datetime.now().isoformat(timespec="seconds")}
    TASK_DIR.mkdir(parents=True, exist_ok=True)
    tmp = record_path(year).with_suffix(".json.part")
    tmp.write_text(json.dumps(rec, indent=2))
    tmp.replace(record_path(year))                        # không để JSON cụt khi bị ngắt
    log.info("Submit %d: task %s, %d cảnh, kỳ %s→%s", year, task.id, n_images, start, end)
    return rec


# ── Drive + GeoTIFF ───────────────────────────────────────────────────────────
def drive_service():
    return build("drive", "v3", credentials=ee.data.get_persistent_credentials(),
                 cache_discovery=False)


def find_drive_file(drive, rec: dict, status: dict) -> dict | None:
    """File export theo tên duy nhất; lỗi nếu GEE tách nhiều mảnh hoặc trùng tên."""
    q = f"name contains '{rec['drive_name'][:-4]}' and trashed = false"
    for uri in status.get("destination_uris", []):
        if "#folders/" in uri:
            q += f" and '{uri.split('#folders/')[1]}' in parents"
    files, token = [], None
    while True:
        resp = drive.files().list(q=q, fields="nextPageToken,files(id,name,size)", pageSize=100,
                                  pageToken=token).execute()
        files += resp.get("files", [])
        token = resp.get("nextPageToken")
        if not token:
            break
    if not files:
        return None
    if len(files) > 1 or files[0]["name"] != rec["drive_name"]:
        raise RuntimeError(f"Drive có {[f['name'] for f in files]} — mong đúng 1 file {rec['drive_name']}")
    return files[0]


def check_grid(src, ny: int, nx: int, transform: tuple, name: str):
    t = src.transform
    if src.crs is None or src.crs.to_epsg() != 4326:
        raise ValueError(f"{name}: CRS {src.crs} ≠ EPSG:4326")
    if (src.height, src.width) != (ny, nx) or not np.allclose(
            [t.a, t.b, t.c, t.d, t.e, t.f], transform, rtol=0, atol=1e-9):
        raise ValueError(f"{name}: lưới {src.width}x{src.height} {tuple(t)[:6]} ≠ {nx}x{ny} {transform}")


def validate_counts(arr: np.ndarray, year: int):
    """Bất biến của ảnh đếm; vi phạm → raise (năm đó coi như lỗi, file cũ giữ nguyên)."""
    nc = len(CLASSES)
    v = arr[nc]
    problems = []
    if not np.isfinite(arr).all():
        problems.append("có NaN/inf")
    if (arr < 0).any():
        problems.append("có giá trị âm")
    if (v > 10000 + 1e-3).any():
        problems.append(f"{int((v > 10000 + 1e-3).sum())} ô valid > 10000 (lưới nền lệch?)")
    if not np.allclose(arr[:nc].sum(0), v, rtol=0, atol=1e-3):
        problems.append("tổng 9 lớp ≠ valid")
    if (arr[nc + 1] < v - 1e-3).any():
        problems.append("n_obs < valid (pixel hợp lệ phải có ≥1 cảnh)")
    if problems:
        raise ValueError(f"{year}: ảnh đếm sai — " + "; ".join(problems))


def download(drive, rec: dict, file: dict, out: Path):
    """Tải file Drive, kiểm lưới + số band, ghi lại có tên band/tags; thay file cũ nguyên tử."""
    raw = out.with_suffix(".drive.part")
    try:
        with open(raw, "wb") as fh:
            dl = MediaIoBaseDownload(fh, drive.files().get_media(fileId=file["id"]),
                                     chunksize=64 << 20)
            done = False
            while not done:
                _, done = dl.next_chunk()
        with rasterio.open(raw) as src:
            check_grid(src, NY, NX, TRANSFORM, file["name"])
            if src.count != len(BANDS):
                raise ValueError(f"{file['name']}: {src.count} band ≠ {len(BANDS)}")
            arr = src.read().astype(np.float32)
            crs, transform = src.crs, src.transform
        validate_counts(arr, rec["year"])                 # lỗi → không ghi đè file cũ
    except ValueError as e:                               # dữ liệu export sai → phải tính lại
        raw.unlink(missing_ok=True)
        raise RuntimeError(f"{e} — dữ liệu export sai; sửa code rồi --refresh {rec['year']}") from e
    except Exception as e:                                # mạng / ổ đĩa → export vẫn dùng lại được
        raw.unlink(missing_ok=True)
        raise RuntimeError(f"{e} — lỗi tải tạm thời; chạy lại (KHÔNG cần --refresh)") from e

    tmp = out.with_suffix(".tif.part")
    profile = dict(driver="GTiff", width=NX, height=NY, count=len(BANDS), dtype="float32",
                   crs=crs, transform=transform, compress="deflate", predictor=3,
                   tiled=True, blockxsize=256, blockysize=256)
    try:
        write_counts(tmp, profile, arr, rec, file["name"])
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    finally:
        raw.unlink(missing_ok=True)
    tmp.replace(out)

    v = arr[len(CLASSES)]
    log.info("Tải %d → %s (%d cảnh, ô có dữ liệu %.1f%%, kỳ %s→%s)", rec["year"], out.name,
             rec["n_images"], 100 * (v > 0).mean(), rec["date_start"], rec["date_end_exclusive"])


def write_counts(tmp: Path, profile: dict, arr: np.ndarray, rec: dict, drive_name: str):
    with rasterio.open(tmp, "w", **profile) as dst:
        dst.write(arr)
        for i, b in enumerate(BANDS, 1):
            dst.set_band_description(i, b)
        dst.update_tags(source=COLLECTION, classes=",".join(CLASSES), drive_file=drive_name,
                        units="count of 1e-4 deg base pixels (10000 per 0.01 deg cell); "
                              "n_obs = sum over pixels of #scenes",
                        composite="mode(label) per 1e-4 deg pixel",
                        **{k: str(rec.get(k, "")) for k in ("task_id", "date_start",
                                                            "date_end_exclusive", "n_images",
                                                            "region", "submitted")})


# ── Main ──────────────────────────────────────────────────────────────────────
def prepare(year: int, refresh: bool, args) -> dict | None:
    """Trả record task cần theo dõi cho năm này, hoặc None nếu đã xong."""
    out = OUT_DIR / f"dw_counts_{year}.tif"
    rec = load_record(year)
    if out.exists() and not refresh:
        with rasterio.open(out) as src:
            have = src.tags().get("task_id")
        if not rec or rec["task_id"] == have:
            return None                                   # file khớp record → xong, khỏi hỏi GEE
    st = task_status(rec["task_id"])["state"] if rec else None
    known = OUTSTANDING | TERMINAL_BAD | {"COMPLETED"}
    if st is not None and st not in known and not refresh:
        raise RuntimeError(f"{year}: task {rec['task_id']} ở trạng thái lạ {st} — "
                           f"dùng --refresh {year} để bỏ qua task này")

    if out.exists() and not refresh:
        # Tới đây: rec có và khác task của file → task refresh lần trước chưa thay được file cũ
        if st in TERMINAL_BAD:
            raise RuntimeError(f"{year}: task refresh {rec['task_id']} {st} — file hiện có là "
                               f"bản cũ; chạy lại với --refresh {year}")
        log.info("%d: theo dõi tiếp task refresh %s (%s)", year, rec["task_id"], st)
        return rec

    ids = {s["id"] for s in outstanding_by_description(f"dw_counts_{year}")}
    if refresh:
        mine = {rec["task_id"]} if rec and st in OUTSTANDING else set()
        cancel_and_wait(sorted(ids | mine))
        return submit(year, args)                         # file cũ giữ tới khi file mới tải xong
    stray = ids - ({rec["task_id"]} if rec else set())
    if stray:
        raise RuntimeError(f"{year}: task {stray} cùng tên đang chạy mà không khớp "
                           f"{record_path(year).name} — huỷ trên GEE hoặc dùng --refresh {year}")
    if rec is None:
        return submit(year, args)
    if st in TERMINAL_BAD:
        log.warning("%d: task cũ %s %s — submit lại", year, rec["task_id"], st)
        return submit(year, args)
    return rec                                            # đang chạy / đã xong → chờ / tải


def main():
    this_year = dt.date.today().year
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, nargs="+", default=list(range(FIRST_YEAR, this_year + 1)))
    ap.add_argument("--refresh", type=int, nargs="*", default=[],
                    help="tính lại các năm này (huỷ task cũ, giữ file cũ tới khi có file mới)")
    ap.add_argument("--no-wait", action="store_true", help="chỉ submit rồi thoát")
    ap.add_argument("--shard-size", type=int, default=DEFAULT_SHARD,
                    help="ô 0.01° mỗi cạnh shard; giảm (vd. 16) nếu task hết bộ nhớ / quá lớn")
    ap.add_argument("--parallel-scale", type=int, default=1, help="tăng (vd. 4) nếu task hết bộ nhớ")
    args = ap.parse_args()
    years = sorted(set(args.years) | set(args.refresh))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    lock = open(OUT_DIR / ".lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit("Đang có một tiến trình download_dynamic_world.py khác chạy — thoát.")

    # Không có timeout thì một request treo (thấy thật 15/11: "Connection max age expired"
    # rồi đứng 8 giờ) làm dừng cả vòng theo dõi. Lỗi timeout rơi vào nhánh thử lại bên dưới.
    socket.setdefaulttimeout(300)
    ee.Initialize(project=PROJECT)
    ee.data.setDeadline(180_000)
    tracked, failed = {}, {}
    for y in years:
        try:
            rec = prepare(y, y in args.refresh, args)
        except Exception as e:                            # một năm lỗi không chặn các năm khác
            failed[y] = str(e)
            continue
        if rec:
            tracked[y] = rec
    log.info("Năm cần theo dõi: %s", sorted(tracked) or "không có")
    if args.no_wait:
        for y, msg in sorted(failed.items()):
            log.error("%d: %s", y, msg)
        sys.exit(1 if failed else 0)

    drive = drive_service() if tracked else None
    missing_polls = {y: 0 for y in tracked}
    status_errors = {y: 0 for y in tracked}
    while True:
        waiting = {}
        for y, rec in tracked.items():
            if y in failed or rec.get("done"):
                continue
            try:
                st = task_status(rec["task_id"])
                status_errors[y] = 0
            except Exception as e:                        # lỗi RPC tạm thời không được kéo đổ năm khác
                status_errors[y] += 1
                if status_errors[y] > MAX_STATUS_ERRORS:
                    failed[y] = f"không hỏi được trạng thái task {MAX_STATUS_ERRORS}× liên tiếp: {e}"
                else:
                    log.warning("%d: lỗi hỏi trạng thái (%d/%d): %s", y, status_errors[y],
                                MAX_STATUS_ERRORS, e)
                    waiting[y] = "?"
                continue
            state = st["state"]
            if state == "COMPLETED":
                try:
                    f = find_drive_file(drive, rec, st)
                    if f is None:
                        missing_polls[y] += 1
                        if missing_polls[y] > MISSING_FILE_POLLS:
                            failed[y] = f"COMPLETED nhưng không thấy {rec['drive_name']} trên Drive"
                        else:
                            waiting[y] = "COMPLETED, chờ Drive"
                        continue
                    download(drive, rec, f, OUT_DIR / f"dw_counts_{y}.tif")
                    rec["done"] = True
                except Exception as e:
                    failed[y] = f"tải/kiểm lỗi: {e}"
            elif state in TERMINAL_BAD:
                failed[y] = f"{state}: {st.get('error_message')}"
            elif state in OUTSTANDING:
                waiting[y] = state
            else:
                failed[y] = f"trạng thái lạ {state}"
        if not waiting:
            break
        log.info("Chờ GEE: %s", waiting)
        time.sleep(POLL_S)

    for y, msg in sorted(failed.items()):
        log.error("%d: %s", y, msg)
    absent = [y for y in years if not (OUT_DIR / f"dw_counts_{y}.tif").exists()]
    if failed or absent:
        log.error("Chưa đủ file: %s", absent)
        sys.exit(1)
    log.info("Xong: đủ %d năm.", len(years))


if __name__ == "__main__":
    main()
