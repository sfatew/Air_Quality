"""
Download CAMS PM2.5 (surface particulate matter d < 2.5 µm) 2020 → 2026
for a padded Vietnam bounding box via the ADS API.

Sản phẩm theo tháng (cố định, KHÔNG fallback lẫn lộn)
───────────────────────────────────────────────────
  EAC4 reanalysis — `cams-global-reanalysis-eac4`
      0,75° × 0,75°, 3 giờ/lần (00, 03, …, 21 UTC), tức thời.
      Catalogue ADS: có từ 2003-01-01 tới 2025-12-31.
  Forecast / analysis — `cams-global-atmospheric-composition-forecasts`
      0,4° × 0,4°, base 00/12 UTC × leadtime 0/3/6/9 h → cùng 8 mốc/ngày như EAC4.
      Dùng cho các tháng EAC4 CHƯA có (2026). Không phải reanalysis.

  Script cũ `fetch_cams_bbox.py` (AOD) đổi sang forecast khi EAC4 lỗi vì BẤT KỲ lý
  do gì, kể cả lỗi hàng đợi tạm thời → một tháng có thể lặng lẽ thành forecast dù
  EAC4 có. Ở đây mỗi tháng được gán đúng MỘT sản phẩm theo phạm vi thời gian của
  catalogue ADS (đọc lúc chạy); lỗi thì thử lại cùng sản phẩm, không đổi.

Vùng tải
────────
  [N 25, W 101, S 7, E 111] — rộng hơn hộp lưới 0,05° (8–23,5°N, 102–110°E) mỗi phía
  ≥ 0,5°. Điểm lưới 0,75° của EAC4 không trùng mép hộp (điểm cuối 23,25°N /
  109,5°E), nên nếu tải đúng hộp thì về sau nội suy xuống 0,05° sẽ phải NGOẠI suy
  ở dải biên bắc và đông.

Đầu ra
──────
  /home/slow_data/Air_Quality/CAM/pm25/_monthly_raw/cams_pm25_YYYYMM.nc
      PM25 (time, latitude, longitude)  µg m-3   (= pm2p5 [kg m-3] × 1e9)
      time UTC; attrs ghi rõ sản phẩm nguồn và độ phân giải
  /home/slow_data/Air_Quality/CAM/pm25/manifest.csv   — tháng → sản phẩm, số mốc, lưới
  /home/slow_data/Air_Quality/CAM/pm25/failed_requests.log

Chạy
────
  python fetch_cams_pm25.py --probe          # thử 1 mốc giờ mỗi sản phẩm, không tải hàng loạt
  python fetch_cams_pm25.py                  # 2020-01 → 2026-09
  python fetch_cams_pm25.py --start 2024-01 --end 2024-12 --workers 4
"""

import argparse
import calendar
import json
import os
import tempfile
import threading
import time
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import cdsapi
import numpy as np
import pandas as pd
import xarray as xr

# ── Configuration ──────────────────────────────────────────────────────────────
AREA        = [25.0, 101.0, 7.0, 111.0]       # [N, W, S, E] — ADS convention, có đệm
START       = (2020, 1)
END         = (2026, 9)
MAX_WORKERS = 1                               # ADS: "Maximum number queued requests for this
                                              # dataset has been reached ... request data sequentially"
MAX_RETRIES = 5
RETRY_DELAY = 120                             # giây, gấp đôi mỗi lần
QUEUE_FULL_WAIT = 300                         # giây chờ khi ADS báo hàng đợi đầy
QUEUE_FULL_MAX = 60                           # số lần chờ tối đa cho lỗi hàng đợi đầy (~5 giờ)

OUTPUT_DIR  = "/home/slow_data/Air_Quality/CAM/pm25"
MONTHLY_DIR = os.path.join(OUTPUT_DIR, "_monthly_raw")
MANIFEST    = os.path.join(OUTPUT_DIR, "manifest.csv")
FAILED_LOG  = os.path.join(OUTPUT_DIR, "failed_requests.log")

DS_EAC4     = "cams-global-reanalysis-eac4"
DS_FORECAST = "cams-global-atmospheric-composition-forecasts"
VARIABLE    = "particulate_matter_2.5um"
EAC4_TIMES  = [f"{h:02d}:00" for h in (0, 3, 6, 9, 12, 15, 18, 21)]
FC_TIMES    = ["00:00", "12:00"]
FC_LEADS    = ["0", "3", "6", "9"]            # → 00,03,06,09 và 12,15,18,21 UTC

# Dự phòng khi không đọc được catalogue (giá trị đọc được lúc viết script).
EAC4_END_FALLBACK = (2025, 12)
CATALOGUE = "https://ads.atmosphere.copernicus.eu/api/catalogue/v1/collections/{}"

KG_TO_UG = 1e9


# ── Phạm vi EAC4 ───────────────────────────────────────────────────────────────
def eac4_last_month() -> tuple[int, int]:
    """Tháng cuối EAC4 có dữ liệu, đọc từ catalogue ADS (rơi về hằng số nếu lỗi mạng)."""
    try:
        with urllib.request.urlopen(CATALOGUE.format(DS_EAC4), timeout=60) as r:
            j = json.load(r)
        end = j["extent"]["temporal"]["interval"][0][1]
        t = pd.Timestamp(end).tz_localize(None) if pd.Timestamp(end).tzinfo is None \
            else pd.Timestamp(end).tz_convert(None)
        # Chỉ coi một tháng là "có EAC4" khi có trọn tháng.
        if t.day < calendar.monthrange(t.year, t.month)[1]:
            t = t - pd.offsets.MonthEnd(1)
        return (t.year, t.month)
    except Exception as e:
        print(f"[catalogue] không đọc được ({e!r}) → dùng EAC4_END_FALLBACK {EAC4_END_FALLBACK}")
        return EAC4_END_FALLBACK


def iter_months(start, end):
    y, m = start
    while (y, m) <= end:
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1


# ── Yêu cầu ADS ────────────────────────────────────────────────────────────────
def _month_range(year, month):
    n = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-01/{year}-{month:02d}-{n:02d}"


def build_request(dataset: str, date_range: str, times=None, area=AREA) -> dict:
    if dataset == DS_EAC4:
        return {"variable": [VARIABLE], "date": date_range,
                "time": times or EAC4_TIMES, "area": area, "data_format": "netcdf_zip"}
    return {"variable": [VARIABLE], "date": date_range,
            "time": times or FC_TIMES, "leadtime_hour": FC_LEADS,
            "type": ["forecast"], "area": area, "data_format": "netcdf_zip"}


# ── Mở + chuẩn hoá file tải về ─────────────────────────────────────────────────
def _flatten_forecast_time(ds: xr.Dataset) -> xr.Dataset:
    """(forecast_reference_time × forecast_period) → một trục `time` = base + lead."""
    base = next((d for d in ("forecast_reference_time", "reference_time") if d in ds.dims), None)
    lead = next((d for d in ("forecast_period", "step", "leadtime_hour") if d in ds.dims), None)
    if base is None or lead is None:
        return ds
    bv = ds[base].values
    lv = ds[lead].values
    if not np.issubdtype(lv.dtype, np.timedelta64):
        lv = lv.astype("timedelta64[h]")
    valid = (bv[:, None] + lv[None, :]).reshape(-1)
    out = {}
    for v in ds.data_vars:
        a = ds[v]
        sp = [d for d in a.dims if d not in (base, lead)]
        arr = a.transpose(base, lead, *sp).values
        out[v] = xr.DataArray(arr.reshape(arr.shape[0] * arr.shape[1], *arr.shape[2:]),
                              dims=("time", *sp),
                              coords={"time": valid, **{d: ds[d] for d in sp}},
                              attrs=a.attrs)
    res = xr.Dataset(out, attrs=ds.attrs).sortby("time")
    _, uniq = np.unique(res["time"].values, return_index=True)
    return res.isel(time=np.sort(uniq)) if len(uniq) < res.sizes["time"] else res


def open_download(path: str) -> xr.Dataset:
    if zipfile.is_zipfile(path):
        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(path) as z:
                z.extractall(tmp)
            parts = [xr.open_dataset(os.path.join(tmp, n)).load()
                     for n in os.listdir(tmp) if n.endswith(".nc")]
            if not parts:
                raise RuntimeError(f"zip không chứa .nc: {path}")
            ds = xr.merge(parts, compat="no_conflicts") if len(parts) > 1 else parts[0]
    else:
        ds = xr.open_dataset(path).load()
    ds = _flatten_forecast_time(ds)
    if "valid_time" in ds.dims:
        ds = ds.rename({"valid_time": "time"})
    for v in ("valid_time", "expver", "number"):
        if v in ds.variables and v not in ds.dims:
            ds = ds.drop_vars(v)
    return ds


def postprocess(ds: xr.Dataset, dataset: str) -> xr.Dataset:
    """pm2p5 [kg m-3] → PM25 [µg m-3], gắn thuộc tính nguồn."""
    src = next((v for v in ("pm2p5", "PM25", "particulate_matter_2.5um") if v in ds), None)
    if src is None:
        raise KeyError(f"không thấy biến PM2.5 trong file, có: {list(ds.data_vars)}")
    units = str(ds[src].attrs.get("units", "kg m**-3")).replace(" ", "")
    if units not in ("kgm**-3", "kgm-3", "kg/m3", "kgm^-3"):
        raise ValueError(f"đơn vị PM2.5 bất ngờ: {ds[src].attrs.get('units')!r}")
    pm = (ds[src].astype(np.float64) * KG_TO_UG).astype(np.float32)
    lat = ds["latitude"].values
    res = float(abs(lat[1] - lat[0])) if lat.size > 1 else np.nan
    out = xr.Dataset({"PM25": pm})
    out["PM25"].attrs = {"long_name": "particulate matter d < 2.5 um (surface)",
                         "units": "ug m-3", "source_variable": src,
                         "source_units": ds[src].attrs.get("units", "")}
    out["time"].attrs = {"long_name": "time", "note": "UTC, timezone-naive"}
    out.attrs = {"source_dataset": dataset,
                 "product_type": "reanalysis" if dataset == DS_EAC4 else "forecast/analysis",
                 "grid_resolution_deg": res,
                 "area_NWSE": str(AREA),
                 "created": datetime.now().isoformat(timespec="seconds")}
    return out


# ── Một tháng ─────────────────────────────────────────────────────────────────
_lock = threading.Lock()


def _say(*a):
    with _lock:
        print(*a, flush=True)


def process_month(i, total, year, month, dataset) -> dict:
    tag = f"{year}{month:02d}"
    clean = os.path.join(MONTHLY_DIR, f"cams_pm25_{tag}.nc")
    if os.path.exists(clean):
        with xr.open_dataset(clean) as d:
            info = {"month": f"{year}-{month:02d}", "dataset": d.attrs.get("source_dataset"),
                    "n_times": d.sizes["time"], "res_deg": d.attrs.get("grid_resolution_deg"),
                    "status": "cached"}
        _say(f"[{i:2d}/{total}] {year}-{month:02d} đã có — bỏ qua")
        return info

    raw = os.path.join(MONTHLY_DIR, f"cams_pm25_{tag}_raw.zip")
    req = build_request(dataset, _month_range(year, month))
    # Hai loại lỗi tách riêng: "hàng đợi đầy" là giới hạn tốc độ của ADS (chờ rồi gửi lại,
    # không tính vào số lần thử); lỗi khác thì thử lại MAX_RETRIES lần với thời gian chờ tăng dần.
    last, attempt, queue_waits = None, 0, 0
    while True:
        if os.path.exists(raw):
            os.remove(raw)
        try:
            _say(f"[{i:2d}/{total}] {year}-{month:02d} gửi yêu cầu ({dataset}) …")
            cdsapi.Client(quiet=True).retrieve(dataset, req, raw)
            last = None
            break
        except Exception as e:
            last = e
            if "Maximum number queued requests" in str(e) and queue_waits < QUEUE_FULL_MAX:
                queue_waits += 1
                _say(f"[{i:2d}/{total}] {year}-{month:02d} hàng đợi ADS đầy — chờ {QUEUE_FULL_WAIT}s "
                     f"({queue_waits}/{QUEUE_FULL_MAX})")
                time.sleep(QUEUE_FULL_WAIT)
                continue
            attempt += 1
            _say(f"[{i:2d}/{total}] {year}-{month:02d} lần {attempt} lỗi: {e!r}"[:400])
            if attempt >= MAX_RETRIES:
                break
            time.sleep(RETRY_DELAY * 2 ** (attempt - 1))
    if last is not None:
        raise last

    ds = postprocess(open_download(raw), dataset)
    tmp = clean + ".tmp"
    ds.to_netcdf(tmp, encoding={"PM25": {"zlib": True, "complevel": 4}})
    os.replace(tmp, clean)
    os.remove(raw)
    t = pd.to_datetime(ds["time"].values)
    _say(f"[{i:2d}/{total}] {year}-{month:02d} xong: {ds.sizes['time']} mốc "
         f"{t.min():%m-%d %H} → {t.max():%m-%d %H} UTC, "
         f"PM25 TB {float(ds['PM25'].mean()):.1f} µg/m³")
    return {"month": f"{year}-{month:02d}", "dataset": dataset, "n_times": ds.sizes["time"],
            "res_deg": ds.attrs["grid_resolution_deg"], "status": "downloaded"}


def update_manifest(rows: list[dict]) -> None:
    new = pd.DataFrame(rows)
    if os.path.exists(MANIFEST):
        old = pd.read_csv(MANIFEST)
        new = pd.concat([old[~old["month"].isin(new["month"])], new])
    new.sort_values("month").to_csv(MANIFEST, index=False)


# ── Thăm dò ───────────────────────────────────────────────────────────────────
def probe() -> None:
    """Một mốc giờ, vùng nhỏ, cho mỗi sản phẩm — kiểm tên biến, đơn vị, lưới."""
    small = [22.0, 105.0, 20.0, 107.0]
    for ds_id, day, times in ((DS_EAC4, "2020-01-01/2020-01-01", ["06:00"]),
                              (DS_FORECAST, "2026-01-01/2026-01-01", ["00:00"])):
        out = os.path.join(tempfile.gettempdir(), f"probe_{ds_id}.zip")
        req = build_request(ds_id, day, times=times, area=small)
        if ds_id == DS_FORECAST:
            req["leadtime_hour"] = ["6"]
        t0 = time.time()
        cdsapi.Client(quiet=True).retrieve(ds_id, req, out)
        d = postprocess(open_download(out), ds_id)
        os.remove(out)
        print(f"[probe] {ds_id}: OK sau {time.time()-t0:.0f}s · dims {dict(d.sizes)} · "
              f"lưới {d.attrs['grid_resolution_deg']}° · PM25 {float(d['PM25'].min()):.1f}–"
              f"{float(d['PM25'].max()):.1f} µg/m³ · biến gốc {d['PM25'].attrs['source_variable']}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--start", default=f"{START[0]}-{START[1]:02d}")
    ap.add_argument("--end", default=f"{END[0]}-{END[1]:02d}")
    ap.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = ap.parse_args()

    os.makedirs(MONTHLY_DIR, exist_ok=True)
    if args.probe:
        probe()
        return

    start = tuple(int(x) for x in args.start.split("-"))
    end = tuple(int(x) for x in args.end.split("-"))
    eac4_end = eac4_last_month()
    months = list(iter_months(start, end))
    plan = [(y, m, DS_EAC4 if (y, m) <= eac4_end else DS_FORECAST) for y, m in months]
    n_eac4 = sum(p[2] == DS_EAC4 for p in plan)
    print(f"CAMS PM2.5: {len(plan)} tháng {args.start} → {args.end} · EAC4 tới "
          f"{eac4_end[0]}-{eac4_end[1]:02d} → {n_eac4} tháng EAC4, "
          f"{len(plan) - n_eac4} tháng forecast · vùng {AREA} · {args.workers} luồng", flush=True)

    rows, failed = [], []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(process_month, i + 1, len(plan), y, m, d): (y, m)
                for i, (y, m, d) in enumerate(plan)}
        for fut in as_completed(futs):
            y, m = futs[fut]
            try:
                rows.append(fut.result())
                update_manifest(rows)
            except Exception as e:
                _say(f"  ✗ {y}-{m:02d} THẤT BẠI: {e!r}"[:400])
                failed.append((y, m, repr(e)[:300]))

    if failed:
        with open(FAILED_LOG, "w") as f:
            f.write(f"# Tháng CAMS PM2.5 thất bại ({datetime.now().isoformat(timespec='seconds')})\n")
            for y, m, e in sorted(failed):
                f.write(f"{y}-{m:02d}\t{e}\n")
        print(f"\n{len(failed)} tháng thất bại → {FAILED_LOG}")
    print(f"\nXong: {len(rows)} tháng có file · manifest {MANIFEST}")


if __name__ == "__main__":
    main()
