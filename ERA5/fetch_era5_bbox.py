#!/usr/bin/env python3
"""Download ERA5 over Vietnam, chunked by (variable, month).
Files for the same month are grouped into subfolder YYYYMM/.
Parallelized with a thread pool."""

import argparse
import calendar
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import cdsapi

VIETNAM_AREA = [23.5, 102.0, 8.0, 110.0]
GRID = [0.25, 0.25]

ERA5_VARIABLES = [
    "2m_temperature", "2m_dewpoint_temperature",
    "surface_pressure", "mean_sea_level_pressure",
    "10m_u_component_of_wind", "10m_v_component_of_wind",
    "100m_u_component_of_wind", "100m_v_component_of_wind",
    "boundary_layer_height", "total_cloud_cover",
    "cloud_base_height", "total_column_water_vapour",
    "surface_solar_radiation_downwards", "total_precipitation",
    "forecast_albedo", "convective_available_potential_energy",
]

SHORT = {
    "2m_temperature": "t2m", "2m_dewpoint_temperature": "d2m",
    "surface_pressure": "sp", "mean_sea_level_pressure": "msl",
    "10m_u_component_of_wind": "u10", "10m_v_component_of_wind": "v10",
    "100m_u_component_of_wind": "u100", "100m_v_component_of_wind": "v100",
    "boundary_layer_height": "blh", "total_cloud_cover": "tcc",
    "cloud_base_height": "cbh", "total_column_water_vapour": "tcwv",
    "surface_solar_radiation_downwards": "ssrd", "total_precipitation": "tp",
    "forecast_albedo": "fal", "convective_available_potential_energy": "cape",
}

HOURS = [f"{h:02d}:00" for h in range(24)]

# Thread-local CDS clients (mỗi thread giữ 1 client riêng)
_thread_local = threading.local()
_print_lock = threading.Lock()


def get_client():
    if not hasattr(_thread_local, "client"):
        _thread_local.client = cdsapi.Client(quiet=True)
    return _thread_local.client


def log(msg: str):
    with _print_lock:
        print(msg, flush=True)


def month_chunks(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        _, last = calendar.monthrange(y, m)
        d0 = start.day if (y, m) == (start.year, start.month) else 1
        d1 = end.day if (y, m) == (end.year, end.month) else last
        yield y, m, [f"{d:02d}" for d in range(d0, d1 + 1)]
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1


def build_jobs(start: date, end: date, out_dir: Path, ext: str):
    jobs = []
    for year, month, days in month_chunks(start, end):
        month_dir = out_dir / f"{year}{month:02d}"
        for var in ERA5_VARIABLES:
            short = SHORT[var]
            fname = month_dir / f"era5_vietnam_{short}_{year}{month:02d}.{ext}"
            jobs.append((year, month, days, var, fname))
    return jobs


def fetch_one(job, fmt, retries, sleep):
    """Trả về (job, status, last_error). status ∈ {'skip','ok','fail'}."""
    year, month, days, var, fname = job

    if fname.exists() and fname.stat().st_size > 0:
        return job, "skip", None

    fname.parent.mkdir(parents=True, exist_ok=True)
    req = {
        "product_type": "reanalysis",
        "format": fmt,
        "variable": var,
        "year": str(year),
        "month": f"{month:02d}",
        "day": days,
        "time": HOURS,
        "area": VIETNAM_AREA,
        "grid": GRID,
    }
    client = get_client()
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            client.retrieve("reanalysis-era5-single-levels", req, str(fname))
            return job, "ok", None
        except Exception as e:
            last_err = e
            log(f"[err ] {fname.name} (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(sleep)
    return job, "fail", last_err


def run_pass(jobs, fmt, retries, sleep, workers, out_dir, label):
    """Chạy song song 1 batch jobs, trả về (stats_delta, failed_jobs)."""
    stats = {"skip": 0, "ok": 0, "fail": 0}
    failed = []
    total = len(jobs)
    done = 0

    log(f"\n=== {label}: {total} requests, {workers} workers ===")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_one, j, fmt, retries, sleep): j
                   for j in jobs}
        for fut in as_completed(futures):
            job, status, err = fut.result()
            stats[status] += 1
            done += 1
            rel = job[4].relative_to(out_dir)
            log(f"[{done:>3}/{total}] [{status:>4}] {rel}")
            if status == "fail":
                failed.append((*job, err))
    return stats, failed


def download(start, end, out_dir, fmt="netcdf",
             retries=3, sleep=30, workers=4,
             final_retries=2, final_sleep=120):
    out_dir.mkdir(parents=True, exist_ok=True)
    ext = "nc" if fmt == "netcdf" else "grib"

    jobs = build_jobs(start, end, out_dir, ext)
    total = len(jobs)
    stats = {"skip": 0, "ok": 0, "fail": 0}

    # ---------------- pass 1 ----------------
    delta, failed_jobs = run_pass(jobs, fmt, retries, sleep, workers,
                                  out_dir, "PASS 1")
    for k in stats:
        stats[k] += delta[k]

    # ---------------- final retry rounds ----------------
    for rnd in range(1, final_retries + 1):
        if not failed_jobs:
            break
        log(f"\n=== Sleeping {final_sleep}s before retry round {rnd} ===")
        time.sleep(final_sleep)

        # bỏ phần err ra khỏi tuple để tái dùng fetch_one
        retry_jobs = [tuple(j[:5]) for j in failed_jobs]
        delta, failed_jobs = run_pass(
            retry_jobs, fmt, retries, sleep, workers,
            out_dir, f"FINAL RETRY round {rnd}",
        )
        # các job retry trước đó đã được tính vào "fail" ở pass trước;
        # giờ trừ ra rồi cộng vào status mới
        stats["fail"] -= (delta["ok"] + delta["skip"] + delta["fail"])
        for k in delta:
            stats[k] += delta[k]

    # ---------------- summary ----------------
    print("\n" + "=" * 60)
    print(f"SUMMARY  ({start} → {end})")
    print("=" * 60)
    print(f"  Total requests : {total}")
    print(f"  Skipped (cached): {stats['skip']}")
    print(f"  Success         : {stats['ok']}")
    print(f"  Failed          : {stats['fail']}")

    if failed_jobs:
        log_path = out_dir / "failed_requests.log"
        with log_path.open("w") as f:
            f.write(f"# Failed ERA5 requests ({datetime.now().isoformat()})\n")
            for year, month, days, var, fname, err in failed_jobs:
                f.write(f"{year}-{month:02d}\t{var}\t{fname}\t{err}\n")
        print(f"\n  Failed list written to: {log_path}")
        print("  Failed files:")
        for _, _, _, var, fname, _ in failed_jobs:
            print(f"    - {fname.relative_to(out_dir)}  ({var})")
    print("=" * 60)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out", default="./era5_vietnam")
    p.add_argument("--format", choices=["netcdf", "grib"], default="netcdf")
    p.add_argument("--workers", type=int, default=4,
                   help="Số request đồng thời (khuyến nghị 4-8, tối đa ~20).")
    p.add_argument("--retries", type=int, default=3,
                   help="Số lần retry mỗi request trong pass.")
    p.add_argument("--final-retries", type=int, default=2,
                   help="Số vòng final retry cho job lỗi.")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    if end < start:
        raise SystemExit("end < start")
    download(start, end, Path(args.out), fmt=args.format,
             retries=args.retries, final_retries=args.final_retries,
             workers=args.workers)
    print("Done.")