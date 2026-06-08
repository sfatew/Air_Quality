"""
Download CAMS Global Reanalysis (EAC4) total aerosol optical depth at 550 nm
for the Vietnam bounding box via CDS API.

Strategy
────────
  One CDS request per month → checkpoint .nc file per month → merge at the end.
  Restarting the script skips months already on disk.
  Months are fetched concurrently (ThreadPoolExecutor) to amortise CDS queue time.

Requires
────────
  pip install cdsapi xarray netCDF4 scipy
  ~/.cdsapirc  (or CDSAPI_URL / CDSAPI_KEY env vars) with valid credentials.
  CDS account: https://cds.climate.copernicus.eu

Dataset
───────
  cams-global-reanalysis-eac4
    Spatial : 0.75° × 0.75° global grid
    Temporal: 3-hourly (00, 03, 06, 09, 12, 15, 18, 21 UTC)
    Coverage: 2003-01-01 → ~2 years before present
    All variables are instantaneous (no accumulation, no deaccumulation needed).

Output variable
───────────────
  AOD550    total aerosol optical depth at 550 nm    [dimensionless]
"""

import calendar
import os
import tempfile
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import cdsapi
import pandas as pd
import xarray as xr

# ── Configuration ──────────────────────────────────────────────────────────────
BBOX         = [23.5, 102.0, 8.0, 110.0]   # [N, W, S, E] — CDS convention
START        = (2022, 9)
END          = (2025, 9)

# How many months to download concurrently.
# CDS fair-use limit is ~20 active jobs per user; 1 job per month → keep ≤16.
MAX_WORKERS  = 4

# Retry settings for CDS queue-rejection errors
MAX_RETRIES  = 5      # max attempts per month
RETRY_DELAY  = 120    # seconds before first retry (doubles each attempt)

OUTPUT_DIR   = "/home/slow_data/Air_Quality/CAM"
MONTHLY_DIR  = os.path.join(OUTPUT_DIR, "_monthly_raw")

# ── CDS request constants ─────────────────────────────────────────────────────
CDS_DATASET   = "cams-global-reanalysis-eac4"
CDS_VARIABLES =  [
        "black_carbon_aerosol_optical_depth_550nm",
        "dust_aerosol_optical_depth_550nm",
        "organic_matter_aerosol_optical_depth_550nm",
        "sea_salt_aerosol_optical_depth_550nm",
        "sulphate_aerosol_optical_depth_550nm",
        "total_aerosol_optical_depth_550nm"
    ]
CDS_TIMES     = [f"{h:02d}:00" for h in (0, 3, 6, 9, 12, 15, 18, 21)]

os.makedirs(OUTPUT_DIR,  exist_ok=True)
os.makedirs(MONTHLY_DIR, exist_ok=True)


# ── Month iterator ─────────────────────────────────────────────────────────────
def iter_months(start: tuple[int, int], end: tuple[int, int]):
    y, m = start
    while (y, m) <= end:
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1


# ── CDS download ───────────────────────────────────────────────────────────────
def _build_request(year: int, month: int) -> dict:
    """Build the CDS API request body for a single month."""
    n_days = calendar.monthrange(year, month)[1]
    start  = f"{year}-{month:02d}-01"
    end    = f"{year}-{month:02d}-{n_days:02d}"
    return {
        "variable":    CDS_VARIABLES,
        "date":        f"{start}/{end}",   # new ADS API: single ISO date range
        "time":        CDS_TIMES,
        "area":        BBOX,
        "data_format": "netcdf_zip",
    }


def download_month(year: int, month: int, out_path: str) -> None:
    """Submit one CDS request for a single month."""
    client = cdsapi.Client(quiet=True)
    client.retrieve(CDS_DATASET, _build_request(year, month), out_path)


# ── Open a CDS download (zip or plain nc) ─────────────────────────────────────
def _open_one_file(path: str) -> xr.Dataset:
    """
    Open a single CDS download, handling both ZIP and plain NetCDF formats.

    xr.open_dataset is lazy — we call .load() inside the context manager so
    all data is read into memory before the temp directory (or the original
    file handle) is released.
    """
    if zipfile.is_zipfile(path):
        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(path) as z:
                z.extractall(tmp)
            nc_files = [
                os.path.join(tmp, name)
                for name in os.listdir(tmp)
                if name.endswith(".nc")
            ]
            if not nc_files:
                raise RuntimeError(f"No .nc files found inside zip: {path}")
            parts = [xr.open_dataset(f).load() for f in nc_files]
            ds = xr.merge(parts, compat="no_conflicts") if len(parts) > 1 else parts[0]
    else:
        ds = xr.open_dataset(path).load()

    # Normalise dimension name: new CDS API uses 'valid_time', old uses 'time'
    if "valid_time" in ds.dims:
        ds = ds.rename({"valid_time": "time"})
    if "valid_time" in ds.coords and "valid_time" not in ds.dims:
        ds = ds.drop_vars("valid_time")

    return ds


# ── Unit conversions & derived variables ──────────────────────────────────────
def postprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Rename CDS short names to output variable names and attach CF attributes.
    AOD is dimensionless — no unit conversion needed.
    """
    # CDS may expose the variable under either short name depending on API version
    rename_map = {
        "aod550":   "AOD550",
        "taod550":  "AOD550",
        "bcaod550": "BCAOD550",
        "duaod550": "DUAOD550",
        "omaod550": "OMAOD550",
        "ssaod550": "SSAOD550",
        "suaod550": "SUAOD550",
    }
    present = {k: v for k, v in rename_map.items() if k in ds}
    ds = ds.rename(present)

    # Drop CDS metadata variables that vary across files and block concat
    for drop_var in ("expver", "number"):
        if drop_var in ds:
            ds = ds.drop_vars(drop_var)

    cf_attrs = {
        "AOD550": ("total aerosol optical depth at 550 nm", "1"),
        "BCAOD550": ("black carbon aerosol optical depth at 550 nm", "1"),
        "DUAOD550": ("dust aerosol optical depth at 550 nm", "1"),
        "OMAOD550": ("organic matter aerosol optical depth at 550 nm", "1"),
        "SSAOD550": ("sea salt aerosol optical depth at 550 nm", "1"),
        "SUAOD550": ("sulfate aerosol optical depth at 550 nm", "1"),
    }
    for var, (long_name, units) in cf_attrs.items():
        if var in ds:
            ds[var].attrs = {"long_name": long_name, "units": units}

    return ds


# ── Per-month worker (called from thread pool) ────────────────────────────────
_print_lock = threading.Lock()

def _safe_print(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs)


def _process_month(i: int, total: int, year: int, month: int) -> str:
    """
    Download + post-process one month.  Returns the path to the clean .nc file.
    Retries up to MAX_RETRIES times on CDS queue-rejection errors.
    Designed to be called from a ThreadPoolExecutor worker.
    """
    import time

    tag      = f"{year}{month:02d}"
    clean_nc = os.path.join(MONTHLY_DIR, f"cams_{tag}.nc")

    if os.path.exists(clean_nc):
        _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  skip (cached)")
        return clean_nc

    raw_nc = os.path.join(MONTHLY_DIR, f"cams_{tag}_raw.nc")

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        if os.path.exists(raw_nc):
            os.remove(raw_nc)

        try:
            if attempt == 1:
                _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  requesting …")
            else:
                delay = RETRY_DELAY * (2 ** (attempt - 2))  # 120 s, 240 s, 480 s …
                _safe_print(
                    f"[{i:2d}/{total}] {year}-{month:02d}  retry {attempt}/{MAX_RETRIES} "
                    f"(waiting {delay}s) …"
                )
                time.sleep(delay)

            download_month(year, month, raw_nc)
            last_exc = None
            break  # success — exit retry loop

        except Exception as exc:
            last_exc = exc
            _safe_print(
                f"[{i:2d}/{total}] {year}-{month:02d}  attempt {attempt}/{MAX_RETRIES} "
                f"failed: {exc!r}"
            )

    if last_exc is not None:
        raise last_exc

    _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  post-processing …")
    ds_raw   = _open_one_file(raw_nc)
    ds_clean = postprocess(ds_raw)

    ds_clean.time.attrs = {
        "long_name": "time",
        "note":      "UTC, timezone-naive",
    }

    ds_clean.to_netcdf(clean_nc)

    if os.path.exists(raw_nc):
        os.remove(raw_nc)

    _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  saved → {os.path.basename(clean_nc)}")
    return clean_nc


# ── Main ──────────────────────────────────────────────────────────────────────
def fetch_bbox() -> None:
    months = list(iter_months(START, END))
    total  = len(months)
    print(f"CAMS bounding-box download: {total} months  (MAX_WORKERS={MAX_WORKERS})")
    print(f"Bbox : N={BBOX[0]} W={BBOX[1]} S={BBOX[2]} E={BBOX[3]}")
    print(f"Range: {START[0]}-{START[1]:02d}  →  {END[0]}-{END[1]:02d}\n")

    failed = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_process_month, i + 1, total, y, m): (y, m)
            for i, (y, m) in enumerate(months)
        }
        for fut in as_completed(futures):
            y, m = futures[fut]
            try:
                fut.result()
            except Exception as exc:
                _safe_print(f"  ✗  {y}-{m:02d} FAILED: {exc}")
                failed.append((y, m))

    if failed:
        print(f"\nWarning: {len(failed)} month(s) failed:")
        for y, m in sorted(failed):
            print(f"  {y}-{m:02d}")
        log_path = os.path.join(OUTPUT_DIR, "failed_requests.log")
        with open(log_path, "w") as f:
            f.write(f"# Failed CAMS months ({pd.Timestamp.now().isoformat()})\n")
            for y, m in sorted(failed):
                f.write(f"{y}-{m:02d}\n")
        print(f"Failed list written to: {log_path}")

    print(f"\nDone → monthly files in {MONTHLY_DIR}")


if __name__ == "__main__":
    fetch_bbox()
