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

Datasets
────────
  Primary: cams-global-reanalysis-eac4
    Spatial : 0.75° × 0.75° global grid
    Temporal: 3-hourly (00, 03, 06, 09, 12, 15, 18, 21 UTC)
    Coverage: 2003-01-01 → ~2 years before present
    All variables are instantaneous (no accumulation, no deaccumulation needed).

  Fallback: cams-global-atmospheric-composition-forecasts
    Spatial : 0.4° × 0.4°  (only difference from EAC4 — handled downstream
                            by stage_b's `_interp_to_grid`, which reads
                            latitude/longitude from the file directly)
    Temporal: 3-hourly to match EAC4 — base times (00:00, 12:00) ×
              leadtime_hour (0, 3, 6, 9) → valid times {00,03,06,09,12,15,18,21}
    Coverage: near-real-time (closes the EAC4 ~2-year latency gap)
    Used per-month when EAC4 retrieval fails.  On-disk schema after
    postprocess is identical to EAC4 (time/lat/lon/AOD550 + species).

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
import numpy as np
import pandas as pd
import xarray as xr

# ── Configuration ──────────────────────────────────────────────────────────────
BBOX         = [23.5, 102.0, 8.0, 110.0]   # [N, W, S, E] — CDS convention
START        = (2026, 2)
END          = (2026, 2)

# How many months to download concurrently.
# CDS fair-use limit is ~20 active jobs per user; 1 job per month → keep ≤16.
MAX_WORKERS  = 4

# Retry settings for CDS queue-rejection errors
MAX_RETRIES  = 5      # max attempts per month
RETRY_DELAY  = 120    # seconds before first retry (doubles each attempt)

OUTPUT_DIR   = "/home/slow_data/Air_Quality/CAM"
MONTHLY_DIR  = os.path.join(OUTPUT_DIR, "_monthly_raw")

# ── CDS request constants ─────────────────────────────────────────────────────
CDS_DATASET            = "cams-global-reanalysis-eac4"
CDS_DATASET_FORECAST   = "cams-global-atmospheric-composition-forecasts"

CDS_VARIABLES =  [
        "black_carbon_aerosol_optical_depth_550nm",
        "dust_aerosol_optical_depth_550nm",
        "organic_matter_aerosol_optical_depth_550nm",
        "sea_salt_aerosol_optical_depth_550nm",
        "sulphate_aerosol_optical_depth_550nm",
        "total_aerosol_optical_depth_550nm"
    ]
CDS_TIMES              = [f"{h:02d}:00" for h in (0, 3, 6, 9, 12, 15, 18, 21)]
CDS_FORECAST_TIMES     = ["00:00", "12:00"]
CDS_FORECAST_LEADTIMES = ["0", "3", "6", "9"]          # 3-hourly to match EAC4's grid

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
    """Build the CDS API request body for a single month — EAC4 reanalysis."""
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


def _build_request_forecast(year: int, month: int) -> dict:
    """Build the CDS API request body for the forecast fallback — 3-hourly via
    (00:00, 12:00) base times × (0, 3, 6, 9) h leadtime → 8 valid times/day
    matching EAC4's {00,03,06,09,12,15,18,21} UTC grid."""
    n_days = calendar.monthrange(year, month)[1]
    start  = f"{year}-{month:02d}-01"
    end    = f"{year}-{month:02d}-{n_days:02d}"
    return {
        "variable":      CDS_VARIABLES,
        "date":          f"{start}/{end}",
        "time":          CDS_FORECAST_TIMES,
        "leadtime_hour": CDS_FORECAST_LEADTIMES,
        "type":          ["forecast"],
        "area":          BBOX,
        "data_format":   "netcdf_zip",
    }


def download_month(year: int, month: int, out_path: str) -> str:
    """Download one month of CAMS data.

    Tries EAC4 first; on any failure, falls back to the near-real-time forecast
    product.  Returns the dataset id that produced the file on disk (used by the
    caller for logging).  Raises if both datasets fail.
    """
    client = cdsapi.Client(quiet=True)
    try:
        client.retrieve(CDS_DATASET, _build_request(year, month), out_path)
        return CDS_DATASET
    except Exception as exc_eac4:
        try:
            client.retrieve(
                CDS_DATASET_FORECAST,
                _build_request_forecast(year, month),
                out_path,
            )
            return CDS_DATASET_FORECAST
        except Exception as exc_fc:
            raise RuntimeError(
                f"both CDS datasets failed for {year}-{month:02d} "
                f"(eac4: {exc_eac4!r}; forecast: {exc_fc!r})"
            ) from exc_fc


# ── Open a CDS download (zip or plain nc) ─────────────────────────────────────
def _flatten_forecast_time(ds: xr.Dataset) -> xr.Dataset:
    """Collapse (forecast_reference_time × forecast_period) into a single 1-D
    `time` axis of valid times.  No-op for EAC4 (which already ships a 1-D time).

    The forecast product returns variables shaped (n_base, n_lead, lat, lon)
    where n_base = 2 (00:00 + 12:00 base times) and n_lead = 12 (0..11 h).  We
    compute valid_time = base + lead and reshape to (n_base*n_lead, lat, lon) so
    downstream code can treat the file identically to EAC4's 3-hourly output.
    """
    base_dim = next(
        (n for n in ("forecast_reference_time", "reference_time") if n in ds.dims),
        None,
    )
    lead_dim = next(
        (n for n in ("forecast_period", "step", "leadtime_hour") if n in ds.dims),
        None,
    )
    if base_dim is None or lead_dim is None:
        return ds

    base_vals = ds[base_dim].values                    # datetime64[ns]
    lead_vals = ds[lead_dim].values                    # timedelta64 or integer hours
    if not np.issubdtype(lead_vals.dtype, np.timedelta64):
        lead_vals = lead_vals.astype("timedelta64[h]")

    valid_2d = base_vals[:, None] + lead_vals[None, :]  # (n_base, n_lead)
    valid_1d = valid_2d.reshape(-1)

    new_vars: dict[str, xr.DataArray] = {}
    for v in ds.data_vars:
        arr = ds[v]
        spatial = [d for d in arr.dims if d not in (base_dim, lead_dim)]
        arr = arr.transpose(base_dim, lead_dim, *spatial)
        np_arr = arr.values
        flat = np_arr.reshape(np_arr.shape[0] * np_arr.shape[1], *np_arr.shape[2:])
        new_vars[v] = xr.DataArray(
            flat,
            dims=("time", *spatial),
            coords={"time": valid_1d, **{d: ds[d] for d in spatial}},
            attrs=arr.attrs,
        )

    out = xr.Dataset(new_vars, attrs=ds.attrs).sortby("time")
    # Drop duplicate valid times (defensive — the request schema shouldn't produce any)
    _, uniq = np.unique(out["time"].values, return_index=True)
    if len(uniq) < out.sizes["time"]:
        out = out.isel(time=np.sort(uniq))
    return out


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

    # Forecast files have (forecast_reference_time × forecast_period) instead of
    # a 1-D time axis.  Flatten to match the EAC4 schema before downstream use.
    ds = _flatten_forecast_time(ds)

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

    last_exc      = None
    used_dataset  = None
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

            used_dataset = download_month(year, month, raw_nc)
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

    src_label = "forecast" if used_dataset == CDS_DATASET_FORECAST else "eac4"
    _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  post-processing ({src_label}) …")
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
