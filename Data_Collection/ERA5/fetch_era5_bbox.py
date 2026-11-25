"""
Download ERA5 single-level data for the Vietnam bounding box via CDS API.

Strategy
────────
  Two CDS requests per month (instant + accumulated variables submitted
  separately → smaller jobs, faster CDS queue scheduling).
  Checkpoint .nc file per month → merge at the end.
  Restarting the script skips months already on disk.

Requires
────────
  pip install cdsapi xarray netCDF4 scipy
  ~/.cdsapirc  (or CDSAPI_URL / CDSAPI_KEY env vars) with valid credentials.
  CDS account: https://cds.climate.copernicus.eu

Output variables (all saved in the final NetCDF)
────────────────────────────────────────────────
  T2m          2-m temperature                        [K → °C]
  Td2m         2-m dewpoint temperature               [K → °C]
  RH           2-m relative humidity (derived)        [%]
  Psfc         surface pressure                       [Pa → hPa]
  U10          10-m U wind component (eastward)       [m/s]
  V10          10-m V wind component (northward)      [m/s]
  WS10m        10-m wind speed (derived)              [m/s]
  WD10m        10-m wind direction met. (derived)     [°]
  PBLH         boundary layer height                  [m]
  CloudCover   total cloud cover                      [0–1 → %]
  SolarRad     surface solar radiation downwelling    [J/m² per hour → W/m²]
  Precip       total precipitation                    [m per hour → mm]
  Albedo       forecast surface albedo                [0–1 → %]

Notes on accumulated fields (ssrd, tp)
──────────────────────────────────────
  `reanalysis-era5-single-levels` on the CDS already delivers each value as
  the accumulation over the 1 hour ENDING at its validity time, so only a
  unit conversion is needed — no differencing.  (Versions before 2026-10-24
  differenced consecutive hours as if they were run-cumulative; that wiped
  out afternoon radiation and ~2/3 of the rain.  Fix existing monthly files
  with --repair-accum.)
  Timestamps stay in UTC throughout — no timezone shift is applied.

Usage
─────
  python fetch_era5_bbox.py                  # download missing months START → END
  python fetch_era5_bbox.py --repair-accum   # re-download ssrd/tp only and overwrite
                                             # SolarRad/Precip in existing monthly files
  python fetch_era5_bbox.py --repair-accum --months 202404   # one month (test)
"""

import calendar
import glob
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
# bbox is [N, W, S, E] — CDS convention.  Chọn bằng --region (mặc định vietnam).
REGIONS = {
    "vietnam": dict(bbox=[23.5, 102.0, 8.0, 110.0], start=(2020, 1), end=(2022, 1),
                    output_dir="/home/slow_data/Air_Quality/ERA5"),
    "china":   dict(bbox=[54.0, 73.0, 18.0, 135.0], start=(2020, 1), end=(2026, 10),
                    output_dir="/home/slow_data/Air_Quality/ERA5/china"),
}
BBOX         = REGIONS["vietnam"]["bbox"]
START        = REGIONS["vietnam"]["start"]
END          = REGIONS["vietnam"]["end"]

# How many months to download concurrently.  The CDS now rejects a second queued
# request on the same dataset ("Maximum number queued requests … request data
# sequentially", 2026-11), so months and their instant/accum parts run one at a time.
MAX_WORKERS  = 1

# Retry settings for CDS queue-rejection errors
MAX_RETRIES  = 5      # max attempts per month
RETRY_DELAY  = 120    # seconds before first retry (doubles each attempt)

OUTPUT_DIR   = REGIONS["vietnam"]["output_dir"]
OUTPUT_FILE  = os.path.join(OUTPUT_DIR, "Vietnam_ERA5_bbox.nc")
MONTHLY_DIR  = os.path.join(OUTPUT_DIR, "_monthly_raw")

# Skip the giant single-file merge.  Downstream code should use ERA5/load.py
# (xr.open_mfdataset over MONTHLY_DIR), which is more robust, uses less disk,
# and extends naturally when new months are added.
MERGE_MONTHLY = False

# ── Split variables by step type for separate CDS requests ────────────────────
# FIX (performance): mixing instant+accum in one request forces CDS to split
# them internally and serialise the queue jobs.  Two small requests schedule
# faster and can run concurrently on the CDS back-end.

CDS_INSTANT_VARS = [
    "2m_temperature",                      # t2m  — instantaneous [K]
    "2m_dewpoint_temperature",             # d2m  — instantaneous [K]
    "surface_pressure",                    # sp   — instantaneous [Pa]
    "mean_sea_level_pressure",             # msl  — instantaneous [Pa]
    "10m_u_component_of_wind",             # u10  — instantaneous [m/s]
    "10m_v_component_of_wind",             # v10  — instantaneous [m/s]
    "100m_u_component_of_wind",            # u100 — instantaneous [m/s]
    "100m_v_component_of_wind",            # v100 — instantaneous [m/s]
    "boundary_layer_height",               # blh  — instantaneous [m]
    "total_cloud_cover",                   # tcc  — instantaneous [0–1]
    "cloud_base_height",                   # cbh  — instantaneous [m]
    "total_column_water_vapour",           # tcwv — instantaneous [kg m-2]
    "forecast_albedo",                     # fal  — instantaneous [0–1]
    "convective_available_potential_energy",  # cape — instantaneous [J kg-1]
]

CDS_ACCUM_VARS = [
    "surface_solar_radiation_downwards",   # ssrd — 1-h accumulation [J/m²]
    "total_precipitation",                 # tp   — 1-h accumulation [m]
]

# Global attribute stamped on monthly files whose SolarRad/Precip come from the
# corrected (no-differencing) processing; --repair-accum skips files that have it.
ACCUM_FIX_ATTR = "accum_fix"
ACCUM_FIX_NOTE = "ssrd/tp taken as 1-h accumulations, unit conversion only (no differencing)"

os.makedirs(OUTPUT_DIR,  exist_ok=True)
os.makedirs(MONTHLY_DIR, exist_ok=True)


def set_region(name: str) -> None:
    global BBOX, START, END, OUTPUT_DIR, OUTPUT_FILE, MONTHLY_DIR
    r = REGIONS[name]
    BBOX, START, END, OUTPUT_DIR = r["bbox"], r["start"], r["end"], r["output_dir"]
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"{name.capitalize()}_ERA5_bbox.nc")
    MONTHLY_DIR = os.path.join(OUTPUT_DIR, "_monthly_raw")
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
def _build_request(year: int, month: int, variables: list[str]) -> dict:
    """Build the CDS API request body for a single month and variable list."""
    n_days   = calendar.monthrange(year, month)[1]
    all_days = [f"{d:02d}" for d in range(1, n_days + 1)]
    all_hrs  = [f"{h:02d}:00" for h in range(24)]
    return {
        "product_type": "reanalysis",
        "variable":     variables,
        "year":         str(year),
        "month":        f"{month:02d}",
        "day":          all_days,
        "time":         all_hrs,
        "area":         BBOX,
        "data_format":  "netcdf",   # FIX: explicit format avoids ambiguous server
                                    # behaviour; new CDS API key is 'data_format'
    }


def download_month(year: int, month: int,
                   out_instant: str, out_accum: str) -> None:
    """Submit the instant then the accumulated CDS request for one month (sequential, see MAX_WORKERS)."""
    client = cdsapi.Client(quiet=True)
    for variables, out_path in ((CDS_INSTANT_VARS, out_instant), (CDS_ACCUM_VARS, out_accum)):
        client.retrieve("reanalysis-era5-single-levels",
                        _build_request(year, month, variables), out_path)


# ── Open a CDS download (zip or plain nc) ─────────────────────────────────────
def _open_one_file(path: str) -> xr.Dataset:
    """
    Open a single CDS download, handling both ZIP and plain NetCDF formats.

    FIX (critical): xr.open_dataset is lazy.  We call .load() inside the
    context manager so all data is read into memory before the temp directory
    (or the original file handle) is released.

    FIX (robustness): the new CDS API may return either a .zip containing one
    or more .nc files, or a plain .nc directly.  Both are handled here.
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
            parts = [xr.open_dataset(f).load() for f in nc_files]  # .load() ← FIX
            ds = xr.merge(parts, compat="no_conflicts") if len(parts) > 1 else parts[0]
    else:
        # Plain NetCDF — load immediately so the file handle is not kept open
        ds = xr.open_dataset(path).load()

    # Normalise dimension name: new CDS API uses 'valid_time', old uses 'time'
    if "valid_time" in ds.dims:
        ds = ds.rename({"valid_time": "time"})
    if "valid_time" in ds.coords and "valid_time" not in ds.dims:
        ds = ds.drop_vars("valid_time")

    return ds


def open_month_datasets(instant_path: str, accum_path: str) -> xr.Dataset:
    """Load and merge the two monthly download files."""
    ds_instant = _open_one_file(instant_path)
    ds_accum   = _open_one_file(accum_path)

    t_i = ds_instant.time.values
    t_a = ds_accum.time.values

    if not ds_instant.time.equals(ds_accum.time):
        # Diagnose the mismatch before deciding what to do
        _safe_print(
            f"    ⚠ time mismatch: instant has {len(t_i)} steps "
            f"({t_i[0]} … {t_i[-1]}), "
            f"accum has {len(t_a)} steps ({t_a[0]} … {t_a[-1]})"
        )
        # Tolerate a minor tail difference (CDS sometimes returns N vs N±1 hours
        # for the most recent months).  Take the intersection.
        common = np.intersect1d(t_i, t_a)
        if len(common) == 0:
            msg = (
                "Time axes of instant and accum downloads share no common timestamps"
                " — CDS returned completely inconsistent data.\n"
                f"  instant: {len(t_i)} steps  {t_i[0]} ... {t_i[-1]}\n"
                f"  accum  : {len(t_a)} steps  {t_a[0]} ... {t_a[-1]}"
            )
            raise ValueError(msg)
        dropped = max(len(t_i), len(t_a)) - len(common)
        _safe_print(f"    → using {len(common)} common timestamps (dropped {dropped} non-overlapping)")
        ds_instant = ds_instant.sel(time=common)
        ds_accum   = ds_accum.sel(time=common)

    # Drop CDS metadata variables that differ between instant/accum files
    # (expver = experiment version, changes for recent near-real-time data)
    for drop_var in ("expver", "number"):
        if drop_var in ds_instant:
            ds_instant = ds_instant.drop_vars(drop_var)
        if drop_var in ds_accum:
            ds_accum = ds_accum.drop_vars(drop_var)

    return xr.merge([ds_instant, ds_accum], compat="no_conflicts")


# ── Unit conversions & derived variables ──────────────────────────────────────
def postprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Convert units, derive RH / WS / WD, rename, add CF attrs.
    Operates on UTC-timestamped data; no timezone shift is applied.
    """
    rename_map = {
        "t2m":   "T2m",
        "d2m":   "Td2m",
        "sp":    "Psfc",
        "msl":   "MSLP",
        "u10":   "U10",
        "v10":   "V10",
        "u100":  "U100",
        "v100":  "V100",
        "blh":   "PBLH",
        "tcc":   "CloudCover",
        "cbh":   "CBH",
        "tcwv":  "TCWV",
        "ssrd":  "SolarRad",
        "tp":    "Precip",
        "fal":   "Albedo",
        "cape":  "CAPE",
    }

    # ssrd / tp are already 1-h accumulations (see module notes): the unit
    # conversions below are all they need — no differencing.

    # Rename to output variable names
    present = {k: v for k, v in rename_map.items() if k in ds}
    ds = ds.rename(present)

    # Unit conversions
    if "T2m"      in ds: ds["T2m"]      = ds["T2m"]  - 273.15          # K → °C
    if "Td2m"     in ds: ds["Td2m"]     = ds["Td2m"] - 273.15          # K → °C
    if "Psfc"     in ds: ds["Psfc"]     = ds["Psfc"] / 100.0           # Pa → hPa
    if "MSLP"     in ds: ds["MSLP"]     = ds["MSLP"] / 100.0           # Pa → hPa
    if "SolarRad" in ds:
        ds["SolarRad"] = (ds["SolarRad"] / 3600.0).clip(min=0)         # J/m²/h → W/m²
    if "Precip"   in ds:
        ds["Precip"]   = (ds["Precip"]   * 1000.0).clip(min=0)         # m → mm
    if "CloudCover" in ds: ds["CloudCover"] = ds["CloudCover"] * 100    # 0–1 → %
    if "Albedo"     in ds: ds["Albedo"]     = ds["Albedo"]     * 100    # 0–1 → %

    # Relative humidity — August-Roche-Magnus approximation (T, Td in °C)
    if "T2m" in ds and "Td2m" in ds:
        T, Td = ds["T2m"], ds["Td2m"]
        rh = (
            100.0
            * np.exp(17.625 * Td / (243.04 + Td))
            / np.exp(17.625 * T  / (243.04 + T))
        )
        ds["RH"] = rh.clip(0, 100).astype(np.float32)

    # 10-m wind speed and direction (meteorological convention)
    if "U10" in ds and "V10" in ds:
        u, v = ds["U10"], ds["V10"]
        ds["WS10m"] = np.sqrt(u**2 + v**2).astype(np.float32)
        # Direction FROM which wind blows, measured clockwise from North
        ds["WD10m"] = ((270.0 - np.degrees(np.arctan2(v, u))) % 360.0).astype(np.float32)

    # CF-style variable attributes
    cf_attrs = {
        "T2m":        ("2-metre air temperature",                  "degC"),
        "Td2m":       ("2-metre dewpoint temperature",             "degC"),
        "RH":         ("2-metre relative humidity",                "%"),
        "Psfc":       ("surface pressure",                         "hPa"),
        "MSLP":       ("mean sea level pressure",                  "hPa"),
        "U10":        ("10-m U wind component (eastward)",         "m s-1"),
        "V10":        ("10-m V wind component (northward)",        "m s-1"),
        "WS10m":      ("10-m wind speed",                          "m s-1"),
        "WD10m":      ("10-m wind direction (meteorological)",     "degrees"),
        "U100":       ("100-m U wind component (eastward)",        "m s-1"),
        "V100":       ("100-m V wind component (northward)",       "m s-1"),
        "PBLH":       ("planetary boundary layer height",          "m"),
        "CloudCover": ("total cloud cover",                        "%"),
        "CBH":        ("cloud base height",                        "m"),
        "TCWV":       ("total column water vapour",                "kg m-2"),
        "SolarRad":   ("surface solar radiation downwelling",      "W m-2"),
        "Precip":     ("total precipitation",                      "mm"),
        "Albedo":     ("forecast surface albedo",                  "%"),
        "CAPE":       ("convective available potential energy",    "J kg-1"),
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
    clean_nc = os.path.join(MONTHLY_DIR, f"era5_{tag}.nc")

    if os.path.exists(clean_nc):
        _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  skip (cached)")
        return clean_nc

    raw_instant = os.path.join(MONTHLY_DIR, f"era5_{tag}_instant.nc")
    raw_accum   = os.path.join(MONTHLY_DIR, f"era5_{tag}_accum.nc")

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        # Clean up any partial downloads from a previous attempt
        for f in (raw_instant, raw_accum):
            if os.path.exists(f):
                os.remove(f)

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

            download_month(year, month, raw_instant, raw_accum)
            last_exc = None
            break  # success — exit retry loop

        except Exception as exc:
            last_exc = exc
            # Broader retry: retry on ANY exception (network blips, CDS 5xx,
            # zip-corruption, transient xarray errors, etc.).  The previous
            # narrow filter on "temporarily limited"/"rejected" missed too
            # many recoverable failures.  The outer logger in fetch_bbox()
            # records the month if all attempts are exhausted.
            _safe_print(
                f"[{i:2d}/{total}] {year}-{month:02d}  attempt {attempt}/{MAX_RETRIES} "
                f"failed: {exc!r}"
            )

    if last_exc is not None:
        raise last_exc

    _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  post-processing …")
    ds_raw   = open_month_datasets(raw_instant, raw_accum)
    ds_clean = postprocess(ds_raw)

    ds_clean.time.attrs = {
        "long_name": "time",
        "note":      "UTC, timezone-naive",
    }
    ds_clean.attrs[ACCUM_FIX_ATTR] = ACCUM_FIX_NOTE

    ds_clean.to_netcdf(clean_nc)

    for f in (raw_instant, raw_accum):
        if os.path.exists(f):
            os.remove(f)

    _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  saved → {os.path.basename(clean_nc)}")
    return clean_nc


# ── Repair: SolarRad / Precip of months saved with the old differencing ──────
def _repair_accum_year(year: int, files: list[str]) -> list[str]:
    """
    Re-download ssrd/tp for the months of `year` that still lack ACCUM_FIX_ATTR and
    overwrite SolarRad/Precip in each era5_YYYYMM.nc; every other variable is left
    untouched.  ONE CDS request per year: the CDS rejects parallel requests ("request
    data sequentially") and each request waits in its queue, so 7 yearly jobs beat
    78 monthly ones (a year of 2 variables is ~17.5k fields, within the CDS limit).
    Each month is written to a temp file then os.replace()d, so an interrupted run
    never leaves a half-written month.
    """
    import time

    todo = []
    for f in files:
        with xr.open_dataset(f) as ds:
            if ACCUM_FIX_ATTR not in ds.attrs:
                todo.append(f)
    if not todo:
        _safe_print(f"{year}: skip (all {len(files)} months already fixed)")
        return []
    months = [int(os.path.basename(f)[9:11]) for f in todo]
    request = _build_request(year, months[0], CDS_ACCUM_VARS)
    request["month"] = [f"{m:02d}" for m in months]
    request["day"] = [f"{d:02d}" for d in range(1, 32)]     # CDS skips non-existent dates

    raw_accum = os.path.join(MONTHLY_DIR, f"era5_{year}_accum.nc")
    for attempt in range(1, MAX_RETRIES + 1):
        if os.path.exists(raw_accum):
            os.remove(raw_accum)
        try:
            if attempt == 1:
                _safe_print(f"{year}: requesting ssrd/tp for {len(months)} month(s) …")
            else:
                delay = RETRY_DELAY * (2 ** (attempt - 2))
                _safe_print(f"{year}: retry {attempt}/{MAX_RETRIES} (waiting {delay}s) …")
                time.sleep(delay)
            cdsapi.Client(quiet=True).retrieve("reanalysis-era5-single-levels", request, raw_accum)
            break
        except Exception as exc:
            _safe_print(f"{year}: attempt {attempt}/{MAX_RETRIES} failed: {exc!r}")
            if attempt == MAX_RETRIES:
                raise

    acc = _open_one_file(raw_accum)
    acc = acc.drop_vars([v for v in ("expver", "number") if v in acc])
    acc = postprocess(acc)                       # → SolarRad [W/m²], Precip [mm]

    for clean_nc in todo:
        ds = xr.open_dataset(clean_nc).load()    # loaded: the file is replaced below
        ds.close()
        # Same hours as the existing file (KeyError if the new download lacks any)
        new = acc[["SolarRad", "Precip"]].sel(time=ds.time.values)
        for c in ("latitude", "longitude"):
            if not np.array_equal(new[c].values, ds[c].values):
                raise ValueError(f"{c} of the new download differs from {os.path.basename(clean_nc)}")
        for v in ("SolarRad", "Precip"):
            ds[v] = new[v].astype(np.float32)
        ds.attrs[ACCUM_FIX_ATTR] = ACCUM_FIX_NOTE
        tmp_nc = clean_nc + ".tmp"
        ds.to_netcdf(tmp_nc)
        os.replace(tmp_nc, clean_nc)
        _safe_print(f"  {os.path.basename(clean_nc)}  SolarRad/Precip replaced "
                    f"(mean {float(ds.SolarRad.mean()):.1f} W/m², {float(ds.Precip.mean()) * 24:.2f} mm/day)")
    os.remove(raw_accum)
    return todo


def repair_accum(months: list[str] | None = None) -> None:
    """Run _repair_accum_year year by year over the monthly files (or only the YYYYMM given)."""
    files = sorted(glob.glob(os.path.join(MONTHLY_DIR, "era5_??????.nc")))
    if months:
        files = [f for f in files if os.path.basename(f)[5:11] in set(months)]
    by_year: dict[int, list[str]] = {}
    for f in files:
        by_year.setdefault(int(os.path.basename(f)[5:9]), []).append(f)
    print(f"ERA5 ssrd/tp repair: {len(files)} monthly files in {len(by_year)} year(s), one CDS request per year")
    failed = []
    for year, year_files in by_year.items():     # sequential — the CDS rejects parallel requests
        try:
            _repair_accum_year(year, year_files)
        except Exception as exc:
            _safe_print(f"  ✗  {year} FAILED: {exc!r}")
            failed.append(year)
    print(f"Repair done: {len(by_year) - len(failed)} year(s) ok, failed {failed}")


# ── Main ──────────────────────────────────────────────────────────────────────
def fetch_bbox() -> None:
    months = list(iter_months(START, END))
    total  = len(months)
    print(f"ERA5 bounding-box download: {total} months  (MAX_WORKERS={MAX_WORKERS})")
    print(f"Bbox : N={BBOX[0]} W={BBOX[1]} S={BBOX[2]} E={BBOX[3]}")
    print(f"Range: {START[0]}-{START[1]:02d}  →  {END[0]}-{END[1]:02d}\n")

    # Download up to MAX_WORKERS months concurrently.
    # Each month itself downloads its 2 CDS jobs (instant + accum) in parallel,
    # so the total active CDS jobs = MAX_WORKERS * 2.  Keep MAX_WORKERS ≤ 8.
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
        print(f"\nWarning: {len(failed)} month(s) failed and will be missing from the merge:")
        for y, m in sorted(failed):
            print(f"  {y}-{m:02d}")
        log_path = os.path.join(OUTPUT_DIR, "failed_requests.log")
        with open(log_path, "w") as f:
            f.write(f"# Failed ERA5 months ({pd.Timestamp.now().isoformat()})\n")
            for y, m in sorted(failed):
                f.write(f"{y}-{m:02d}\n")
        print(f"Failed list written to: {log_path}")

    # ── Merge monthly files ────────────────────────────────────────────────────
    if not MERGE_MONTHLY:
        print(
            f"\nMerge step skipped (MERGE_MONTHLY=False). "
            f"Use ERA5/load.py → load_era5_bbox() to read the monthly files lazily."
        )
        return

    monthly_files = sorted(glob.glob(os.path.join(MONTHLY_DIR, "era5_??????.nc")))
    if not monthly_files:
        print("No monthly files found — nothing to merge.")
        return

    print(f"\nMerging {len(monthly_files)} monthly files → {OUTPUT_FILE}")
    # Streaming write: only one month is in RAM at a time (~110 MB).
    # xarray's to_netcdf(mode="a") doesn't actually extend an unlimited dim
    # (it just overwrites matching-shape vars), so we use the netCDF4 library
    # directly to append along the time axis after seeding the file with
    # month 1 via xarray.
    import netCDF4

    last_day = calendar.monthrange(END[0], END[1])[1]
    t_end    = pd.Timestamp(f"{END[0]}-{END[1]:02d}-{last_day:02d} 23:00")

    global_attrs = {
        "title":           "Vietnam ERA5 single-level weather",
        "bounding_box":    f"N={BBOX[0]} W={BBOX[1]} S={BBOX[2]} E={BBOX[3]}",
        "date_range":      f"{START[0]}-{START[1]:02d} to {END[0]}-{END[1]:02d}",
        "source":          "ERA5 reanalysis — Copernicus Climate Data Store (CDS API)",
        "timezone":        "UTC, tz-naive timestamps",
        "grid_resolution": "0.25 degrees",
        "created_by":      "ERA5/fetch_era5_bbox.py",
    }

    def _load_month(fp: str) -> xr.Dataset:
        ds = xr.open_dataset(fp, engine="netcdf4").load()
        for drop_var in ("expver", "number"):
            if drop_var in ds:
                ds = ds.drop_vars(drop_var)
        rename_dims = {}
        if "latitude"  not in ds.dims and "lat" in ds.dims:
            rename_dims["lat"] = "latitude"
        if "longitude" not in ds.dims and "lon" in ds.dims:
            rename_dims["lon"] = "longitude"
        if rename_dims:
            ds = ds.rename(rename_dims)
        return ds.sel(time=slice(None, t_end))

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    # Seed the output file with the first month (xarray handles all the
    # encoding/attrs/coords setup for us; time is the unlimited dim).
    ds0 = _load_month(monthly_files[0])
    if ds0.sizes["time"] == 0:
        raise RuntimeError(f"First monthly file is empty after slicing: {monthly_files[0]}")
    ds0.attrs = global_attrs
    ds0.to_netcdf(OUTPUT_FILE, mode="w", unlimited_dims=["time"])
    _safe_print(f"  seeded {os.path.basename(monthly_files[0])}  ({ds0.sizes['time']} hours)")
    ds0.close()

    # Append remaining months along the unlimited time axis using netCDF4.
    for fp in monthly_files[1:]:
        ds = _load_month(fp)
        n = ds.sizes["time"]
        if n == 0:
            ds.close()
            continue

        with netCDF4.Dataset(OUTPUT_FILE, mode="a") as nc:
            t_start = nc.dimensions["time"].size
            sl = slice(t_start, t_start + n)

            # Append the time coord (convert to the file's numeric units).
            time_var = nc.variables["time"]
            time_vals = netCDF4.date2num(
                pd.DatetimeIndex(ds["time"].values).to_pydatetime(),
                units=time_var.units,
                calendar=getattr(time_var, "calendar", "standard"),
            )
            time_var[sl] = time_vals

            # Append every data variable that has a time dimension.
            for vname, da in ds.data_vars.items():
                if "time" not in da.dims or vname not in nc.variables:
                    continue
                nc.variables[vname][sl] = da.values

        _safe_print(f"  appended {os.path.basename(fp)}  ({n} hours)")
        ds.close()

    print(f"Done → {OUTPUT_FILE}")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="ERA5 single levels → region bbox, monthly files")
    ap.add_argument("--region", choices=list(REGIONS), default="vietnam")
    ap.add_argument("--repair-accum", action="store_true",
                    help="re-download ssrd/tp only and overwrite SolarRad/Precip in existing monthly files")
    ap.add_argument("--months", nargs="+", metavar="YYYYMM",
                    help="with --repair-accum: only these months")
    args = ap.parse_args()
    set_region(args.region)
    # ~/.cdsapirc is shared with the CAMS scripts, which point it at the ADS; ERA5 only
    # exists on the CDS — stop now instead of retrying 404s for half an hour.
    url = cdsapi.Client(quiet=True).url
    if "cds.climate.copernicus.eu" not in url:
        raise SystemExit(f"cdsapi points to {url}, not the CDS (ERA5 lives there). Run with\n"
                         f"  CDSAPI_URL=https://cds.climate.copernicus.eu/api python {os.path.basename(__file__)} …")
    if args.repair_accum:
        repair_accum(args.months)
    else:
        fetch_bbox()
