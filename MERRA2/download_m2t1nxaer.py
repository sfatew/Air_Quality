"""
MERRA-2 M2T1NXAER downloader (hourly time-averaged aerosol diagnostics).

Source : GES DISC  (goldsmr4.gesdisc.eosdis.nasa.gov)
Product: M2T1NXAER.5.12.4  — one .nc4 granule per day, 24 hourly steps,
         0.5° lat × 0.625° lon global grid.

Auth   : Earthdata Login credentials from config/config.yaml (EARTHDATA section).
         Mirrors the approach in the GES DISC tutorial:
         https://github.com/nasa/gesdisc-tutorials/blob/main/notebooks/
         How_to_Access_GES_DISC_Data_Using_Python.ipynb

Postprocessing
--------------
After each granule downloads, a Vietnam-bbox subset is written next to it
(suffix `_vnm.nc4`).  Bbox = [N=23.5, W=102.0, S=8.0, E=110.0]
matches AOD_map/stage_a and ERA5/fetch_era5_bbox.py.
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
import xarray as xr
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
GESDISC_HOST = "goldsmr4.gesdisc.eosdis.nasa.gov"
PRODUCT_PATH = "data/MERRA2/M2T1NXAER.5.12.4"

# MERRA-2 stream prefix per granule date (see GES DISC docs):
#   100: 1980–1991, 200: 1992–2000, 300: 2001–2010, 400: 2011–present
# (the 401 NRT prefix is reserved for the most recent ~2 months — we fall
#  back to it automatically when the 400 URL 404s.)
def _stream_prefix(d: datetime) -> str:
    if d.year < 1992: return "100"
    if d.year < 2001: return "200"
    if d.year < 2011: return "300"
    return "400"

# Vietnam bbox (same convention as ERA5/fetch_era5_bbox.py: [N, W, S, E])
VIETNAM_BBOX = {"N": 23.5, "W": 102.0, "S": 8.0, "E": 110.0}

# Network
MAX_RETRIES = 5
TIMEOUT = 120
CHUNK = 1024 * 1024  # 1 MiB

# Load Earthdata credentials
with open(ROOT_DIR / "config" / "config.yaml", "r") as f:
    _cfg = yaml.safe_load(f)
EARTHDATA_USER = _cfg["EARTHDATA"]["EARTHDATA_USERNAME"]
EARTHDATA_PASS = _cfg["EARTHDATA"]["EARTHDATA_PASSWORD"]


# ---------------------------------------------------------------------------
# Earthdata-aware HTTP session
# ---------------------------------------------------------------------------
class EarthdataSession(requests.Session):
    """
    Session that re-attaches Basic-Auth on the URS redirect.

    requests strips the Authorization header when it follows a cross-domain
    redirect, so the standard pattern (used in the GES DISC tutorial) is to
    subclass Session and re-inject the header when bouncing back from
    urs.earthdata.nasa.gov to the data host.
    """

    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username: str, password: str):
        super().__init__()
        self.auth = (username, password)
        retries = Retry(
            total=MAX_RETRIES,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.mount("https://", adapter)
        self.headers.update({"user-agent": "merra2-downloader/1.0"})

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url
        if "Authorization" in headers:
            original_host = requests.utils.urlparse(response.request.url).hostname
            redirect_host = requests.utils.urlparse(url).hostname
            if (
                original_host != redirect_host
                and redirect_host != self.AUTH_HOST
                and original_host != self.AUTH_HOST
            ):
                del headers["Authorization"]
        return


# ---------------------------------------------------------------------------
# Downloader
# ---------------------------------------------------------------------------
class M2T1NXAERDownloader:
    def __init__(
        self,
        username: str,
        password: str,
        output_dir: str | Path,
        bbox: Optional[dict] = VIETNAM_BBOX,
        keep_global: bool = False,
    ):
        """
        Parameters
        ----------
        bbox        : {"N", "W", "S", "E"} dict — Vietnam bbox by default.
                      Set to None to skip the postprocessing step.
        keep_global : If False (default), the full-globe granule is deleted
                      after the subset is written.  Saves ~25–30 MB/day.
        """
        self.session = EarthdataSession(username, password)
        self.base_dir = Path(output_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.bbox = bbox
        self.keep_global = keep_global
        self.stats = {
            "downloaded": 0,
            "skipped": 0,
            "subset_written": 0,
            "failed": [],
        }

    # ── URL building ──────────────────────────────────────────────────────
    def _granule_name(self, d: datetime, stream: str) -> str:
        return f"MERRA2_{stream}.tavg1_2d_aer_Nx.{d:%Y%m%d}.nc4"

    def _granule_url(self, d: datetime, stream: str) -> str:
        return (
            f"https://{GESDISC_HOST}/{PRODUCT_PATH}/"
            f"{d:%Y}/{d:%m}/{self._granule_name(d, stream)}"
        )

    # ── Single-day download ───────────────────────────────────────────────
    def _download_one(self, url: str, dest: Path) -> bool:
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            with self.session.get(url, stream=True, timeout=TIMEOUT) as r:
                if r.status_code == 404:
                    return False
                r.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=CHUNK):
                        if chunk:
                            fh.write(chunk)
            tmp.rename(dest)
            return True
        except Exception as e:
            logger.error("Download failed %s: %s", url, e)
            if tmp.exists():
                tmp.unlink()
            self.stats["failed"].append({"url": url, "reason": str(e)})
            return False

    # ── Postprocess: crop to bbox ─────────────────────────────────────────
    def _subset_to_bbox(self, src: Path, dest: Path) -> bool:
        try:
            with xr.open_dataset(src, engine="netcdf4") as ds:
                # MERRA-2 lat/lon are ascending; select an inclusive window.
                sub = ds.sel(
                    lat=slice(self.bbox["S"], self.bbox["N"]),
                    lon=slice(self.bbox["W"], self.bbox["E"]),
                )
                comp = {"zlib": True, "complevel": 4}
                encoding = {v: comp for v in sub.data_vars}
                sub.to_netcdf(dest, engine="netcdf4", encoding=encoding)
            return True
        except Exception as e:
            logger.error("Subset failed %s: %s", src.name, e)
            self.stats["failed"].append({"url": str(src), "reason": f"subset: {e}"})
            return False

    # ── Per-date orchestration ────────────────────────────────────────────
    def download_for_date(self, date_obj: datetime) -> None:
        local_dir = self.base_dir / f"{date_obj:%Y}" / f"{date_obj:%m}"
        local_dir.mkdir(parents=True, exist_ok=True)

        stream = _stream_prefix(date_obj)
        granule = self._granule_name(date_obj, stream)
        dest = local_dir / granule
        subset_dest = local_dir / granule.replace(".nc4", "_vnm.nc4")

        # Already processed?
        if self.bbox is not None and subset_dest.exists() and subset_dest.stat().st_size > 0:
            self.stats["skipped"] += 1
            return
        if self.bbox is None and dest.exists() and dest.stat().st_size > 0:
            self.stats["skipped"] += 1
            return

        # Download (try standard stream, then NRT fallback)
        ok = False
        if not dest.exists() or dest.stat().st_size == 0:
            url = self._granule_url(date_obj, stream)
            ok = self._download_one(url, dest)
            if not ok and stream == "400":
                nrt_url = self._granule_url(date_obj, "401")
                ok = self._download_one(nrt_url, dest)
            if ok:
                self.stats["downloaded"] += 1
            else:
                return
        else:
            ok = True

        # Subset
        if self.bbox is not None and ok:
            if self._subset_to_bbox(dest, subset_dest):
                self.stats["subset_written"] += 1
                if not self.keep_global:
                    dest.unlink()

    # ── Range driver ──────────────────────────────────────────────────────
    def download_range(self, start: datetime, end: datetime) -> None:
        d = start
        days = (end - start).days + 1
        bar = tqdm(
            range(days),
            desc="M2T1NXAER",
            unit="day",
            mininterval=1.0,
            smoothing=0.1,
        )
        for _ in bar:
            try:
                self.download_for_date(d)
            except Exception as e:
                logger.error("Error on %s: %s", d.strftime("%Y-%m-%d"), e)
                self.stats["failed"].append(
                    {"url": d.strftime("%Y-%m-%d"), "reason": str(e)}
                )
            bar.set_postfix(
                dl=self.stats["downloaded"],
                sub=self.stats["subset_written"],
                skip=self.stats["skipped"],
                err=len(self.stats["failed"]),
                refresh=False,
            )
            d += timedelta(days=1)
            time.sleep(0.1)  # be polite to the server


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------
def run_merra2_m2t1nxaer(
    start: str,
    end: Optional[str] = None,
    output_dir: str = "/home/slow_data/Air_Quality/MERRA2/M2T1NXAER",
    bbox: Optional[dict] = VIETNAM_BBOX,
    keep_global: bool = False,
) -> dict:
    """
    Download a date range of M2T1NXAER granules and crop each to `bbox`.

    Parameters
    ----------
    start, end  : "YYYY-MM-DD".  `end` defaults to yesterday (UTC).
    output_dir  : Root output folder.  Layout: <root>/<YYYY>/<MM>/<granule>.nc4
    bbox        : Crop window.  None = keep full globe.
    keep_global : Keep the original global file alongside the subset.
    """
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = (
        datetime.strptime(end, "%Y-%m-%d")
        if end else datetime.utcnow() - timedelta(days=1)
    )

    dl = M2T1NXAERDownloader(
        username=EARTHDATA_USER,
        password=EARTHDATA_PASS,
        output_dir=output_dir,
        bbox=bbox,
        keep_global=keep_global,
    )
    dl.download_range(start_date, end_date)

    report = {
        "product": "M2T1NXAER",
        "start": start_date.strftime("%Y-%m-%d"),
        "end": end_date.strftime("%Y-%m-%d"),
        "output_dir": output_dir,
        "bbox": bbox,
        **dl.stats,
    }
    _print_final_report(report)
    return report


def _print_final_report(r: dict) -> None:
    failed = r["failed"]
    sep = "=" * 70
    print(sep)
    print(f"MERRA-2 {r['product']} — {r['start']} → {r['end']}")
    print(sep)
    print(f"  Output dir       : {r['output_dir']}")
    print(f"  BBox             : {r['bbox']}")
    print(f"  Downloaded       : {r['downloaded']}")
    print(f"  Subset written   : {r['subset_written']}")
    print(f"  Skipped (cached) : {r['skipped']}")
    print(f"  Failed           : {len(failed)}")
    if failed:
        for err in failed[:20]:
            print(f"    x {err['url']}")
            print(f"      -> {err['reason']}")
        if len(failed) > 20:
            print(f"    ... and {len(failed) - 20} more")
    print(sep)


if __name__ == "__main__":
    run_merra2_m2t1nxaer(
        start="2022-09-01",
        end="2026-04-01",
        output_dir="/home/slow_data/Air_Quality/MERRA2/M2T1NXAER",
        bbox=VIETNAM_BBOX,
        keep_global=False,
    )
