"""
AERONET AOD Downloader
========================
Download AOD data from AERONET V3 web service.
No authentication required.

AERONET stations near Hanoi:
  - Nghia_Do (21.048°N, 105.800°E) — Hanoi city, closest to 3 monitoring stations
  - Bac_Giang (21.30°N, 106.20°E)
  - Son_La (21.33°N, 103.90°E)

Data levels:
  - Level 1.0: unscreened
  - Level 1.5: cloud-screened
  - Level 2.0: cloud-screened + quality-assured (recommended)

Usage:
    from aeronet_downloader import download_aeronet, download_all_vietnam
    
    # Single station
    df = download_aeronet("Nghia_Do", "2022-01-01", "2022-12-31")
    
    # All Vietnam stations
    dfs = download_all_vietnam("2022-01-01", "2022-12-31")
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from io import StringIO

import requests
import pandas as pd

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

AERONET_URL = "https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3"

# AERONET stations in Vietnam (and nearby)
AERONET_STATIONS = {
    "NGHIA_DO": {"lat": 21.048, "lon": 105.800, "city": "Hanoi"},
    "Bac_Giang": {"lat": 21.30, "lon": 106.20, "city": "Bac Giang"},
    "Son_La": {"lat": 21.33, "lon": 103.90, "city": "Son La"},
    "Bac_Lieu": {"lat": 9.28, "lon": 105.73, "city": "Bac Lieu"},
    "Nha_Trang": {"lat": 12.20, "lon": 109.21, "city": "Nha Trang"},
}

# Mapping to our monitoring stations
STATION_MAPPING = {
    "HN: 556 Nguyễn Văn Cừ": "NGHIA_DO",
    "HN: CV Nhân Chính": "NGHIA_DO",
    "HN: ĐHBK Giải Phóng": "NGHIA_DO",
}


def download_aeronet(
    site: str = "NGHIA_DO",
    start: str = "2022-01-01",
    end: str = "2022-12-31",
    level: str = "20",
    avg: str = "10",
    output_dir: str = None,
) -> pd.DataFrame:
    """
    Download AOD data from AERONET V3 web service.

    Parameters
    ----------
    site : str
        AERONET site name (e.g. 'NGHIA_DO', 'Bac_Giang', 'Son_La')
    start, end : str
        Date range 'YYYY-MM-DD'
    level : str
        '10' = Level 1.0 (unscreened)
        '15' = Level 1.5 (cloud-screened)
        '20' = Level 2.0 (cloud-screened + quality-assured)
    avg : str
        '10' = All points
        '20' = Daily average
    output_dir : str, optional
        Save CSV to this directory. If None, don't save.

    Returns
    -------
    pd.DataFrame
    """
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")

    params = {
        "site": site,
        "year": start_dt.year,
        "month": start_dt.month,
        "day": start_dt.day,
        "year2": end_dt.year,
        "month2": end_dt.month,
        "day2": end_dt.day,
        f"AOD{level}": 1,
        "AVG": avg,
        "if_no_html": 1,
    }

    level_names = {"10": "Level 1.0", "15": "Level 1.5", "20": "Level 2.0"}
    avg_names = {"10": "All points", "20": "Daily average"}

    logger.info(
        "AERONET download: %s | %s → %s | %s | %s",
        site, start, end, level_names.get(level, level), avg_names.get(avg, avg),
    )

    try:
        resp = requests.get(AERONET_URL, params=params, timeout=300, verify=False)
        resp.raise_for_status()
    except Exception as e:
        logger.error("Download failed: %s", e)
        return pd.DataFrame()

    text = resp.text

    # Check for no data
    if "No data found" in text or len(text.strip()) < 100:
        logger.warning("No AERONET data for %s in %s → %s", site, start, end)
        return pd.DataFrame()

    # Parse: skip header lines (varies, typically 6-7 lines before column names)
    lines = text.strip().split("\n")

    # Find the header line (contains "Date(dd:mm:yyyy)")
    header_idx = None
    for i, line in enumerate(lines):
        if "Date(dd:mm:yyyy)" in line:
            header_idx = i
            break

    if header_idx is None:
        logger.error("Cannot find header in AERONET response")
        return pd.DataFrame()

    # Parse CSV from header line onwards
    csv_text = "\n".join(lines[header_idx:])
    df = pd.read_csv(StringIO(csv_text), sep=",", na_values=["-999.", "-999", "N/A"])

    # Clean column names
    df.columns = df.columns.str.strip()

    # Parse datetime
    if "Date(dd:mm:yyyy)" in df.columns and "Time(hh:mm:ss)" in df.columns:
        df["datetime"] = pd.to_datetime(
            df["Date(dd:mm:yyyy)"] + " " + df["Time(hh:mm:ss)"],
            format="%d:%m:%Y %H:%M:%S",
            errors="coerce",
        )
    elif "Date(dd:mm:yyyy)" in df.columns:
        df["datetime"] = pd.to_datetime(
            df["Date(dd:mm:yyyy)"],
            format="%d:%m:%Y",
            errors="coerce",
        )

    # Add metadata
    df = df.assign(
        datetime=pd.to_datetime(
            df["Date(dd:mm:yyyy)"] + " " + df["Time(hh:mm:ss)"],
            format="%d:%m:%Y %H:%M:%S", errors="coerce",
        ),
        site=site,
        site_lat=AERONET_STATIONS[site]["lat"],
        site_lon=AERONET_STATIONS[site]["lon"],
    )

    # Sort
    if "datetime" in df.columns:
        df.sort_values("datetime", inplace=True)
        df.reset_index(drop=True, inplace=True)

    logger.info("Got %d records for %s", len(df), site)

    # Save
    if output_dir:
        out_path = Path(output_dir) / site
        out_path.mkdir(parents=True, exist_ok=True)
        filename = f"aeronet_{site}_L{level}_{start}_{end}.csv"
        filepath = out_path / filename
        df.to_csv(filepath, index=False)
        logger.info("Saved: %s", filepath)

    return df


def download_all_vietnam(
    start: str = "2022-01-01",
    end: str = "2022-12-31",
    level: str = "20",
    avg: str = "10",
    output_dir: str = "/home/slow_data/Air_Quality/AERONET",
    stations: list = None,
) -> dict:
    """
    Download AERONET data for all Vietnam stations.

    Parameters
    ----------
    stations : list, optional
        List of site names. Default: all Vietnam stations.

    Returns
    -------
    dict : {site_name: DataFrame}
    """
    if stations is None:
        stations = list(AERONET_STATIONS.keys())

    results = {}
    for site in stations:
        df = download_aeronet(
            site=site, start=start, end=end,
            level=level, avg=avg, output_dir=output_dir,
        )
        results[site] = df

    # Summary
    logger.info("=" * 50)
    logger.info("AERONET DOWNLOAD SUMMARY")
    logger.info("=" * 50)
    for site, df in results.items():
        if df.empty:
            logger.info("  %s: No data", site)
        else:
            logger.info("  %s: %d records (%s → %s)",
                        site, len(df),
                        df["datetime"].min().strftime("%Y-%m-%d") if "datetime" in df.columns else "?",
                        df["datetime"].max().strftime("%Y-%m-%d") if "datetime" in df.columns else "?")
    logger.info("=" * 50)

    return results


def get_aod_columns(df: pd.DataFrame) -> list:
    """Get AOD column names from AERONET DataFrame."""
    return [c for c in df.columns if c.startswith("AOD_") and "nm" in c.lower() or
            c.startswith("AOD_") and c[4:].replace(".", "").isdigit()]


def extract_aod_550(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract AOD at 500nm (AERONET standard) and interpolate to 550nm
    using Angstrom Exponent for comparison with VIIRS.

    VIIRS AOD is at 550nm. AERONET measures at 500nm.
    Conversion: AOD_550 = AOD_500 * (550/500)^(-AE)
    """
    df = df.copy()

    # AOD at 500nm
    aod_500_col = [c for c in df.columns if "AOD_500" in c]
    ae_col = [c for c in df.columns if "Angstrom" in c and "440-675" in c]

    if not aod_500_col:
        logger.warning("No AOD_500nm column found")
        return df

    aod_col = aod_500_col[0]
    df["AOD_500"] = pd.to_numeric(df[aod_col], errors="coerce")

    if ae_col:
        df["AE_440_675"] = pd.to_numeric(df[ae_col[0]], errors="coerce")
        # Interpolate to 550nm
        df["AOD_550_interp"] = df["AOD_500"] * (550.0 / 500.0) ** (-df["AE_440_675"])
    else:
        # Simple approximation without AE
        df["AOD_550_interp"] = df["AOD_500"] * 0.93  # rough factor

    return df


if __name__ == "__main__":
    # Download Nghia_Do (Hanoi) — closest to our 3 stations
    df = download_aeronet(
        site="NGHIA_DO",
        start="2022-01-01",
        end="2026-04-01",
        level="15",       # Level 1.5 (Level 2.0 không có data)
        output_dir="/home/slow_data/Air_Quality/AERONET",
    )
    print(len(df))