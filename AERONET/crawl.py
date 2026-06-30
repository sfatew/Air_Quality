"""
AERONET AOD Downloader
========================
Download AOD data from AERONET V3 web service.
No authentication required.

Data levels:
  - Level 1.0: unscreened
  - Level 1.5: cloud-screened
  - Level 2.0: cloud-screened + quality-assured (recommended)

Output structure:
  <output_dir>/10/<station_name>.csv   (all points)
  <output_dir>/20/<station_name>.csv   (daily average)
"""

import logging
from datetime import datetime, timedelta
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
SITES_CSV = Path("/home/work1/projects/Air_Quality/Masterdata/AERONET_sites.csv")


def load_aeronet_stations(csv_path: Path = SITES_CSV) -> dict:
    df = pd.read_csv(csv_path)
    stations = {}
    for _, row in df.iterrows():
        stations[row["stationName"]] = {
            "lat": row["latitude"],
            "lon": row["longitude"],
            "city": row["Location"],
        }
    return stations


AERONET_STATIONS = load_aeronet_stations()


def _parse_aeronet_response(text: str, site: str) -> pd.DataFrame:
    if "No data found" in text or len(text.strip()) < 100:
        return pd.DataFrame()

    lines = text.strip().split("\n")
    header_idx = None
    for i, line in enumerate(lines):
        if "Date(dd:mm:yyyy)" in line:
            header_idx = i
            break

    if header_idx is None:
        logger.error("Cannot find header in AERONET response for %s", site)
        return pd.DataFrame()

    csv_text = "\n".join(lines[header_idx:])
    df = pd.read_csv(StringIO(csv_text), sep=",", na_values=["-999.", "-999", "N/A"])
    df.columns = df.columns.str.strip()
    df = df.copy()

    station_info = AERONET_STATIONS.get(site, {})
    df["datetime"] = pd.to_datetime(
        df["Date(dd:mm:yyyy)"] + " " + df["Time(hh:mm:ss)"],
        format="%d:%m:%Y %H:%M:%S", errors="coerce",
    ) + pd.Timedelta(hours=7)
    df["site"] = site
    df["site_lat"] = station_info.get("lat")
    df["site_lon"] = station_info.get("lon")
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _get_existing_max_date(filepath: Path) -> datetime | None:
    if not filepath.exists():
        return None
    try:
        existing = pd.read_csv(filepath, usecols=["datetime"], parse_dates=["datetime"])
        if existing.empty:
            return None
        return existing["datetime"].max().to_pydatetime()
    except Exception:
        return None


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
    If the destination CSV already exists, only fetches data newer than
    the latest record and upserts into the existing file.
    """
    end_dt = datetime.strptime(end, "%Y-%m-%d")

    # Determine effective start date based on existing data
    if output_dir:
        out_path = Path(output_dir) / avg
        filepath = out_path / f"{site}.csv"
        existing_max = _get_existing_max_date(filepath)
        if existing_max:
            effective_start = (existing_max + timedelta(days=1)).strftime("%Y-%m-%d")
            if datetime.strptime(effective_start, "%Y-%m-%d") > end_dt:
                logger.info("SKIP %s (avg=%s): already up-to-date through %s", site, avg, existing_max.strftime("%Y-%m-%d"))
                return pd.read_csv(filepath, parse_dates=["datetime"])
            logger.info("UPSERT %s (avg=%s): existing data through %s, fetching from %s", site, avg, existing_max.strftime("%Y-%m-%d"), effective_start)
            start = effective_start

    start_dt = datetime.strptime(start, "%Y-%m-%d")

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
        logger.error("Download failed for %s: %s", site, e)
        return pd.DataFrame()

    new_df = _parse_aeronet_response(resp.text, site)

    if new_df.empty:
        logger.warning("No new AERONET data for %s in %s → %s", site, start, end)
        if output_dir and filepath.exists():
            return pd.read_csv(filepath, parse_dates=["datetime"])
        return pd.DataFrame()

    logger.info("Got %d new records for %s", len(new_df), site)

    # Upsert: merge with existing data
    if output_dir:
        out_path.mkdir(parents=True, exist_ok=True)
        if filepath.exists():
            existing_df = pd.read_csv(filepath, parse_dates=["datetime"])
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined.drop_duplicates(subset=["datetime"], keep="last", inplace=True)
            combined.sort_values("datetime", inplace=True)
            combined.reset_index(drop=True, inplace=True)
            combined.to_csv(filepath, index=False)
            logger.info("Upserted %s: %d total records", filepath, len(combined))
            return combined
        else:
            new_df.to_csv(filepath, index=False)
            logger.info("Saved: %s (%d records)", filepath, len(new_df))

    return new_df


def download_all_vietnam(
    start: str = "2022-01-01",
    end: str = "2026-12-31",
    level: str = "15",
    avg_types: list = None,
    output_dir: str = "/home/slow_data/Air_Quality/AERONET",
    stations: list = None,
) -> dict:
    """
    Download AERONET data for all Vietnam stations.
    Downloads both all-points (avg=10) and daily-average (avg=20) by default.

    Returns
    -------
    dict : {(site_name, avg_type): DataFrame}
    """
    if stations is None:
        stations = list(AERONET_STATIONS.keys())
    if avg_types is None:
        avg_types = ["10", "20"]

    results = {}
    for site in stations:
        for avg in avg_types:
            df = download_aeronet(
                site=site, start=start, end=end,
                level=level, avg=avg, output_dir=output_dir,
            )
            results[(site, avg)] = df

    logger.info("=" * 50)
    logger.info("AERONET DOWNLOAD SUMMARY")
    logger.info("=" * 50)
    for (site, avg), df in results.items():
        avg_label = "all-points" if avg == "10" else "daily-avg"
        if df.empty:
            logger.info("  %s [%s]: No data", site, avg_label)
        else:
            logger.info("  %s [%s]: %d records (%s → %s)",
                        site, avg_label, len(df),
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
    # Specify the exact site names you want to download (e.g., ["NGHIA_DO", "Son_La"])
    # Set to None or an empty list [] to default to ALL sites in SITES_CSV.
    TARGET_SITES = ["Bac_Lieu"] 

    # Determine which stations to pass to the downloader
    sites_to_download = TARGET_SITES if TARGET_SITES else list(AERONET_STATIONS.keys())

    results = download_all_vietnam(
        start="2025-08-22",
        end="2026-05-01",
        level="15",
        output_dir="/home/slow_data/Air_Quality/AERONET/raw",
        stations=sites_to_download,  # Pass the target sites here
    )
    print("+++++++++ END +++++++++")

    with open("log.txt", "w") as f:
        f.write(str(results))