"""
AERONET AOD Processor
=====================
Build processed_dataset from raw AERONET all-points (avg=10) CSVs.

Output columns:
    datetime, station_name, aod_500, aod_550, resolution, ae

aod_550 is interpolated from aod_500 using the Angstrom Exponent:
    aod_550 = aod_500 * (550 / 500) ** (-ae)

AE preference (first non-null wins per row):
    1. 440-675_Angstrom_Exponent  (tightest bracket around 500-550 nm)
    2. 500-870_Angstrom_Exponent  (fallback)
"""

import logging
from pathlib import Path

import pandas as pd
import numpy as np


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

RAW_DIR = Path("/home/slow_data/Air_Quality/AERONET/raw")
PROCESS_DIR = Path("/home/slow_data/Air_Quality/AERONET/process")

AE_PRIMARY = "440-675_Angstrom_Exponent"
AE_FALLBACK = "500-870_Angstrom_Exponent"
AOD_500_COL = "AOD_500nm"


def process_station(csv_path: Path, resolution: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["datetime"])
    station = csv_path.stem

    if AOD_500_COL not in df.columns:
        logger.warning("No %s in %s — skipping", AOD_500_COL, csv_path)
        return pd.DataFrame()

    aod_500 = pd.to_numeric(df[AOD_500_COL], errors="coerce")
    ae_primary = pd.to_numeric(df.get(AE_PRIMARY), errors="coerce")
    ae_fallback = pd.to_numeric(df.get(AE_FALLBACK), errors="coerce")

    ae = ae_primary.combine_first(ae_fallback)

    ae_source = np.where(
        ae_primary.notna(), AE_PRIMARY,
        np.where(ae_fallback.notna(), AE_FALLBACK, None),
    )

    aod_550 = aod_500 * (550.0 / 500.0) ** (-ae)

    out = pd.DataFrame({
        "datetime": df["datetime"],
        "station_name": station,
        "aod_500": aod_500,
        "aod_550": aod_550,
        "resolution": resolution,
        "ae": ae,
        "ae_source": ae_source,
    })

    out = out.dropna(subset=["aod_500"]).reset_index(drop=True)
    logger.info(
        "%s: %d rows (aod_500), %d also have aod_550",
        station, len(out), int(out["aod_550"].notna().sum()),
    )
    return out


def build_processed_dataset(
    raw_dir: Path = RAW_DIR,
    out_dir: Path = PROCESS_DIR,
    avg: str = "10",
) -> pd.DataFrame:
    resolution = {"10": "all_points", "20": "daily_avg"}.get(avg, avg)
    src_dir = raw_dir / avg

    frames = []
    for csv_path in sorted(src_dir.glob("*.csv")):
        frames.append(process_station(csv_path, resolution))

    if not frames:
        logger.warning("No CSVs found in %s", src_dir)
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined.sort_values(["station_name", "datetime"], inplace=True)
    combined.reset_index(drop=True, inplace=True)

    df_500 = combined.drop(columns=["aod_550"]).dropna(subset=["aod_500"]).reset_index(drop=True)
    df_550 = combined.drop(columns=["aod_500"]).dropna(subset=["aod_550"]).reset_index(drop=True)

    dir_500 = out_dir / "aod_500" / avg
    dir_550 = out_dir / "aod_550" / avg
    dir_500.mkdir(parents=True, exist_ok=True)
    dir_550.mkdir(parents=True, exist_ok=True)

    for station, g in df_500.groupby("station_name", sort=False):
        path = dir_500 / f"{station}.csv"
        g.drop(columns=["station_name"]).to_csv(path, index=False)
        logger.info("Saved %d rows → %s", len(g), path)

    for station, g in df_550.groupby("station_name", sort=False):
        path = dir_550 / f"{station}.csv"
        g.drop(columns=["station_name"]).to_csv(path, index=False)
        logger.info("Saved %d rows → %s", len(g), path)

    return combined


if __name__ == "__main__":
    df = build_processed_dataset(avg="20")
    print("Stations:", df["station_name"].unique())
    print("AE sources used:", df["ae_source"].value_counts().to_dict())
