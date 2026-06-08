"""
Add a `region` column to envisoft_station_map.csv classifying each station
into one of 7 Vietnamese geographic regions:

    NW  - North West
    NE  - North East
    RRD - Red River Delta
    NCC - North Central Coast
    SCC - South Central Coast
    CH  - Central Highlands
    SE  - South East

Mekong River Delta provinces (Long An, Trà Vinh, Vĩnh Long, Sóc Trăng, ...)
are folded into SE because the user's scheme defines only 7 regions and the
southern climate (rainy/dry seasons) is shared with SE.

Usage:
    python add_region.py
"""

from pathlib import Path
import re
import unicodedata
import pandas as pd

CSV_PATH = Path(__file__).resolve().parents[1] / "envisoft_station_map.csv"


PROVINCE_TO_REGION = {
    # North West
    "lao cai":      "NW",
    "yen bai":      "NW",
    "dien bien":    "NW",
    "lai chau":     "NW",
    "son la":       "NW",
    "hoa binh":     "NW",

    # North East
    "ha giang":     "NE",
    "cao bang":     "NE",
    "bac kan":      "NE",
    "lang son":     "NE",
    "tuyen quang":  "NE",
    "thai nguyen":  "NE",
    "phu tho":      "NE",
    "bac giang":    "NE",
    "quang ninh":   "NE",

    # Red River Delta
    "ha noi":       "RRD",
    "hai phong":    "RRD",
    "hai duong":    "RRD",
    "hung yen":     "RRD",
    "vinh phuc":    "RRD",
    "bac ninh":     "RRD",
    "ha nam":       "RRD",
    "nam dinh":     "RRD",
    "thai binh":    "RRD",
    "ninh binh":    "RRD",

    # North Central Coast
    "thanh hoa":    "NCC",
    "nghe an":      "NCC",
    "ha tinh":      "NCC",
    "quang binh":   "NCC",
    "quang tri":    "NCC",
    "thua thien hue": "NCC",
    "hue":          "NCC",

    # South Central Coast
    "da nang":      "SCC",
    "quang nam":    "SCC",
    "quang ngai":   "SCC",
    "binh dinh":    "SCC",
    "phu yen":      "SCC",
    "khanh hoa":    "SCC",
    "ninh thuan":   "SCC",
    "binh thuan":   "SCC",

    # Central Highlands
    "kon tum":      "CH",
    "gia lai":      "CH",
    "dak lak":      "CH",
    "dak nong":     "CH",
    "lam dong":     "CH",

    # South East (incl. Mekong River Delta — folded into SE)
    "binh phuoc":   "SE",
    "tay ninh":     "SE",
    "binh duong":   "SE",
    "dong nai":     "SE",
    "ba ria - vung tau": "SE",
    "vung tau":     "SE",
    "hcm":          "SE",
    "tp.hcm":       "SE",
    "ho chi minh":  "SE",
    # Mekong delta provinces -> SE
    "long an":      "SE",
    "tien giang":   "SE",
    "ben tre":      "SE",
    "tra vinh":     "SE",
    "vinh long":    "SE",
    "dong thap":    "SE",
    "an giang":     "SE",
    "kien giang":   "SE",
    "can tho":      "SE",
    "hau giang":    "SE",
    "soc trang":    "SE",
    "bac lieu":     "SE",
    "ca mau":       "SE",
}

# Special station IDs that don't follow the "Province: ..." pattern.
SPECIAL_ID_TO_REGION = {
    "US_EMBASSY_HAN":   "RRD",
    "US_CONSULATE_HCM": "SE",
}


def _ascii_lower(text: str) -> str:
    """Strip Vietnamese diacritics and lowercase."""
    text = text.replace("Đ", "D").replace("đ", "d")
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def _extract_province(name: str) -> str:
    """Take the part before the first ':' as province name."""
    if ":" not in name:
        return ""
    return name.split(":", 1)[0].strip()


def classify(station_id: str, station_name: str) -> str:
    if station_id in SPECIAL_ID_TO_REGION:
        return SPECIAL_ID_TO_REGION[station_id]

    province_key = _ascii_lower(_extract_province(station_name))
    province_key = re.sub(r"\s+", " ", province_key)
    if province_key in PROVINCE_TO_REGION:
        return PROVINCE_TO_REGION[province_key]

    return "UNKNOWN"


def main() -> None:
    df = pd.read_csv(CSV_PATH)

    df["region"] = [
        classify(str(sid), str(sname))
        for sid, sname in zip(df["stationId"], df["stationName"])
    ]

    unknown = df[df["region"] == "UNKNOWN"]
    if not unknown.empty:
        print("Stations not classified:")
        print(unknown[["stationId", "stationName"]].to_string(index=False))

    df.to_csv(CSV_PATH, index=False)
    print(f"\nWrote {len(df)} rows to {CSV_PATH}")
    print("\nRegion counts:")
    print(df["region"].value_counts().to_string())


if __name__ == "__main__":
    main()