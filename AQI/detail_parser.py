"""
Shared parser for tedp.vn /api/data_hour/{recordId} detail responses.
Used by both fetch_full_details.py and rebuild_historical_v2.py.
Vietnamese keys are prepended; English aliases kept for vendor variations.
"""

# CSV column -> ordered candidate API keys. First hit wins.
FIELD_MAP = {
    # meteorology
    "Temperature":    ["Nhiệt độ", "Temperature", "Temp"],
    "Humidity":       ["Độ ẩm", "Humidity", "RH", "Humid"],
    "Pressure":       ["Áp suất khí quyển", "Pressure", "Barometer"],
    "Wind Speed":     ["Tốc độ gió", "Wind Speed", "WindSpd", "Wind Spd (sai)"],
    "Wind Direction": ["Hướng gió", "Wind Direction", "WinDir"],
    "Radiation":      ["Bức xạ", "Radiation"],
    "Rainfall":       ["Lượng mưa", "Rainfall", "Rain"],
    "InnerTemp":      ["InnerTemp"],
    # pollutants
    "PM1":            ["PM-1", "PM1"],
    "PM2.5":          ["PM-2-5", "PM2.5"],
    "PM10":           ["PM-10", "PM10"],
    "CO":             ["CO"],
    "NO":             ["NO"],
    "NO2":            ["NO2"],
    "NOx":            ["NOx"],
    "O3":             ["O3"],
    "SO2":            ["SO2"],
    "H2S":            ["H2S"],
    "TSP":            ["TSP"],
    "Hg":             ["Hg"],
    "TVOC":           ["TVOC"],
    # particle size bins (LCS)
    "PX1-1":          ["PX1-1"],
    "PX1-2":          ["PX1-2"],
    "PX2-1":          ["PX2-1"],
    "PX2-2":          ["PX2-2"],
    "PX3-1":          ["PX3-1"],
    "PX3-2":          ["PX3-2"],
    # VOCs
    "Benzen":         ["Benzen", "Benzene"],
    "Toluen":         ["Toluen", "Toluene"],
    "Styrene":        ["Styrene"],
    "EthylBenzen":    ["EthylBenzen", "EthylBenzene"],
    "o-Xylene":       ["o-Xylene"],
    "m-p-Xylene":     ["m-p-Xylene", "m,p-Xylene"],
    "Butadiene":      ["Butadiene"],
    "Acrylonitrile":  ["Acrylonitrile"],
    "Vinyl chloride": ["Vinyl chloride", "VinylChloride"],
    "Tetrachloroethene": ["Tetrachloroethene"],
}

OUTPUT_COLUMNS = [
    "Timestamp", "Record_ID",
    # pollutants
    "PM1", "PM2.5", "PM10", "CO", "NO", "NO2", "NOx", "O3", "SO2", "H2S",
    "TSP", "Hg", "TVOC",
    # particle size bins (LCS-specific)
    "PX1-1", "PX1-2", "PX2-1", "PX2-2", "PX3-1", "PX3-2",
    # met
    "Temperature", "Humidity", "Pressure", "Wind Speed", "Wind Direction",
    "Radiation", "Rainfall", "InnerTemp",
    # VOCs
    "Benzen", "Toluen", "Styrene", "EthylBenzen", "o-Xylene", "m-p-Xylene",
    "Butadiene", "Acrylonitrile", "Vinyl chloride", "Tetrachloroethene",
    "Detailed_Status",
]

def parse_detail(json_payload):
    """
    Parse tedp.vn /api/data_hour/{id} response.
    Returns (row_dict, unknown_keys_list).
    unknown_keys_list is every data key not covered by FIELD_MAP — useful
    to write to a sidecar log for schema discovery.
    """
    data = json_payload.get("data", {}) if isinstance(json_payload, dict) else {}
    row = {}
    row["Timestamp"] = json_payload.get("getTime")
    row["Record_ID"] = ""
    link = json_payload.get("_links", {}).get("self", {}).get("href", "")
    if link:
        row["Record_ID"] = link.split("/")[-1]

    mapped = set()
    for col, candidates in FIELD_MAP.items():
        v = None
        for k in candidates:
            if k in data and data[k] is not None:
                v = data[k]
                mapped.add(k)
                break
        row[col] = v if v is not None else ""

    unknown = [k for k in data.keys() if k not in mapped]
    return row, unknown
