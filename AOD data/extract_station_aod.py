import sys
import os
import pandas as pd
import rasterio
import numpy as np

if len(sys.argv) < 2:
    print("⚠️ Thiếu đường dẫn file GeoTIFF")
    sys.exit(1)

aod_file = sys.argv[1]
stations_file = "/home/work1/projects/Air_Quality/AOD data/vietnam_meteostat_stations.csv"
OUTPUT_DIR = "/home/slow_data/Air_Quality/AOD/station_aod"

# Các ngưỡng uncertainty và tên file tương ứng
uncertainty_thresholds = [0.5, 0.7, 0.8, 1, 1.2, 1.5]
output_files = [OUTPUT_DIR + f"/all_station{str(threshold).replace('.', '')}.csv" for threshold in uncertainty_thresholds]

# Lấy timestamp từ tên file
filename = os.path.basename(aod_file)
parts = filename.split("_")
timestamp = parts[4] + "_" + parts[5]
aot_col_name = f"AOT_{timestamp}"

# Đọc danh sách trạm
stations = pd.read_csv(stations_file)
stations = stations.rename(columns={"name": "station_name"})

if "station_id" not in stations.columns:
    stations["station_id"] = range(len(stations))

# Đọc ảnh AOT và uncertainty
with rasterio.open(aod_file) as src:
    # Đọc toàn bộ band 1 và band 2 trước để tăng tốc
    aot_band = src.read(1)
    uncertainty_band = src.read(2)
    
    # Tạo dict để lưu giá trị cho từng ngưỡng
    aot_values_dict = {threshold: [] for threshold in uncertainty_thresholds}
    
    for _, row in stations.iterrows():
        lon, lat = row["longitude"], row["latitude"]
        try:
            rowcol = src.index(lon, lat)
            aot_value = aot_band[rowcol[0], rowcol[1]]
            uncertainty_value = uncertainty_band[rowcol[0], rowcol[1]]
            
            for threshold in uncertainty_thresholds:
                if np.isnan(aot_value) or np.isnan(uncertainty_value) or uncertainty_value >= threshold:
                    aot_values_dict[threshold].append(None)
                else:
                    aot_values_dict[threshold].append(aot_value)
        except Exception as e:
            for threshold in uncertainty_thresholds:
                aot_values_dict[threshold].append(None)

# Lưu ra các file CSV tương ứng với từng ngưỡng uncertainty
for threshold, output_csv in zip(uncertainty_thresholds, output_files):
    stations_copy = stations.copy()
    stations_copy[aot_col_name] = aot_values_dict[threshold]
    
    # Ghi hoặc cập nhật file tổng
    if os.path.exists(output_csv):
        df_old = pd.read_csv(output_csv)
        df_merged = pd.merge(df_old, 
                            stations_copy[["station_id", aot_col_name]], 
                            on="station_id", 
                            how="left")
    else:
        df_merged = stations_copy[["station_id", "station_name", "latitude", "longitude", 
                                 aot_col_name]]
    df_merged.to_csv(output_csv, index=False)
print(f"✅ Hoàn tất xử lý ")