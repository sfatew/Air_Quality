import os
import glob
import pandas as pd

# 1. Define the folder path and output file name
folder_path = '/home/slow_data/Air_Quality/weather'
output_file = 'era5_rh_pblh_merged.csv'

# 2. Find all CSV files in the folder
csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

# List to store each individual dataframe
df_list = []

print(f"Found {len(csv_files)} CSV files. Starting merge process...")

for file in csv_files:
    # Extract the base file name (e.g., 'weather_Bắc Giang... .csv')
    base_name = os.path.basename(file)
    
    # Remove the .csv extension and the 'weather_' prefix to get the station name
    # Using replace(..., 1) ensures we only remove the first occurrence of "weather_"
    name_without_ext = os.path.splitext(base_name)[0]
    station_name = name_without_ext.replace('weather_', '', 1)
    
    # Read the CSV file
    try:
        df = pd.read_csv(file)
        
        # Add the station_name column
        df['station_name'] = station_name
        
        # Rename 'Humidity' to 'RH'
        # Using inplace=True to modify the existing dataframe
        df.rename(columns={'Humidity': 'RH'}, inplace=True)
        
        # Append to our list
        df_list.append(df)
        
    except Exception as e:
        print(f"Error reading {file}: {e}")

# 3. Concatenate all dataframes into one
if df_list:
    merged_df = pd.concat(df_list, ignore_index=True)
    
    # Optional: Reorder columns to put station_name first or next to Timestamp
    # Let's put station_name and Timestamp at the front
    cols = merged_df.columns.tolist()
    # Move station_name to the front if you prefer
    cols.insert(0, cols.pop(cols.index('station_name')))
    merged_df = merged_df[cols]
    
    # 4. Save the merged dataframe to a new CSV file
    merged_df.to_csv(output_file, index=False)
    print(f"Merge complete! Saved to {output_file}")
else:
    print("No data was merged. Please check if the folder path is correct and contains CSV files.")