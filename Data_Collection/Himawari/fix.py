import os
import glob
import pandas as pd

def process_csv_files(directory, cutoff_date_str='2022-09-01'):
    """
    Goes through all CSVs in a directory and removes rows where 
    the 'timestamp' column is before the cutoff_date_str.
    """
    # Find all CSV files in the directory
    search_pattern = os.path.join(directory, '*.csv')
    csv_files = glob.glob(search_pattern)
    
    if not csv_files:
        print(f"No CSV files found in {directory}")
        return

    print(f"Found {len(csv_files)} CSV files. Starting processing...")
    
    # Convert cutoff string to a pandas datetime object
    cutoff_date = pd.to_datetime(cutoff_date_str)
    
    files_modified = 0
    
    for file_path in csv_files:
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Check if the 'timestamp' column exists
            if 'timestamp' not in df.columns:
                print(f"Skipping {os.path.basename(file_path)}: 'timestamp' column not found.")
                continue
                
            original_row_count = len(df)
            
            # Convert the 'timestamp' column to datetime objects
            # coerce errors will turn unparseable dates into NaT (Not a Time)
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            
            # Filter the dataframe: keep rows where timestamp is >= cutoff_date
            # We also keep rows where timestamp might be NaT just in case, 
            # or you can drop them. Here we strictly check >= cutoff_date.
            filtered_df = df[df['timestamp'] >= cutoff_date]
            
            new_row_count = len(filtered_df)
            
            # If rows were dropped, save the file back
            if new_row_count < original_row_count:
                # Save the filtered dataframe back to the same CSV file, overwriting it
                filtered_df.to_csv(file_path, index=False)
                rows_deleted = original_row_count - new_row_count
                print(f"Processed {os.path.basename(file_path)}: Deleted {rows_deleted} rows.")
                files_modified += 1
            else:
                print(f"Processed {os.path.basename(file_path)}: No rows needed deletion.")
                
        except Exception as e:
            print(f"Error processing {os.path.basename(file_path)}: {e}")

    print(f"\nProcessing complete! Modified {files_modified} out of {len(csv_files)} files.")

if __name__ == "__main__":
    # Target directory
    target_dir = '/home/slow_data/Air_Quality/Himawari/station_aod_v3/L3'
    
    # Execute the function
    process_csv_files(target_dir)