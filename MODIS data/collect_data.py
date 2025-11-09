import os
import sys
# Get the absolute path of the current script's directory
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory's path
parent_dir = os.path.join(current_script_dir, os.pardir)
# Add the parent directory to sys.path
sys.path.append(parent_dir)
import subprocess
import re
from datetime import datetime, timedelta
import time
from config.config import 


SERVER = 
DOWNLOAD_DIR = r''
USER = 
PASSWORD = 

START_DATE_STR = "2023-03-16"
print(f"📅 Using date: {START_DATE_STR}")

def download_for_date():
    
    # Get the list of files (basenames)
    file_list = get_file_list(ftps, remote_path)
    
    if not file_list:
        print("ℹ️ No matching HHR .zip files found.")
        return False

    local_path = os.path.join(DOWNLOAD_DIR, year, month, day)
    os.makedirs(local_path, exist_ok=True)

    # Loop through and download each file
    for filename in file_list:
        get_file(ftps, filename, local_path)
    
    return True

def get_file_list(year, month, day, data = 'imerg'):
    ''' Get the file listing for the given year/month/day
    using curl.
    Return list of files (could be empty).
    '''

    url = server + '/gpmdata/' + '/'.join([year, month, day]) + f'/{data}/'
    args = [
        'curl', 
        '-n', 
        '-u', f'{user}:{password}', # Pass credentials directly as one argument
        url
    ]
    # print(args)
    process = subprocess.Popen(args,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    stdout = process.communicate()[0].decode()
    # print(stdout)
    if stdout[0] == '<':
        print (f'No {data} files for the given date')
        return []
    file_list = stdout.split()
    return file_list

def get_file(filename):
    ''' Get the given file from arthurhouhttps using curl. '''
    url = server + filename
    output_path = os.path.join(download_dir, os.path.basename(filename))
    args = [
        'curl', 
        '-n', 
        url, 
        '-o', 
        output_path  # No quotes needed here, subprocess handles it
    ]
    process = subprocess.Popen(args,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

    process.wait() # wait so this program doesn't end
                    # before getting all files

if __name__ == '__main__':
    main(sys.argv)