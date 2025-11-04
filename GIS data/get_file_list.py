import sys
import subprocess
import os
from config.config import gis_config


server = gis_config.SERVER
download_dir = r'E:/Air Quality/GIS'
user = gis_config.GIS_USERNAME
password = gis_config.GIS_PASSWORD

def usage():
    print()
    print('Download imerg files for the given date')
    print()
    print('Usage: getImerg DATE')
    print('DATE - Format is YYY-MM-DD')
    print()

def main(argv):
    # make sure the user provided a date
    if len(argv) != 2:
        usage()
        sys.exit(1)

    date_str = argv[1]
    
    # Robust date validation
    try:
        from datetime import datetime
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
    except ValueError:
        print("Error: Invalid date format. Please use YYYY-MM-DD.")
        usage()
        sys.exit(1)

    # loop through the file list and get each file
    file_list = get_file_list(year, month, day)
    for filename in file_list:
        get_file(filename)

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
