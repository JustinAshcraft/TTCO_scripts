import csv
import datetime
import time
import os
import json
import hashlib
import sys

from datetime import datetime
from dotenv import load_dotenv
import shutil
import pysftp
import pytz
load_dotenv()

# Change the current working directory to the directory where the script is located
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

# Load the .env file from the script's directory
load_dotenv(os.path.join(script_dir, '.env'))

# Define the path to the file you want to watch
WB_FILE_NAME = os.getenv('WB_FILE_NAME')
OUTPUT_FILE_NAME = os.getenv('OUTPUT_FILE_NAME')
WB_FILE_LOCATION = os.getenv('WB_FILE_LOCATION')
REMOTE_FTP_LOCATION = os.getenv('REMOTE_FTP_LOCATION')
PARSED_FILE_LOCATION = os.getenv('PARSED_FILE_LOCATION')

FTP_HOST = os.getenv('FTP_HOST')
FTP_USER = os.getenv('FTP_USER')
FTP_PASSWORD = os.getenv('FTP_PASSWORD')

file_to_watch = WB_FILE_LOCATION + WB_FILE_NAME
timestamp = time.strftime("%Y_%m_%d_%H_%M_%S")
back_up_copy = f'./backups/wb_{timestamp}.txt'

print(f'Watching for WB file changes...')

# Output file paths
csv_file = PARSED_FILE_LOCATION + OUTPUT_FILE_NAME + '.txt'
json_file = PARSED_FILE_LOCATION + OUTPUT_FILE_NAME + '.json' # Define the path to the JSON file
# file_to_watch = f'./backups/wb_{timestamp}.txt'

# Get the initial timestamp of the file
if os.path.isfile(file_to_watch):
    initial_timestamp = os.path.getmtime(file_to_watch)
else:
    print(f'File {file_to_watch} does not exist.')

# Define the widths of each column located in WB print file
column_widths = [11, 6, 8, 51, 6, 9, 7, 7, 7, 7, 7]
def convert_encoding(input_file, output_file):
    with open(input_file, 'r', encoding='utf-16') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        contents = infile.read()
        outfile.write(contents)

def parse_row(row):
    parsed_row = []
    start = 0
    for width in column_widths:
        parsed_row.append(row[start:start+width].strip())
        start += width
    return parsed_row

def generate_inventory_id(tire_size, mfg, part, description):
    # Concatenate the three values
    concat_values = tire_size + mfg + part + description

    # Generate a MD5 hash
    hash_object = hashlib.md5(concat_values.encode())
    #hex_dig = hash_object.hexdigest()  # Get the full hex hash
    hex_dig = hash_object.hexdigest()[:8]  # Get the first 8 characters of the hash
    return hex_dig

def save_to_json(data, json_file):
    with open(json_file, 'w') as outfile:
        json.dump(data, outfile)

def txt_to_csv_and_json(file_to_watch, csv_file, json_file):
    lines = file_to_watch.split('\n')

    # Filter out header and footer lines
    data_lines = [line.strip() for line in lines if line.strip() and not line.startswith((' ', '\x0c'))]

    data_for_json = []
    # Write data to CSV file
    with open(csv_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        headers = ['tire_size', 'mfg', 'part_num', 'description', 'stock', 'sug_retail_price', 'sale_price', 'wholesale_price', 'gp', 'excise', 'memo', 'inventory_id', 'stock_on_hand']
        writer.writerow(headers)
        for line in data_lines:
            parsed_row = parse_row(line)
            if parsed_row[0] != 'TIRE SIZE'  and 'TUCKER TIRE CO' not in parsed_row[3]:  
                # Add to above line to skip rows that start with 'TIRE' or have 'ZZZ' or 'INF' in the second column [[---->   and parsed_row[1] not in ['ZZZ', 'INF']   <----]]
                # Also skip the row where the 4th column contains 'TUCKER TIRE CO'
                inventory_id = generate_inventory_id(parsed_row[0], parsed_row[1], parsed_row[2], parsed_row[3])
                parsed_row.insert(11, inventory_id)
                parsed_row.insert(12, parsed_row[4])  # Add stock_on_hand value same as stock column value
                writer.writerow(parsed_row)
                data_for_json.append(dict(zip(headers, parsed_row)))

    # Save data to JSON file
    save_to_json(data_for_json, json_file)


def sftp_upload_files(host, username, password):
        pacific = pytz.timezone('US/Pacific')
        current_time = datetime.now(pacific).strftime('%m-%d-%y %H:%M:%S')
        event_message = f'{current_time} PST => File modified. Uploading to SFTP server...'
        with open('./logs/log.txt', 'a') as f:
            f.write(event_message + '\n')
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None   
        try:
            with pysftp.Connection(host, username=username, password=password, cnopts=cnopts) as sftp:
                with sftp.cd('wb-output'):  # change to the remote directory
                    sftp.put(csv_file)  # upload csv file to remote directory
                    sftp.put(json_file)  # upload json file to remote directory
                    success_message = f'{current_time} PST => Files successfully uploaded!'
                    print(success_message)
                    with open('./logs/log.txt', 'a') as f:
                        f.write(success_message + '\n')
        except Exception as e:
            error_message = f'{current_time} PST => Error occurred during SFTP process: {str(e)}'
            print(error_message)
            with open('./logs/log.txt', 'a') as f:
             f.write(error_message + '\n')


while True:
    try:
        # Get the current timestamp of the file
        current_timestamp = os.path.getmtime(file_to_watch)

        # If the timestamp has changed, the file has been modified
        if current_timestamp != initial_timestamp:
            print(f'{file_to_watch} has been modified. Re-parsing the file...')
            time.sleep(2)
            print(f'Convert encoding to UTF-8...')
            # convert_encoding(wb_file, back_up_copy)
            with open(file_to_watch, 'r', encoding='utf-16') as infile:
                contents = infile.read()
            time.sleep(2)

            # Convert txt to csv and json
            txt_to_csv_and_json(contents, csv_file, json_file)
            print(f'Parsing complete. CSV file saved to {csv_file}. JSON file saved to {json_file}.')

            # Create a backup of the file
            shutil.copy(csv_file, f'./backups/wb_{timestamp}.txt')
            print(f'Backup created.')

            #Upload to SFTP server
            # Call the function to upload the files
            print(f'Uploading files to SFTP server...')
            sftp_upload_files(FTP_HOST, FTP_USER, FTP_PASSWORD)
            time.sleep(2)

            
            initial_timestamp = current_timestamp
            print(f'Watching for WB file changes...')  
        # Wait for a while before checking the file again
        time.sleep(1)

    except KeyboardInterrupt:
        print('Stopped watching the file.')
        break
    except FileNotFoundError:
        print('File does not exist.')
        break
    except Exception as e:
        print(f'An error occurred: {e}')
        break