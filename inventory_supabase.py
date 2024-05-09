import os
import json
import time
from dotenv import load_dotenv
from supabase import create_client, Client
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileChangeHandler(FileSystemEventHandler):
    print('Watching for file changes..')
    def __init__(self, supabase: Client):
        self.supabase = supabase

    def on_modified(self, event, OUTPUT_FILE_NAME, PARSED_FILE_LOCATION):
        print('File modified at: ', event.src_path)
        if event.src_path == PARSED_FILE_LOCATION + OUTPUT_FILE_NAME + '.json':
            self.update_database(event.src_path)

    def update_database(self, file_path):
        print('Updating Supabase...')
        start_time = time.time()
        updated_records = 0
        new_records = 0

        with open(file_path, 'r') as f:
            data = json.load(f)
        
        for item in data:
            # Replace empty strings in numeric fields with None
            for field in ['sug_retail_price', 'wholesale_price', 'sale_price', 'gp', 'excise']:  # Replace with your actual field names
                if item[field] == '':
                    item[field] = None

            try:
                # Check if the item already exists in the database
                existing_item = self.supabase.table('wb_inventory').select('inventory_id').eq('inventory_id', item['inventory_id']).limit(1).execute()
                if existing_item.data and len(existing_item.data) > 0:
                    # Update the existing item
                    self.supabase.table('wb_inventory').update(item).eq('inventory_id', item['inventory_id']).execute()
                    updated_records += 1
                    print(f"Data updated successfully for inventory_id: {item['inventory_id']}")
                else:
                    # Insert the new item
                    self.supabase.table('wb_inventory').insert(item).execute()
                    new_records += 1
                    print(f"New item inserted successfully with inventory_id: {item['inventory_id']}")
            except Exception as e:
                print(f"Error updating data: {str(e)}")

        end_time = time.time()
        total_time = end_time - start_time
        print(f"All data processed. Time taken: {total_time} seconds.")
        print(f"Updated records: {updated_records}")
        print(f"New records created: {new_records}")
            

def main():
    load_dotenv()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUBABASE_KEY = os.getenv("SUPABASE_KEY")
    supabase = create_client(SUPABASE_URL, SUBABASE_KEY)

    OUTPUT_FILE_NAME = os.getenv("OUTPUT_FILE_NAME")
    PARSED_FILE_LOCATION = os.getenv("PARSED_FILE_LOCATION")
    event_handler = FileChangeHandler(supabase)
    observer = Observer()
    observer.schedule(event_handler, PARSED_FILE_LOCATION, recursive=False)
    observer.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
