import os
import time
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

url=os.getenv("JSON_FILE_URL")

def update_database(url):
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    supabase = create_client(supabase_url, supabase_key)
    print('Updating Supabase...')
    start_time = time.time()
    updated_records = 0
    new_records = 0

    response = requests.get(url)
    data = response.json()
    
    for item in data:
        # Replace empty strings in numeric fields with None
        for field in ['sug_retail_price', 'wholesale_price', 'sale_price', 'gp', 'excise']:  # Replace with your actual field names
            if item[field] == '':
                item[field] = None

        try:
            # Check if the item already exists in the database
            existing_item = supabase.table('wb_inventory').select('inventory_id').eq('inventory_id', item['inventory_id']).limit(1).execute()
            if existing_item.data and len(existing_item.data) > 0:
                # Update the existing item
                supabase.table('wb_inventory').update(item).eq('inventory_id', item['inventory_id']).execute()
                updated_records += 1
                print(f"Data updated successfully for inventory id: {item['inventory_id']}")
            else:
                # Insert the new item
                supabase.table('wb_inventory').insert(item).execute()
                new_records += 1
                print(f"New item inserted successfully with inventory_id: {item['inventory_id']}")
        except Exception as e:
            print(f"Error updating data: {str(e)}")

    end_time = time.time()
    total_time = end_time - start_time
    print(f"All data processed. Time taken: {total_time} seconds.")
    print(f"Updated records: {updated_records}")
    print(f"New records created: {new_records}")

update_database(url)