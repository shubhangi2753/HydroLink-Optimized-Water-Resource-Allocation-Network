from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import math

client = MongoClient("mongodb+srv://deepak297512:hans1725@cluster0.51vc1pc.mongodb.net/")  
db = client['AquaLink']
reservoir_collection = db['reservoir']
processed_reservoir_collection = db['processed_reservoir']  

reservoir_data_cursor = reservoir_collection.find()
reservoir_data_list = list(reservoir_data_cursor)

preprocessed_reservoir_data = {}
max_entries = []
min_entries = []

for data in reservoir_data_list:
    district_name = data['district_name']
    monthly_data = data['monthly_data']
    monthly_storage = []
    
    for month, entries in monthly_data.items():
        max_entry = -1
        min_entry = math.inf
        total_storage = 0
        count = 0
        
        for entry in entries['data']:
            entry['district_name'] = district_name
            entry['month'] = month
            entry_date = pd.to_datetime(entry['date'])
            entry['date'] = entry_date.replace(day=1)  # Set the date to the 1st of the month
            total_storage += entry['storage']
            count += 1
            max_entry = max(max_entry, entry['storage'])
            min_entry = min(min_entry, entry['storage'])
        
        avg_storage = total_storage / count if count > 0 else 0
        monthly_storage.append({
            'district_name': district_name,
            'date': entry_date.replace(day=1),  # 1st of the month
            'storage': avg_storage,
            'max_storage': max_entry,
            'min_storage': min_entry
        })
        max_entries.append(max_entry)
        min_entries.append(min_entry)

    # Convert to DataFrame
    reservoir_df = pd.DataFrame(monthly_storage)

    # Parse dates
    reservoir_df['date'] = pd.to_datetime(reservoir_df['date'])

    # Set the date as the index
    reservoir_df.set_index('date', inplace=True)

    # Drop the month column as it's no longer needed
    reservoir_df.drop(columns=['district_name'], inplace=True)

    # Store preprocessed data
    preprocessed_reservoir_data[district_name] = reservoir_df

# Restructure data by date with districts as subnodes
date_aggregated_reservoir_data = {}
for district, df in preprocessed_reservoir_data.items():
    for date, row in df.iterrows():
        date_str = date.strftime('%Y-%m-%d')
        if date_str not in date_aggregated_reservoir_data:
            date_aggregated_reservoir_data[date_str] = {}
        date_aggregated_reservoir_data[date_str][district] = {
            "storage": row["storage"],
            "minimum": row["min_storage"],
            "maximum": row["max_storage"]
        }

# Prepare data for MongoDB insertion
mongo_reservoir_records = []
for date, districts_data in date_aggregated_reservoir_data.items():
    mongo_reservoir_records.append({
        "date": date,
        "districts": districts_data
    })

# Insert data into MongoDB
if mongo_reservoir_records:
    processed_reservoir_collection.insert_many(mongo_reservoir_records)

# The processed data is now stored in the processed_reservoir collection
print("Data preprocessing and insertion completed.")
