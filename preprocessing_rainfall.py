from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler

client = MongoClient('mongodb+srv://deepak297512:hans1725@cluster0.51vc1pc.mongodb.net/')  
db = client['AquaLink']
collection = db['rainfall']

districts = collection.distinct("district")

preprocessed_data = {}
for district in districts:
 
    data_cursor = collection.find({"district": district})
    data_list = list(data_cursor)

    normalized_data = []
    for data in data_list:
        for entry in data['data']:
            entry['district'] = district
            normalized_data.append(entry)

    rainfall_df = pd.DataFrame(normalized_data)

    rainfall_df['date'] = pd.to_datetime(rainfall_df['year'].astype(str) + '-' + rainfall_df['month'], format='%Y-%b')

    rainfall_df.set_index('date', inplace=True)

    rainfall_df.drop(columns=['year', 'month'], inplace=True)

    rainfall_df.ffill(inplace=True)

    scaler = MinMaxScaler()
    rainfall_df[['normal_mm', 'actual_mm']] = scaler.fit_transform(rainfall_df[['normal_mm', 'actual_mm']])

    preprocessed_data[district] = rainfall_df

date_aggregated_data = {}

for district, df in preprocessed_data.items():
    for date, row in df.iterrows():
        if date not in date_aggregated_data:
            date_aggregated_data[date] = {}
        date_aggregated_data[date][district] = {
            "normal_mm": row["normal_mm"],
            "actual_mm": row["actual_mm"]
        }

# Prepare data for MongoDB insertion
mongo_records = []
for date, districts_data in date_aggregated_data.items():
    mongo_records.append({
        "date": date,
        "districts": districts_data
    })

# Insert data into MongoDB
if mongo_records:
    db['processed_collection'].insert_many(mongo_records)

