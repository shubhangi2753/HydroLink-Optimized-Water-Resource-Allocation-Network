import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb+srv://deepak297512:hans1725@cluster0.51vc1pc.mongodb.net/')
db = client['AquaLink']

# Define collection names
input_collection_name = 'river_levels'
final_collection_name = 'processed_river_levels'

db.drop_collection(final_collection_name)
final_collection = db[final_collection_name]

def preprocess_data(data):
    date_range = pd.date_range(start='2015-01-01', end='2020-01-01', freq='MS')
    all_districts = set()
    
    for record in data:
        districts = record['districts']
        for district, values in districts.items():
            if isinstance(values, dict):
                for subdistrict in values.keys():
                    all_districts.add(f"{district} {subdistrict}")
            else:
                all_districts.add(f"{district} None")
    
    all_districts = list(all_districts)
    
    complete_data = pd.DataFrame(index=date_range, columns=all_districts)
    
    for record in data:
        date = pd.to_datetime(record['date'], format='%b-%Y')
        districts = record['districts']
        for district, values in districts.items():
            if isinstance(values, dict):
                for subdistrict, value in values.items():
                    column_name = f"{district} {subdistrict}"
                    if column_name in complete_data.columns:
                        complete_data.at[date, column_name] = pd.to_numeric(value, errors='coerce')
            else:
                column_name = f"{district} None"
                if column_name in complete_data.columns:
                    complete_data.at[date, column_name] = pd.to_numeric(values, errors='coerce')
    
    complete_data = complete_data.apply(pd.to_numeric, errors='coerce')
    
    complete_data.interpolate(method='linear', inplace=True)
    
    complete_data.fillna(method='ffill', inplace=True)
    complete_data.fillna(method='bfill', inplace=True)

    processed_data = []
    for date in date_range:
        date_str = date.strftime('%b-%Y')
        date_dict = {'date': date_str, 'districts': {}}
        for district in all_districts:
            value = complete_data.at[date, district]
            main_district, sub_district = district.split(' ', 1)
            if main_district not in date_dict['districts']:
                date_dict['districts'][main_district] = {}
            if sub_district != 'None':
                date_dict['districts'][main_district][sub_district] = int(value)
            else:
                date_dict['districts'][main_district]['value'] = int(value)
        processed_data.append(date_dict)

    return processed_data

def reorder_district_data(district_data):
    reordered_data = {}
    num_sub_objects = len(district_data) // 4
    for i in range(1, num_sub_objects + 1):
        reordered_data[str(i)] = {
            "Last 10 Year Average": district_data.get(f"{i} Last 10 Year Average", 0),
            "Last Year": district_data.get(f"{i} Last Year", 0),
            "Current Year": district_data.get(f"{i} Current Year", 0),
            "Level (m)": district_data.get(f"{i} Level (m)", 0)
        }
    return reordered_data

def save_to_mongo(collection, data):
    collection.insert_many(data)

input_collection = db[input_collection_name]
data = list(input_collection.find({}))

processed_data = preprocess_data(data)

for document in processed_data:
    for district in document["districts"]:
        document["districts"][district] = reorder_district_data(document["districts"][district])
    final_collection.insert_one(document)

print("Data preprocessing, reordering, and saving to the final collection completed successfully.")