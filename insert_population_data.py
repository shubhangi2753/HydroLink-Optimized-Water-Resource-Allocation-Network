import pandas as pd
from pymongo import MongoClient
# censusindia.gov
client = MongoClient('mongodb+srv://deepak297512:hans1725@cluster0.51vc1pc.mongodb.net/')
db = client['AquaLink']
collection = db['Population']

excel_file_path = r"C:\Users\Lenovo\Downloads\2011-IndiaStateDist-0000.xlsx"
selected_fields = ["State", "Name", "TRU", "No_HH", "TOT_P"]

data = pd.read_excel(excel_file_path)

selected_data = data[selected_fields]

karnataka_data = selected_data[selected_data['State'] == 29]

grouped = karnataka_data.groupby('Name')

nested_data = []
for name, group in grouped:
    district_data = {
        'Name': name,
        'Data': group.drop(columns='Name').to_dict(orient='records')
    }
    nested_data.append(district_data)

collection.insert_many(nested_data)

print("Nested data inserted successfully into MongoDB")
