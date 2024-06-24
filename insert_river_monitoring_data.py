import os
import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb+srv://deepak297512:hans1725@cluster0.51vc1pc.mongodb.net/')
db = client['AquaLink']  # Name of your MongoDB database
collection_name = 'river_levels'  # Name of the new collection
db.drop_collection(collection_name)  # Drop the collection if it exists to ensure it's created fresh
collection = db[collection_name]

def process_folder(folder_path):
    date_data = {}

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            district_name = file_name.split('.')[0] # Extract district name from the file name
            df = pd.read_csv(file_path, delimiter=',', skiprows=1)  # Use comma (',') as delimiter
            # Assuming the first column is 'Dates'
            df['Dates'] = pd.to_datetime(df['Dates'], format='%b-%Y')  # Ensure uniform date format
            df.set_index('Dates', inplace=True)
            
            # Ensure level values are stored as integers
            for column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
            
            data = df.to_dict(orient='index')
            
            # Clean the column names
            cleaned_data = {date.strftime('%b-%Y'): 
                            {k.replace('(Flow in cumecs)', '').strip(): v for k, v in values.items()} 
                            for date, values in data.items()}
            
            for date, values in cleaned_data.items():
                if date not in date_data:
                    date_data[date] = {}
                if district_name not in date_data[date]:
                    date_data[date][district_name] = {}
                
                date_data[date][district_name].update(values)

    # Save data to MongoDB
    for date, districts in date_data.items():
        collection.update_one(
            {'date': date},
            {'$set': {'districts': districts}},
            upsert=True
        )

# Example usage
folder_path = r"D:\AquaLink\Data\River Water Monitoring"
process_folder(folder_path)
