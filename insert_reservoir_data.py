from pymongo import MongoClient
from datetime import datetime
import os

csv_directory = r'D:\AquaLink\Data\Reservoir'

client = MongoClient("mongodb+srv://deepak297512:hans1725@cluster0.51vc1pc.mongodb.net/")
db = client["AquaLink"]
collection = db["reservoir"]

def preprocess_date(date_str):
    formats = ["%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%d.%m.%Y"]  # Add more formats as needed
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    return None

def read_reservoir_data(file_path):
    data = {}
    with open(file_path, 'r') as file:
        next(file)  # Skip the header line
        for line in file:
            parts = line.strip().split(',')
            if len(parts) != 2:
                print(f"Invalid line: {line}")
                continue
            date_str, storage_str = parts[0].strip('"'), parts[1]
            try:
                date = preprocess_date(date_str)
            except ValueError:
                print(f"Invalid date format: {date_str}")
                continue
            try:
                storage = float(storage_str)
            except ValueError:
                print(f"Invalid storage value: {storage_str}")
                continue
            month_year_key = date.strftime("%Y-%m")
            if month_year_key not in data:
                data[month_year_key] = {"data": [], "mean": 0.0}  # Initializing mean value
            data[month_year_key]["data"].append({"date": date, "storage": storage})
    
    # Calculate mean storage value for each month
    for month_data in data.values():
        storage_values = [entry["storage"] for entry in month_data["data"]]
        month_data["mean"] = sum(storage_values) / len(storage_values) if storage_values else 0.0
    
    return data


try:
    for filename in os.listdir(csv_directory):
        if filename.endswith(".csv"): 
            district_name = filename.split('.')[0]
            file_path = os.path.join(csv_directory, filename) 
            district_data = read_reservoir_data(file_path)

            # Insert aggregated data into MongoDB per district
            collection.insert_one({
                "district_name": district_name,
                "monthly_data": district_data
            })

    print("Monthly data per district inserted into MongoDB.")

except Exception as e:
    print("An error occurred:", e)
