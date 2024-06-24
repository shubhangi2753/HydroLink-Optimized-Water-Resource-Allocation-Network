import os
import csv
from pymongo import MongoClient

csv_directory = r'D:\AquaLink\Data\Rainfall'

client = MongoClient("mongodb+srv://deepak297512:hans1725@cluster0.51vc1pc.mongodb.net/")
db = client["AquaLink"]
collection = db["rainfall"]

try:
    for filename in os.listdir(csv_directory):
        if filename.endswith(".csv"):
            district_name = filename.split('.')[0]
            file_path = os.path.join(csv_directory, filename)
            with open(file_path, 'r') as file:
                csvReader = csv.DictReader(file)
                data_list = []  # List to store data for current CSV file
                for row in csvReader:
                    cleaned_row = {key.strip('ï»¿'): value for key, value in row.items()}
                    date = cleaned_row.get("Dates", "").strip()
                    normal_mm = float(cleaned_row.get("NORMAL (mm) ", 0))
                    actual_mm = float(cleaned_row.get("ACTUAL (mm) ", 0))
                    month, year = date.split('-')
                    data = {
                        "district": district_name,
                        "year": int("20" + year),  # Assuming data is for 21st century
                        "month": month,
                        "normal_mm": normal_mm,
                        "actual_mm": actual_mm
                    }
                    data_list.append(data)  # Append data to the list
                # Insert the list of data as a single document
                collection.insert_one({"district": district_name, "data": data_list})
    print("Data inserted successfully")
except Exception as e:
    print("An error occurred:", e)
