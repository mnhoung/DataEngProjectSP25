import pandas as pd
import json
import os
from dataLoadToDB import LoadToDB

files = [
    'recieved_data_20250501.json',
    'recieved_data_20250502.json',
    'recieved_data_20250503.json',
    'recieved_data_20250504.json',
    'recieved_data_20250505.json',
    'recieved_data_20250506.json',
    'recieved_data_20250507.json',
    'recieved_data_20250508.json'
]

directory = '../recieved_data'
all_data = []

for filename in files:
    file_path = os.path.join(directory, filename)
    with open(file_path, 'r') as f:
        data = [json.loads(line.strip()) for line in f if line.strip()]
        all_data.extend(data)
    print(f"Loaded {len(data)} rows from {filename}")

df = pd.DataFrame(all_data)

loader = LoadToDB()
loader.run(df)

print(f"Finished loading {len(df)} records from selected files.")
