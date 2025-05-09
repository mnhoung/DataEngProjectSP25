import pandas as pd
import json
import os
from dataLoadToDB import LoadToDB

json_file = '../recieved_data/recieved_data_20250501.json'

with open(json_file, 'r') as file:
    data = [json.loads(line.strip()) for line in file if line.strip()]
print("read json")

df = pd.DataFrame(data)
print("created df")

loader = LoadToDB()
loader.run(df)

print(f"loaded {len(df)} records from {json_file}")
