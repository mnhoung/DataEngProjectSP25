import json
import os

file_path = "/home/pjuyoung/recieved_data/recieved_data_20250502.json"
data_list = []
with open(file_path, "r") as file:
    '''
    lines = file.readlines()
    first_line = lines[0].strip()
    print(first_line)
    '''
    for line in file:
        data = json.loads(line.strip())  # strip any extra whitespace/newlines
        data_list.append(data)

print(data_list[0])
print(data_list[1])
print(len(data_list))
