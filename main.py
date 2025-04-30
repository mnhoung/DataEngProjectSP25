from google.cloud import pubsub_v1
from google.oauth2 import service_account
from concurrent import futures
from urllib.request import urlopen
import csv
import json

PROJECT_ID = "sp25-cs410-trimet-project"
TOPIC_ID = "trimet-topic"
SERVICE_ACCOUNT_FILE = "/home/pjuyoung/term-project/sp25-cs410-trimet-project-service-account.json"
BASE_DIR = "/home/pjuyoung/"
# Base URL template
BASE_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={}"
# Read vehicle IDs from a CSV file
VEHICLE_ID_CSV = "/home/pjuyoung/term-project/vehicle_ids.csv"

def future_callback(future):
    try:
        # Wait for the result of the publish operation.
        future.result()  
    except Exception as e:
        print(f"An error occurred: {e}")

def publish_data(data):
    # Get a credential object from google.oauth2 using your service account file
    pubsub_creds =  (service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))

    # Use that credential object to create your publisher object.
    publisher = pubsub_v1.PublisherClient(credentials=pubsub_creds)
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    future_list = []
    count = 0
    try:
        # Only publish if there's data in the file
        for obj in data:
            json_data = json.dumps(obj).encode()
            future = publisher.publish(topic_path, json_data)
            future.add_done_callback(future_callback)
            future_list.append(future)
            count += 1
            if count % 50000 == 0:
                print(count)

    except Exception as e:
        print(f"Error occured while loading json: {e}")
        exit(0)
    for future in futures.as_completed(future_list):
        continue
    print(f"Published {count} messages to {topic_path}.")

def load_vehicle_ids(csv_path):
    vehicle_ids = []
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row:  # skip empty rows
                    vehicle_ids.append(row[0].strip())
    except FileNotFoundError:
        print(f"[Error] CSV file not found: {csv_path}")
    return vehicle_ids

def fetch_breadcrumb_data(vehicle_id):
    url = BASE_URL.format(vehicle_id)
    try:
        with urlopen(url) as response:
            charset = response.headers.get_content_charset() or 'utf-8'
            data = json.loads(response.read().decode(charset))
            print(f"[✓] Saved data for vehicle {vehicle_id}")
            return data

    except Exception as e:
        print(f"[Error] Vehicle {vehicle_id}: {e}")

def main():
    vehicle_ids = load_vehicle_ids(VEHICLE_ID_CSV)
    for vid in vehicle_ids:
        data = fetch_breadcrumb_data(vid)
        if data:
            publish_data(data)

if __name__ == "__main__":
    main()
