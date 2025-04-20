from google.cloud import pubsub_v1
from google.oauth2 import service_account
from concurrent import futures
from zoneinfo import ZoneInfo
from datetime import datetime
import json
import os

PROJECT_ID = "sp25-cs410-trimet-project"
TOPIC_ID = "trimet-topic"
SERVICE_ACCOUNT_FILE = "/home/pjuyoung/term-project/sp25-cs410-trimet-project-service-account.json"
BASE_DIR = "/home/pjuyoung/"

def future_callback(future):
    try:
        # Wait for the result of the publish operation.
        future.result()  
    except Exception as e:
        print(f"An error occurred: {e}")

# Get a credential object from google.oauth2 using your service account file
pubsub_creds =  (service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))

# Use that credential object to create your publisher object.
publisher = pubsub_v1.PublisherClient(credentials=pubsub_creds)
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def main():
    future_list = []
    count = 0
    timestamp = datetime.now(ZoneInfo("America/Los_Angeles"))
    folder_name = timestamp.strftime("breadcrumb_data_%Y%m%d")
    #folder_name = "breadcrumb_data_20250411"
    folder_path = os.path.join(BASE_DIR, folder_name)

    # Check if the folder exists
    if os.path.exists(folder_path):
        # Loop through the json files
        for filename in os.listdir(folder_path):
            if filename.startswith("vehicle") and filename.endswith(".json"):
                file_path = os.path.join(folder_path, filename)

                # Open and dump the file
                with open(file_path, "r") as file:
                    try:
                        data = json.load(file)

                        # Only publish if there's data in the file
                        if data:
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

if __name__ == "__main__":
    main()
