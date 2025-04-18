from google.cloud import pubsub_v1
from google.oauth2 import service_account
import json
import os

project_id = "sp25-cs410-trimet-project"
topic_id = "trimet-topic"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)
base_dir = "/home/pjuyoung/"


def fetch_data(file_path):
    # Open and dump the file
    file_path = os.path.join(file_path, "vehicle_3321_20250416_190052.json")
    with open(file_path, "r") as file:
        try:
            data = json.load(file)

            # Only publish if there's data in the file
            if data:
                for obj in data:
                    json_data = json.dumps(obj).encode("utf-8")
                    future = publisher.publish(topic_path, json_data)
                    print(future.result())
        except Exception as e:
            print(f"Error occured while loading json: {e}")
            exit(0)

def main():
    '''
    for folder in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder)

        # Only open breadcrumb folders
        if folder.startswith("breadcrumb_data"):
            # Loop through the json files
            for filename in os.listdir(folder_path):
                if filename.startswith("vehicle") and filename.endswith(".json"):
                    file_path = os.path.join(folder_path, filename)
                    fetch_data(file_path)
    '''
    fetch_data("/home/pjuyoung/breadcrumb_data_20250417/")
    print(f"Published messages to {topic_path}.")
if __name__ == "__main__":
    main()
