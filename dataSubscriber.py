from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from zoneinfo import ZoneInfo
from datetime import datetime
import os
import json

PROJECT_ID = "sp25-cs410-trimet-project"
SUBSCRIPTION_ID = "trimet-topic-sub"
SERVICE_ACCOUNT_FILE = "/home/pjuyoung/term-project/sp25-cs410-trimet-project-service-account.json"
OUTPUT_DIR = "/home/pjuyoung/recieved_data/"

# Number of seconds the subscriber should listen for messages
#TIMEOUT = 60.0
COUNT = 0
message_list = []

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # print(f"Received {message}.")
    global COUNT
    global message_list
    COUNT += 1
    message_data = message.data.decode()
    message_list.append(json.loads(message_data))
    message.ack()

def write_file():
    global message_list
    timestamp = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y%m%d')
    filename = os.path.join(OUTPUT_DIR, f"recieved_data_{timestamp}.json")
    #filename = os.path.join(OUTPUT_DIR, f"recieved_data_20250415.json")

    with open(filename, "w") as file:
        json.dump(message_list, file)

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

pubsub_creds =  (service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))
subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_creds)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `TIMEOUT` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        # streaming_pull_future.result(timeout=TIMEOUT)
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
    except KeyboardInterrupt:
        write_file()
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

write_file()

print(f"{COUNT} messages received")
