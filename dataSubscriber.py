from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from zoneinfo import ZoneInfo
from datetime import datetime
import os
import json
import threading
import pandas as pd
from dataLoadToDB import LoadToDB
import signal
import atexit
import sys

PROJECT_ID = "sp25-cs410-trimet-project"
SUBSCRIPTION_ID = "trimet-topic-sub"
SERVICE_ACCOUNT_FILE = "/home/pjuyoung/term-project/sp25-cs410-trimet-project-service-account.json"
OUTPUT_DIR = "/home/pjuyoung/recieved_data/"
BATCH_SIZE = 10000

# Number of seconds the subscriber should listen for messages
#TIMEOUT = 60.0
COUNT = 0
message_list = []
lock = threading.Lock()

def graceful_shutdown(sig=None, frame=None):
    print("\n[Shutdown] Signal received. Flushing remaining messages to DB...")
    load_to_db(triggered_by_timer=True)
    sys.exit(0)

# Register this function to handle termination
atexit.register(graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)   # Ctrl+C
signal.signal(signal.SIGTERM, graceful_shutdown)  # VM shutdown or kill

def start_timer():
    print("Starting 20 minute timer...")
    threading.Timer(1200, lambda: load_to_db(triggered_by_timer=True)).start()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global COUNT
    global message_list
    COUNT += 1
    message_data = message.data.decode()
    if message_data:
        write_file(message_data)
        with lock:
            message_list.append(json.loads(message_data))
            if len(message_list) >= BATCH_SIZE:
                load_to_db()
    message.ack()

def write_file(message_data):
    timestamp = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y%m%d')
    filename = os.path.join(OUTPUT_DIR, f"recieved_data_{timestamp}.json")
    #filename = os.path.join(OUTPUT_DIR, f"recieved_data_20250411.json")

    with open(filename, "a") as file:
        json.dump(json.loads(message_data), file)
        file.write("\n")

def load_to_db(triggered_by_timer=False):
    global message_list
    with lock:
        if message_list:
            df = pd.DataFrame(message_list)
            loader = LoadToDB()
            loader.run(df)
            message_list = []
    if triggered_by_timer:
        start_timer()

def main():
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    pubsub_creds =  (service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))
    subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_creds)
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    start_timer()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `TIMEOUT` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            # streaming_pull_future.result(timeout=TIMEOUT)
            streaming_pull_future.result()
        except:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
        finally:
            load_to_db()

    print(f"{COUNT} messages received")

if __name__ == "__main__":
    main()
