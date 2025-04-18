from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.oauth2 import service_account

PROJECT_ID = "sp25-cs410-trimet-project"
SUBSCRIPTION_ID = "trimet-topic-sub"
SERVICE_ACCOUNT_FILE = "/home/pjuyoung/term-project/sp25-cs410-trimet-project-service-account.json"

# Number of seconds the subscriber should listen for messages
TIMEOUT = 60.0
COUNT = 0

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # print(f"Received {message}.")
    global COUNT
    COUNT += 1
    message.ack()

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
        streaming_pull_future.result(timeout=TIMEOUT)
        # streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

print(f"{COUNT} messages received")
