from google.cloud import pubsub_v1
import json
import os
import glob

# 🔹 Find Google Cloud credentials JSON file
files = glob.glob("*.json")
if not files:
    raise FileNotFoundError("❌ No Google Cloud credentials JSON file found!")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# 🔹 Google Cloud Pub/Sub Configuration
PROJECT_ID = "kinetic-bot-451603-k4"
SUBSCRIPTION_ID = "mnist_predict-sub"

# 🔹 Initialize Pub/Sub Subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def callback(message):
    """Handles incoming messages from Pub/Sub."""
    try:
        decoded_message = message.data.decode("utf-8")
        parsed_data = json.loads(decoded_message)
        print(f"✅ Received Processed Result:\n{json.dumps(parsed_data, indent=4)}")
        message.ack()

    except Exception as e:
        print(f"❌ Error processing message: {e}")
        message.nack()

print(f"🔄 Listening for messages on {subscription_path}...")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("🔴 Subscription cancelled.")
except Exception as e:
    streaming_pull_future.cancel()
    print(f"❌ Subscription stopped due to error: {e}")
