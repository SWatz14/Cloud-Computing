from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import json
import os
import glob

# Set up Google Cloud credentials
files=glob.glob('*.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Google Cloud Pub/Sub configuration
project_id = "dark-automata-448802-k3"
topic_name = "csv-records-topic"
subscription_id = "csv-records-topic-sub"
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = subscriber.topic_path(project_id, topic_name)

def callback(message):

    try:
        # Deserialize the message data
        data = json.loads(message.data.decode("utf-8"))
        print(f"Received message: {data}")
        # Acknowledge the message
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")

def consume_messages():

    try:
        subscriber.subscribe(subscription_path, callback=callback)
        print(f"Listening for messages on {subscription_path}...\n")
        while True:
            pass  # Keep the main thread alive
    except KeyboardInterrupt:
        print("Stopped listening for messages.")
    except Exception as e:
        print(f"Error in consume_messages: {e}")

if __name__ == "__main__":
    consume_messages()

