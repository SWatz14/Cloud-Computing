from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file
import base64
import os
import random
import numpy as np                      # pip install numpy    ##to install
import time
import json                             # Import the json module
import redis                            # Import the redis module

# Search the current directory for the JSON file (including the service account key)
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]


# Set up Redis connection
redis_host = '34.118.129.114'  # Change this if using Google Cloud Redis
redis_port = 6379
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0)

# Google Cloud Pub/Sub configuration
project_id = "perfect-altar-449720-k7"
subscription_id = "Image-sub"
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Callback function to save image data to Redis
def save_to_redis(message):
    try:
        # Deserialize the message data
        data = json.loads(message.data.decode("utf-8"))
        image_name = data['image_name']
        image_data = data['image_data']

        # Store image in Redis with image name as the key
        redis_client.set(image_name, image_data)
        print(f"Stored image '{image_name}' in Redis")

        # Acknowledge the message
        message.ack()

    except Exception as e:
        print(f"Error processing message: {e}")

# Start consuming messages from the Pub/Sub topic
def consume_messages():
    try:
        subscriber.subscribe(subscription_path, callback=save_to_redis)
        print(f"Listening for messages on {subscription_path}...\n")

        # Keep the process running to listen for messages
        while True:
            pass  # Keeps the program running, processing messages in the background

    except Exception as e:
        print(f"Error in consume_messages: {e}")

if __name__ == "__main__":
    consume_messages()

