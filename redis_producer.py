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
topic_name = "Image"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# Folder containing the images
image_folder = "C:\\ProjectMilestone2\\Design\\Dataset_Occluded_Pedestrian"  # Replace with the actual folder path

# Function to serialize and publish image data
def publish_image(image_path):
    try:
        # Read and serialize the image
        with open(image_path, "rb") as img_file:
            image_data = base64.b64encode(img_file.read()).decode('utf-8')  # Serialize the image to base64

        # Create message data
        message_data = {
            'image_name': os.path.basename(image_path),
            'image_data': image_data
        }

        # Publish the message to Pub/Sub
        future = publisher.publish(topic_path, json.dumps(message_data).encode('utf-8'))
        print(f"Published image {os.path.basename(image_path)} to the topic with ID: {future.result()}")

    except Exception as e:
        print(f"Error publishing image {image_path}: {e}")

# Search for images in the folder and publish them
def publish_images():
    for image_filename in os.listdir(image_folder):
        if image_filename.endswith(('.png', '.jpg', '.jpeg', '.gif')):  # Add more image extensions as needed
            image_path = os.path.join(image_folder, image_filename)
            publish_image(image_path)

if __name__ == "__main__":
    publish_images()

