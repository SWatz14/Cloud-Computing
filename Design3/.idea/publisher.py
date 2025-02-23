
from google.cloud import pubsub_v1
import cv2
import base64
import glob
import os

# 🔹 Automatically detect Google Cloud credentials JSON file
files = glob.glob("*.json")
if not files:
    raise FileNotFoundError("❌ No Google Cloud credentials JSON file found!")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# 🔹 Google Cloud Pub/Sub Configuration
PROJECT_ID = "kinetic-bot-451603-k4"
TOPIC_NAME = "mnist_image"

# 🔹 Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

# 🔹 Path to dataset folder
dataset_folder = os.path.join(os.getcwd(), "Dataset_Occluded_Pedestrian")

# 🔹 Find images that start with "A_" or "C_" with multiple formats
image_files = []
for ext in ["*.jpg", "*.jpeg", "*.png"]:
    image_files.extend(glob.glob(os.path.join(dataset_folder, "A_*" + ext)))
    image_files.extend(glob.glob(os.path.join(dataset_folder, "C_*" + ext)))

if not image_files:
    print("❌ No images starting with 'A_' or 'C_' found in the dataset!")
    exit()

def publish_image(image_path):
    """Reads an image, encodes it in base64, and publishes it to Pub/Sub."""
    image = cv2.imread(image_path)
    if image is None:
        print(f"❌ Error: Image '{image_path}' not found!")
        return

    _, buffer = cv2.imencode(".jpg", image)
    image_bytes = buffer.tobytes()

    # Convert to base64 (keeps it as bytes)
    encoded_image = base64.b64encode(image_bytes)

    try:
        future = publisher.publish(topic_path, encoded_image)
        print(f"✅ Published '{image_path}' with Message ID: {future.result()}")
    except Exception as e:
        print(f"❌ Error publishing '{image_path}': {e}")

# 🔹 Publish all matching images
for image in image_files:
    publish_image(image)

