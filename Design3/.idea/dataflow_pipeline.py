import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import cv2
import numpy as np
import torch
import torchvision.transforms as transforms
from ultralytics import YOLO
from PIL import Image
from google.cloud import pubsub_v1
import base64
import json
import glob
import sys

# 🔹 Automatically detect Google Cloud credentials JSON file
files = glob.glob("*.json")

if not files:
    print("❌ No Google Cloud credentials JSON file found! Exiting.")
    sys.exit(1)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
print(f"✅ Using credentials: {files[0]}")

# 🔹 Google Cloud Configuration
PROJECT_ID = "kinetic-bot-451603-k4"
REGION = "us-central1"
INPUT_TOPIC = f"projects/{PROJECT_ID}/topics/mnist_image"
OUTPUT_TOPIC = f"projects/{PROJECT_ID}/topics/mnist_predict"
GCS_BUCKET = "gs://kinetic-bucket-staging"

# 🔹 Load YOLO model (Pedestrian Detection)
detection_model = YOLO("yolov8n.pt")

# 🔹 Load MiDaS model (Depth Estimation)
depth_model = torch.hub.load("intel-isl/MiDaS", "MiDaS_small")
depth_model.eval()


def estimate_depth(image):
    """Estimate depth using MiDaS model."""
    transform = transforms.Compose([
        transforms.Resize((256, 256)),
        transforms.ToTensor(),
    ])
    image_tensor = transform(Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))).unsqueeze(0)
    image_tensor = image_tensor.to(dtype=torch.float32)

    with torch.no_grad():
        depth_map = depth_model(image_tensor).squeeze().numpy()

    return depth_map


class ProcessImage(beam.DoFn):
    def process(self, element):
        """Processes an image for pedestrian detection & depth estimation."""
        try:
            # 🔹 Extract raw bytes from Pub/Sub message
            message_data = element.decode("utf-8")  # Decode Pub/Sub message to string
            image_data = base64.b64decode(message_data)  # Convert base64 string to bytes

            # 🔹 Convert to OpenCV image
            image = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)

            if image is None:
                print("❌ Error: Failed to decode image")
                return

            # 🔹 Run pedestrian detection (YOLO)
            results = detection_model(image)

            # 🔹 Estimate depth (MiDaS)
            depth_map = estimate_depth(image)

            # 🔹 Process results
            detections = []
            for r in results:
                for box in r.boxes:
                    x1, y1, x2, y2 = map(int, box.xyxy[0])
                    label = detection_model.names[int(box.cls[0])]

                    if label == "person":
                        avg_depth = np.mean(depth_map[y1:y2, x1:x2])
                        detections.append({"bounding_box": [x1, y1, x2, y2], "depth": avg_depth})

            # 🔹 Publish processed results to `mnist_predict`
            publisher = pubsub_v1.PublisherClient()
            publisher.publish(OUTPUT_TOPIC, json.dumps(detections).encode("utf-8"))

            yield detections

        except Exception as e:
            print(f"❌ Error processing image: {e}")


def run():
    """Apache Beam pipeline reading from Pub/Sub and running on Google Cloud Dataflow."""

    # 🔹 Configure Pipeline Options for Google Cloud Dataflow
    options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region=REGION,
        temp_location=f"{GCS_BUCKET}/temp",
        staging_location=f"{GCS_BUCKET}/staging",
        job_name="pedestrian-detection-pipeline",
    )

    # 🔹 Define the Apache Beam pipeline
    with beam.Pipeline(options=options) as pipeline:
        images = (pipeline
                  | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC).with_output_types(bytes)
                  | "Process Image" >> beam.ParDo(ProcessImage())
                  | "Publish Results" >> beam.io.WriteToPubSub(OUTPUT_TOPIC, with_attributes=False))


if __name__ == "__main__":
    run()
