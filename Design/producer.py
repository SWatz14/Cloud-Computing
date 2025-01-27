from google.cloud import pubsub_v1
import csv
import json
import os
import glob


# Set up Google Cloud credentials
files=glob.glob('*.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Google Cloud Pub/Sub configuration
project_id = "dark-automata-448802-k3"
topic_name = "csv-records-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print (f"Publisher client: {topic_path}")

def produce_csv_data(csv_file):
    try:
        with open(csv_file, mode='r', newline='') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                data = json.dumps({"data": row})  # Convert the row to JSON format
                future = publisher.publish(topic_path, data.encode("utf-8"))
                print(f"Published message ID {future.result()}")
    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    csv_file = "Labels.csv"  # Specify the correct CSV file name
    produce_csv_data(csv_file)


      