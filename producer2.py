from google.cloud import pubsub_v1
import csv
import json
import os
import glob

<<<<<<< HEAD
# Set up Google Cloud credentials
files = glob.glob('*.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Google Cloud Pub/Sub configuration
project_id = "perfect-altar-449720-k7"
topic_name = "csv-records-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

def produce_csv_data(csv_file):
    """Publish CSV records to Pub/Sub."""
=======

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
>>>>>>> 07bb24d28beb21613eb1b722537a0e8e18db6a3f
    try:
        with open(csv_file, mode='r', newline='') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
<<<<<<< HEAD

                data = {
                    "id": row[0],
                    "label": row[1],
                    "other_column": row[2],
                }
                data_json = json.dumps(data)
                future = publisher.publish(topic_path, data_json.encode("utf-8"))
=======
                data = json.dumps({"data": row})  # Convert the row to JSON format
                future = publisher.publish(topic_path, data.encode("utf-8"))
>>>>>>> 07bb24d28beb21613eb1b722537a0e8e18db6a3f
                print(f"Published message ID {future.result()}")
    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
<<<<<<< HEAD
    csv_file = "Labels.csv"
    produce_csv_data(csv_file)

=======
    csv_file = "Labels.csv"  # Specify the correct CSV file name
    produce_csv_data(csv_file)


>>>>>>> 07bb24d28beb21613eb1b722537a0e8e18db6a3f
      