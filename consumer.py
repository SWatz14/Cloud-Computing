<<<<<<< HEAD
from google.cloud import pubsub_v1
import json
import os
import glob
import signal
import sys
import mysql.connector  # MySQL connector

# Set up Google Cloud credentials
files = glob.glob('*.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Google Cloud Pub/Sub configuration
project_id = "perfect-altar-449720-k7"
=======
from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import json
import os
import glob

# Set up Google Cloud credentials
files=glob.glob('*.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Google Cloud Pub/Sub configuration
project_id = "dark-automata-448802-k3"
>>>>>>> 07bb24d28beb21613eb1b722537a0e8e18db6a3f
topic_name = "csv-records-topic"
subscription_id = "csv-records-topic-sub"
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
<<<<<<< HEAD

# MySQL connection details
db_config = {
    "host": "34.152.48.73:3306",
    "user": "usr",
    "password": "mysql-password",
    "database": "pedestrian_data",
}


def save_to_mysql(data):
    """Save the received data into the MySQL database."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()


        insert_query = """
            INSERT INTO your_table_name (id, label, other_column)
            VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (data['id'], data['label'], data['other_column']))
        conn.commit()  # Commit the transaction
        print(f"Inserted {data} into the database.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()


def callback(message):
    """Callback function for processing Pub/Sub messages."""
=======
topic_path = subscriber.topic_path(project_id, topic_name)

def callback(message):

>>>>>>> 07bb24d28beb21613eb1b722537a0e8e18db6a3f
    try:
        # Deserialize the message data
        data = json.loads(message.data.decode("utf-8"))
        print(f"Received message: {data}")
<<<<<<< HEAD

        # Save the data to MySQL
        save_to_mysql(data)

=======
>>>>>>> 07bb24d28beb21613eb1b722537a0e8e18db6a3f
        # Acknowledge the message
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")

<<<<<<< HEAD

def consume_messages():
    """Consume messages from the Pub/Sub subscription."""

    def signal_handler(sig, frame):
        print("\nGracefully shutting down...")
        subscriber.close()
        sys.exit(0)

    try:
        # Subscribe to the subscription and start receiving messages asynchronously
        future = subscriber.subscribe(subscription_path, callback=callback)
        signal.signal(signal.SIGINT, signal_handler)  # Graceful shutdown
        print(f"Listening for messages on {subscription_path}...\n")

        # Block until the future is completed (in the case of a shutdown or error)
        future.result()  # Will raise an exception if the listener crashes or is stopped

    except Exception as e:
        print(f"Error in consume_messages: {e}")


=======
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

>>>>>>> 07bb24d28beb21613eb1b722537a0e8e18db6a3f
if __name__ == "__main__":
    consume_messages()

