import os
import json
import time
from google.cloud import pubsub_v1

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\saisu\energyoptimization-456710-2de3a5de0520.json"

# Replace with your actual GCP project ID and topic
GCP_PROJECT_ID = "energyoptimization-456710"  # e.g., "energyoptimization-456710"
PUBSUB_TOPIC_ID = "energy-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)

def generate_test_data():
    return {
        "room": "999",
        "occupancy": 0,
        "power_kwh": 5.6,
        "temperature": 21.0,
        "hvac_on": True,
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }

print("🚨 Sending test data to GCP Pub/Sub...")

for i in range(5):
    data = generate_test_data()
    json_data = json.dumps(data).encode("utf-8")
    publisher.publish(topic_path, data=json_data)
    print(f"✅ Sent test message {i+1}: {data}")
    time.sleep(2)

print("✅ Done sending test messages to Pub/Sub.")
