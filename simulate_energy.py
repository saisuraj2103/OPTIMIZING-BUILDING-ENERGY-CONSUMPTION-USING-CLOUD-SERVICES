import os
import json
import time
import random
from azure.iot.device import IoTHubDeviceClient, Message
from google.cloud import pubsub_v1

# 🔐 Set GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\saisu\energyoptimization-456710-2de3a5de0520.json"

# 🔗 Replace with your Azure connection string
AZURE_CONNECTION_STRING = "HostName=my-energy-iot-hub123.azure-devices.net;DeviceId=simulated-energy-device;SharedAccessKey=SX4HLdGGxRie9KldmzAjy1pemzcoRBYknxZDIzMZvGU="

# 🔗 Replace with your GCP project ID
GCP_PROJECT_ID = "energyoptimization-456710"
GCP_TOPIC_ID = "energy-topic"

# Azure IoT Hub client
iot_client = IoTHubDeviceClient.create_from_connection_string(AZURE_CONNECTION_STRING)

# Google Cloud Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_PROJECT_ID, GCP_TOPIC_ID)

# Generate random mock data
def generate_data():
    return {
        "room": random.choice(["101", "102", "103"]),
        "occupancy": random.randint(0, 10),
        "power_kwh": round(random.uniform(1.0, 5.0), 2),
        "temperature": round(random.uniform(22.0, 30.0), 1),
        "hvac_on": random.choice([True, False]),
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }

# Loop to send data
print("Streaming energy data to Azure IoT Hub and Google Cloud Pub/Sub...")
while True:
    data = generate_data()
    json_data = json.dumps(data)

    # Send to Azure
    iot_client.send_message(Message(json_data))

    # Send to GCP
    publisher.publish(topic_path, data=json_data.encode("utf-8"))

    print("Sent:", data)
    time.sleep(2)
