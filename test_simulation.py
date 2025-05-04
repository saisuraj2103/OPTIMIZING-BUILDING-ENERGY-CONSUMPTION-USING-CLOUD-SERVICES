from azure.iot.device import IoTHubDeviceClient, Message
import time
import json

# Replace with your actual Azure device connection string
AZURE_CONNECTION_STRING = "HostName=my-energy-iot-hub123.azure-devices.net;DeviceId=simulated-energy-device;SharedAccessKey=SX4HLdGGxRie9KldmzAjy1pemzcoRBYknxZDIzMZvGU="

# Create client
try:
    client = IoTHubDeviceClient.create_from_connection_string(AZURE_CONNECTION_STRING)
    client.connect()
    print("✅ Connected to Azure IoT Hub.")
except Exception as e:
    print("❌ Could not connect to Azure IoT Hub:", e)
    exit()

# Test data for alert logic
def generate_test_data():
    return {
        "room": "999",
        "occupancy": 0,
        "power_kwh": 5.6,
        "temperature": 21.0,
        "hvac_on": True,
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }

print("🚨 Sending test data...")
for i in range(5):
    try:
        data = generate_test_data()
        json_data = json.dumps(data)
        message = Message(json_data)
        client.send_message(message)
        print(f"✅ Sent test message {i+1}: {data}")
        time.sleep(3)  # Give more time between messages
    except Exception as e:
        print(f"❌ Error sending message {i+1}: {e}")

# Give time for Azure to fully flush messages
time.sleep(5)
client.shutdown()
print("✅ Test simulation finished.")
