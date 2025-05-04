from azure.iot.device import IoTHubDeviceClient, Message
import time
import json

# Replace this with your actual Azure connection string
AZURE_CONNECTION_STRING = "HostName=my-energy-iot-hub123.azure-devices.net;DeviceId=simulated-energy-device;SharedAccessKey=SX4HLdGGxRie9KldmzAjy1pemzcoRBYknxZDIzMZvGU="

client = IoTHubDeviceClient.create_from_connection_string(AZURE_CONNECTION_STRING)

rooms = ["101", "102", "103"]

for room in rooms:
    bad_data = {
        "room": room,
        "occupancy": 0,
        "power_kwh": 5.5,
        "temperature": 21.0,
        "hvac_on": True,
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }

    print(f"🚨 Sending alert-triggering data for Room {room}")
    message = Message(json.dumps(bad_data))
    client.send_message(message)
    time.sleep(1)

print("✅ Done sending all test data.")
