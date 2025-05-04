import os
from google.cloud import bigquery

# 🔐 Set your GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\saisu\energyoptimization-456710-2de3a5de0520.json"

# 🔧 GCP Configuration
project_id = "energyoptimization-456710"  # e.g., "energyoptimization-456710"
dataset_id = "energy_data"
source_table = "energy_stream"
alert_table = "energy_alerts"
control_table = "energy_control_actions"

# 📡 Create BigQuery client
client = bigquery.Client(project=project_id)

# ⏱️ Query latest data from energy_stream table
query = f"""
SELECT room, occupancy, power_kwh, temperature, hvac_on, timestamp
FROM `{project_id}.{dataset_id}.{source_table}`
WHERE TIMESTAMP(timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
"""
results = client.query(query).result()

# 🚨 Process alerts and actions
alerts = []
control_actions = []

for row in results:
    issue = None

    if row.hvac_on and row.occupancy == 0:
        issue = "Wasted HVAC: Room is empty but HVAC is ON"
    elif row.power_kwh > 4.0 and row.occupancy < 2:
        issue = "High power usage with low occupancy"
    elif row.temperature < 22.5 and row.hvac_on:
        issue = "Overcooling detected while HVAC is ON"

    if issue:
        # Add alert
        alerts.append({
            "room": row.room,
            "occupancy": row.occupancy,
            "power_kwh": row.power_kwh,
            "hvac_on": row.hvac_on,
            "timestamp": row.timestamp,
            "issue": issue
        })

        # Simulate optimization action
        control_actions.append({
            "room": row.room,
            "action": "Turn HVAC OFF",
            "timestamp": row.timestamp,
            "reason": issue
        })
        print(f"⚙️ Action taken for room {row.room}: Turn HVAC OFF")

# ✅ Write alerts to energy_alerts
if alerts:
    alert_table_ref = f"{project_id}.{dataset_id}.{alert_table}"
    alert_errors = client.insert_rows_json(alert_table_ref, alerts)

    if alert_errors:
        print("❌ Errors writing alerts:", alert_errors)
    else:
        print(f"✅ {len(alerts)} alerts written to BigQuery.")
else:
    print("✅ No issues found in the latest data.")

# ✅ Write control actions to energy_control_actions
if control_actions:
    control_table_ref = f"{project_id}.{dataset_id}.{control_table}"
    control_errors = client.insert_rows_json(control_table_ref, control_actions)

    if control_errors:
        print("❌ Errors writing control actions:", control_errors)
    else:
        print(f"✅ {len(control_actions)} simulated actions written to BigQuery.")
