import json
from kafka import KafkaConsumer, KafkaProducer

# --- CONFIGURATION ---
KAFKA_BROKER = "localhost:29092"
INPUT_TOPIC = "raw-traffic"
OUTPUT_TOPIC = "alerts"

# --- SETUP ---
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- STATE MEMORY ---
# Stores the last known status of each sensor.
sensor_states = {}

print(f"🕵️ [SMART DETECTOR] Listening on '{INPUT_TOPIC}' with state management...")

# --- MAIN LOOP ---
for message in consumer:
    data = message.value
    sensor_id = data["sensor_id"]
    
    # We rely on the Gateway's calculation of "status" which is now based on speed
    current_status = data.get("status", "FLOWING")
    
    # Retrieve previous status (Default to FLOWING if new sensor)
    previous_status = sensor_states.get(sensor_id, "FLOWING")
    
    # --- EDGE DETECTION LOGIC ---
    
    # CASE 1: Incident Starts (FLOWING -> BLOCKED)
    # This will now only trigger when speed actually drops below 5.0 km/h
    if current_status == "BLOCKED" and previous_status == "FLOWING":
        
        alert_payload = {
            "type": "TRAFFIC_JAM_STARTED",
            "sensor_id": sensor_id,
            "severity": "HIGH",
            "message": f"🔴 BLOCKAGE STARTED: Traffic stoppage detected ({data['speed_kmh']} km/h)",
            "timestamp": data["timestamp"]
        }
        producer.send(OUTPUT_TOPIC, alert_payload)
        print(f"🚨 [ALERT START] Incident detected on {sensor_id}!")

    # CASE 2: Incident Resolved (BLOCKED -> FLOWING)
    # This will trigger only when speed climbs back above 5.0 km/h
    elif current_status == "FLOWING" and previous_status == "BLOCKED":
        
        alert_payload = {
            "type": "TRAFFIC_JAM_RESOLVED",
            "sensor_id": sensor_id,
            "severity": "INFO",
            "message": f"🟢 RESOLVED: Traffic is flowing again ({data['speed_kmh']} km/h)",
            "timestamp": data["timestamp"]
        }
        producer.send(OUTPUT_TOPIC, alert_payload)
        print(f"✅ [ALERT END] Incident resolved on {sensor_id}.")

    # CASE 3: No State Change
    else:
        # Do nothing to avoid spamming
        pass

    # Update memory for next iteration
    sensor_states[sensor_id] = current_status