import json
from kafka import KafkaConsumer, KafkaProducer

# --- CONFIG ---
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
# Here we save the last known status of each sensor..
# Example: { ‘S-101’: “FLOWING”, ‘S-102’: ‘BLOCKED’ }
sensor_states = {}

print(f"🕵️ [SMART DETECTOR] Listening to '{INPUT_TOPIC}' with status management...")

# --- MAIN LOOP ---
for message in consumer:
    data = message.value
    sensor_id = data["sensor_id"]
    current_status = data.get("status", "FLOWING") # Default FLOWING if the field is missing
    
    # We restore the previous state (if it does not exist, we assume it was FLOWING).
    previous_status = sensor_states.get(sensor_id, "FLOWING")
    
    # TRANSITION LOGIC (EDGE DETECTION)
    
    # CASE 1: Start of accident (From FLOWING to BLOCKED)
    if current_status == "BLOCKED" and previous_status == "FLOWING":
        
        alert_payload = {
            "type": "TRAFFIC_JAM_STARTED",
            "sensor_id": sensor_id,
            "severity": "HIGH",
            "message": f"🔴 START OF BLOCK: Traffic detected as stationary ({data['speed_kmh']} km/h)",
            "timestamp": data["timestamp"]
        }
        producer.send(OUTPUT_TOPIC, alert_payload)
        print(f"🚨 [ALERT START] New accident on {sensor_id}!")

    # CASE 2: End of Accident (From BLOCKED to FLOWING)
    elif current_status == "FLOWING" and previous_status == "BLOCKED":
        
        alert_payload = {
            "type": "TRAFFIC_JAM_RESOLVED",
            "sensor_id": sensor_id,
            "severity": "INFO",
            "message": f"🟢 SOLVED: Traffic has resumed flowing ({data['speed_kmh']} km/h)",
            "timestamp": data["timestamp"]
        }
        producer.send(OUTPUT_TOPIC, alert_payload)
        print(f"✅ [ALERT END] Accident resolved on {sensor_id}.")

    # CASE 3: No change (BLOCKED->BLOCKED or FLOWING->FLOWING)
    else:
        # Let's not do anything to avoid spamming the topic alerts
        pass

    # Let's update the memory with the current status for the next round
    sensor_states[sensor_id] = current_status