import json
import uuid
from kafka import KafkaConsumer, KafkaProducer

# --- CONFIGURATION ---
KAFKA_BROKER = "localhost:29092"
INPUT_TOPIC = "raw-traffic"
OUTPUT_TOPIC = "alerts"

# --- SETUP ---
# generate a random group_id so we always start fresh (LATEST) and don't process old lags
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    group_id=f"detector-{uuid.uuid4()}", 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_states = {}

print(f"🕵️ [SMART DETECTOR] Listening on '{INPUT_TOPIC}'...")
print(f"🔎 DEBUG MONITOR: Watching for 'GW-BATCH-50' specifically...")

for message in consumer:
    data = message.value
    sensor_id = data["sensor_id"]
    current_status = data.get("status", "FLOWING")
    
    # --- DEBUGGING PRINT ---
    # This proves the detector is actually receiving data for your target
    if sensor_id == "GW-BATCH-50":
        print(f"👀 [DEBUG] {sensor_id}: Speed={data['speed_kmh']:.1f} | Status={current_status}")

    # Retrieve previous status
    previous_status = sensor_states.get(sensor_id, "FLOWING")
    
    # CASE 1: Incident Starts
    if current_status == "BLOCKED" and previous_status == "FLOWING":
        alert_payload = {
            "type": "TRAFFIC_JAM_STARTED",
            "sensor_id": sensor_id,
            "severity": "HIGH",
            "message": f"🔴 BLOCKAGE DETECTED: Sensor {sensor_id} reports stoppage ({data['speed_kmh']:.1f} km/h)",
            "timestamp": data["timestamp"]
        }
        producer.send(OUTPUT_TOPIC, alert_payload)
        producer.flush() # Force send immediately
        print(f"🚨 [ALERT START] Incident detected on {sensor_id}!")

    # CASE 2: Incident Resolved
    elif current_status == "FLOWING" and previous_status == "BLOCKED":
        alert_payload = {
            "type": "TRAFFIC_JAM_RESOLVED",
            "sensor_id": sensor_id,
            "severity": "INFO",
            "message": f"🟢 RESOLVED: Traffic flowing on {sensor_id} ({data['speed_kmh']:.1f} km/h)",
            "timestamp": data["timestamp"]
        }
        producer.send(OUTPUT_TOPIC, alert_payload)
        producer.flush()
        print(f"✅ [ALERT END] Incident resolved on {sensor_id}.")

    sensor_states[sensor_id] = current_status