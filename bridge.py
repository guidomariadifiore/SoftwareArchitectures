import json
import time
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# --- CONFIGURATION ---
MQTT_BROKER = "localhost"
MQTT_TOPIC = "raw-traffic"
KAFKA_BROKER = "localhost:29092" # Redpanda/Kafka external door
KAFKA_TOPIC = "raw-traffic"

# --- SETUP KAFKA PRODUCER ---
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"✅ [BRIDGE] Connected to Kafka on {KAFKA_BROKER}")
except Exception as e:
    print(f"❌ [BRIDGE] Kafka connection error: {e}")
    exit(1)

# --- CALLBACKS MQTT ---
def on_connect(client, userdata, flags, rc):
    print(f"✅ [BRIDGE] Connected to Mosquitto. Listening on '{MQTT_TOPIC}'...")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        # 1. Receive from MQTT
        payload = json.loads(msg.payload.decode())
        
        # 2. Send to Kafka (Ingestion)
        # Here, in a real system, we would contact the Schema Registry.
        future = producer.send(KAFKA_TOPIC, payload)
        future.get(timeout=10) # Wait for confirmation (ACK) from Kafka
        
        #commentato per non floodare la console:
        #print(f"➡️ [MQTT -> KAFKA] Package moved: {payload['speed_kmh']} km/h")
    except Exception as e:
        print(f"❌ Bridging error: {e}")

# --- BRIDGE START ---
client = mqtt.Client(client_id="Bridge_Service")
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, 1883)
client.loop_forever()