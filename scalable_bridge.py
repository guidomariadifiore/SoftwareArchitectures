import json
import time
import uuid
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# --- CONFIGURATION ---
MQTT_BROKER = "localhost"
MQTT_TOPIC = "raw-traffic"
# Shared Subscription Syntax: $share/<group_name>/<topic>
# This tells Mosquitto: "Distribute messages among us, don't duplicate!"
SHARED_TOPIC = f"$share/bridge-group/{MQTT_TOPIC}"

KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC = "raw-traffic"

# --- SETUP KAFKA PRODUCER ---
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=5,    # Optimize: Batch messages slightly for speed
        batch_size=16384
    )
    print(f"✅ [BRIDGE] Kafka Connected.")
except Exception as e:
    print(f"❌ [BRIDGE] Kafka Error: {e}")
    exit(1)

# --- CALLBACKS ---
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"✅ Connected to Mosquitto. Joined Shared Group: 'bridge-group'")
    client.subscribe(SHARED_TOPIC)

def on_message(client, userdata, msg):
    try:
        # Fast forward: Decode & Send
        payload = json.loads(msg.payload.decode())
        producer.send(KAFKA_TOPIC, payload)
        # No print() here - it slows us down!
    except Exception:
        pass

# --- MAIN ---
# We MUST use MQTTv5 for Shared Subscriptions
client_id = f"Bridge-{uuid.uuid4().hex[:6]}"
client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5)

client.on_connect = on_connect
client.on_message = on_message

print(f"🚀 Starting Bridge Worker: {client_id}")
client.connect(MQTT_BROKER, 1883)
client.loop_forever()