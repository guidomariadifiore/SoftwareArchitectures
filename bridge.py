import json
import time
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# --- CONFIGURAZIONE ---
MQTT_BROKER = "localhost"
MQTT_TOPIC = "raw-traffic"
KAFKA_BROKER = "localhost:29092" # Porta esterna di Redpanda/Kafka
KAFKA_TOPIC = "raw-traffic"

# --- SETUP KAFKA PRODUCER ---
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"✅ [BRIDGE] Connesso a Kafka su {KAFKA_BROKER}")
except Exception as e:
    print(f"❌ [BRIDGE] Errore connessione Kafka: {e}")
    exit(1)

# --- CALLBACKS MQTT ---
def on_connect(client, userdata, flags, rc):
    print(f"✅ [BRIDGE] Connesso a Mosquitto. In ascolto su '{MQTT_TOPIC}'...")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        # 1. Ricevi da MQTT
        payload = json.loads(msg.payload.decode())
        
        # 2. Invia a Kafka (Ingestion)
        # Qui in un sistema reale contatteremmo lo Schema Registry
        future = producer.send(KAFKA_TOPIC, payload)
        future.get(timeout=10) # Aspetta conferma (ACK) da Kafka
        
        print(f"➡️ [MQTT -> KAFKA] Spostato pacchetto: {payload['speed_kmh']} km/h")
    except Exception as e:
        print(f"❌ Errore nel bridging: {e}")

# --- AVVIO BRIDGE ---
client = mqtt.Client(client_id="Bridge_Service")
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, 1883)
client.loop_forever()