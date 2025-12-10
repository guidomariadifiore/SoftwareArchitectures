import json
from kafka import KafkaConsumer, KafkaProducer

# --- CONFIGURAZIONE ---
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

# --- MEMORIA DI STATO ---
# Qui salviamo l'ultimo stato noto di ogni sensore.
# Esempio: { "S-101": "FLOWING", "S-102": "BLOCKED" }
sensor_states = {}

print(f"🕵️ [SMART DETECTOR] In ascolto su '{INPUT_TOPIC}' con gestione stati...")

# --- MAIN LOOP ---
for message in consumer:
    data = message.value
    sensor_id = data["sensor_id"]
    current_status = data.get("status", "FLOWING") # Default FLOWING se manca il campo
    
    # Recuperiamo lo stato precedente (Se non esiste, assumiamo che fosse FLOWING)
    previous_status = sensor_states.get(sensor_id, "FLOWING")
    
    # LOGICA A TRANSIZIONI (EDGE DETECTION)
    
    # CASO 1: Inizio Incidente (Da FLOWING a BLOCKED)
    if current_status == "BLOCKED" and previous_status == "FLOWING":
        
        alert_payload = {
            "type": "TRAFFIC_JAM_STARTED",
            "sensor_id": sensor_id,
            "severity": "HIGH",
            "message": f"🔴 INIZIO BLOCCO: Rilevato traffico fermo ({data['speed_kmh']} km/h)",
            "timestamp": data["timestamp"]
        }
        producer.send(OUTPUT_TOPIC, alert_payload)
        print(f"🚨 [ALERT START] Nuovo incidente su {sensor_id}!")

    # CASO 2: Fine Incidente (Da BLOCKED a FLOWING)
    elif current_status == "FLOWING" and previous_status == "BLOCKED":
        
        alert_payload = {
            "type": "TRAFFIC_JAM_RESOLVED",
            "sensor_id": sensor_id,
            "severity": "INFO",
            "message": f"🟢 RISOLTO: Il traffico ha ripreso a scorrere ({data['speed_kmh']} km/h)",
            "timestamp": data["timestamp"]
        }
        producer.send(OUTPUT_TOPIC, alert_payload)
        print(f"✅ [ALERT END] Incidente risolto su {sensor_id}.")

    # CASO 3: Nessun cambiamento (BLOCKED->BLOCKED o FLOWING->FLOWING)
    else:
        # Non facciamo nulla per non spammare il topic alerts
        pass

    # Aggiorniamo la memoria con lo stato attuale per il prossimo giro
    sensor_states[sensor_id] = current_status