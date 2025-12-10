import time
import json
import random
import paho.mqtt.client as mqtt
from datetime import datetime

# --- CONFIGURAZIONE ---
BROKER_ADDRESS = "localhost"  # Se Mosquitto gira sul tuo pc
TOPIC = "traffic/sensor1"
SENSOR_ID = "S-101"
# Coordinate (Esempio: Centro di Roma)
LAT = 41.9028
LON = 12.4964

# --- CONNESSIONE MQTT ---
client = mqtt.Client(client_id="Python_Simulator")
try:
    client.connect(BROKER_ADDRESS, 1883)
    print(f"✅ Connesso al Broker MQTT su {BROKER_ADDRESS}")
except Exception as e:
    print(f"❌ Errore connessione Mosquitto: {e}")
    exit(1)

client.loop_start()

# --- STATO INIZIALE ---
current_speed = 50.0  # Velocità iniziale
is_accident = False   # Stato incidente

print("\n--- AVVIO SIMULATORE TRAFFICO ---")
print("Il sensore invia dati ogni secondo.")
print("Premi 'CTRL+C' per fermare.")
print("Per simulare un incidente, cambieremo la velocità dinamicamente.\n")

try:
    while True:
        # 1. Logica di Simulazione
        if not is_accident:
            # Traffico normale: oscilla tra 40 e 60 km/h
            current_speed = random.uniform(40.0, 60.0)
            status = "FLOWING"
        else:
            # Incidente: velocità scende a 0
            current_speed = max(0, current_speed - 10) # Decelera fino a 0
            status = "BLOCKED"

        # 2. Creazione del Payload (Il pacchetto JSON)
        payload = {
            "sensor_id": SENSOR_ID,
            "timestamp": datetime.utcnow().isoformat(),
            "location": {"lat": LAT, "lon": LON},
            "speed_kmh": round(current_speed, 2),
            "status": status
        }

        # 3. Invio (Publish)
        # Questo simula l'invio via rete dal palo della luce al gateway
        client.publish(TOPIC, json.dumps(payload))
        
        print(f"📡 Inviato: {payload['speed_kmh']} km/h | Status: {status}")

        # 4. Input Utente per Simulazione "What-If"
        # Per rendere la demo interattiva, usiamo un file o un timer. 
        # Ma per ora, facciamo che dopo 20 cicli (o se vuoi gestirlo tu) simula il crash.
        # Qui simuliamo un crash randomico con probabilità bassa, 
        # OPPURE puoi modificare questo script per leggere un tasto.
        
        # ESEMPIO SEMPLICE: Ogni 1 secondo invia un dato.
        time.sleep(1)

except KeyboardInterrupt:
    # Se premi CTRL+C, simula l'incidente prima di chiudere? 
    # O semplicemente chiudi.
    print("\n🛑 Simulazione interrotta.")
    client.loop_stop()
    client.disconnect()