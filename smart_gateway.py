import time
import json
import random
import os
from datetime import datetime
import paho.mqtt.client as mqtt

# --- CONFIGURAZIONE ---
BROKER_ADDRESS = "localhost"  # Cambia con l'IP di Mosquitto se non è locale
TOPIC = "raw-traffic"         # Topic dove scrive il gateway (Ingestion Layer)
SENSOR_ID = "GW-01-S-101"     # ID del sensore/gateway
COMMAND_FILE = "command.txt"  # File per controllare la simulazione in tempo reale

# --- VARIABILI GLOBALI ---
buffer = []             # La memoria locale (Buffer)
is_connected = False    # Stato della connessione

# --- CALLBACKS MQTT ---
def on_connect(client, userdata, flags, rc):
    global is_connected
    if rc == 0:
        is_connected = True
        print(f"✅ [NETWORK] Connesso al Broker MQTT ({BROKER_ADDRESS})")
    else:
        print(f"❌ [NETWORK] Connessione fallita, codice: {rc}")

def on_disconnect(client, userdata, rc):
    global is_connected
    is_connected = False
    print("⚠️ [NETWORK] Disconnesso dal Broker! Attivazione Buffering locale...")

# --- SETUP CLIENT ---
client = mqtt.Client(client_id="Smart_Gateway_Python")
client.on_connect = on_connect
client.on_disconnect = on_disconnect

# Tentativo di connessione iniziale (non bloccante per la demo)
try:
    client.connect(BROKER_ADDRESS, 1883)
    client.loop_start() # Avvia il thread di rete in background
except Exception as e:
    print(f"⚠️ [STARTUP] Broker non trovato ({e}). Si parte in modalità OFFLINE.")

# --- FUNZIONI DI UTILITÀ ---
def get_simulation_mode():
    """Legge il file command.txt per sapere se simulare un incidente"""
    if not os.path.exists(COMMAND_FILE):
        return "NORMAL"
    try:
        with open(COMMAND_FILE, "r") as f:
            return f.read().strip().upper()
    except:
        return "NORMAL"

# --- MAIN LOOP (IL CUORE DEL GATEWAY) ---
print(f"\n🚀 Smart Gateway avviato per Sensore {SENSOR_ID}")
print(f"📄 Controlla '{COMMAND_FILE}' per cambiare stato (scrivi CRASH o NORMAL)")
print("----------------------------------------------------------------\n")

current_speed = 50.0

try:
    while True:
        # 1. GENERAZIONE DATI (Simulazione Fisica)
        mode = get_simulation_mode()
        
        if mode == "CRASH":
            # Decelera rapidamente fino a 0
            current_speed = max(0, current_speed - 15)
            status = "BLOCKED"
        else:
            # Traffico normale con leggera oscillazione
            current_speed = random.uniform(40.0, 60.0)
            status = "FLOWING"

        # Creazione del pacchetto dati (Payload)
        payload = {
            "sensor_id": SENSOR_ID,
            "timestamp": datetime.utcnow().isoformat(),
            "location": {"lat": 41.90, "lon": 12.50},
            "speed_kmh": round(current_speed, 2),
            "status": status,
            "buffered": False # Flag per indicare se è dati live o storico
        }
        
        json_payload = json.dumps(payload)

        # 2. LOGICA SMART GATEWAY (Resilienza)
        if is_connected:
            # FASE A: Svuotamento Buffer (Se c'erano dati salvati mentre eravamo offline)
            if len(buffer) > 0:
                print(f"🔄 [RECOVERY] Rinvio {len(buffer)} messaggi bufferizzati al Cloud...")
                # Inviamo tutti i messaggi accumulati
                for old_msg in buffer:
                    # Segniamo che questi dati sono "vecchi" ma recuperati
                    old_data = json.loads(old_msg)
                    old_data["buffered"] = True 
                    client.publish(TOPIC, json.dumps(old_data))
                    time.sleep(0.05) # Piccolo delay per non intasare tutto all'istante
                
                buffer.clear() # Svuota il buffer
                print("✅ [RECOVERY] Buffer svuotato. Sincronizzazione completata.")

            # FASE B: Invio Dato Real-Time
            client.publish(TOPIC, json_payload)
            print(f"📡 [LIVE] Inviato: {payload['speed_kmh']} km/h | Status: {status}")
        
        else:
            # FASE C: Modalità Offline (Salvataggio nel Buffer)
            buffer.append(json_payload)
            print(f"💾 [BUFFER] Broker Offline. Messaggio salvato localmente. (Size: {len(buffer)})")

        # Frequenza di campionamento (1 secondo)
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Gateway arrestato.")
    client.loop_stop()
    client.disconnect()