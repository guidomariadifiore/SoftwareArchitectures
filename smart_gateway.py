import time
import json
import random
import os
from datetime import datetime
import paho.mqtt.client as mqtt

# --- CONFIG ---
BROKER_ADDRESS = "localhost"  # Change to Mosquitto's IP if it is not local
TOPIC = "raw-traffic"         # Topic where the gateway writes (Ingestion Layer)
SENSOR_ID = "GW-01-S-101"     # Sensor/gateway ID
COMMAND_FILE = "command.txt"  # File for controlling the simulation in real time

# --- GLOBAL VARIABLES ---
buffer = []             # Local memory (Buffer)
is_connected = False    # Connection status

# --- CALLBACKS MQTT ---
def on_connect(client, userdata, flags, rc):
    global is_connected
    if rc == 0:
        is_connected = True
        print(f"✅ [NETWORK] Connected to the MQTT Broker ({BROKER_ADDRESS})")
    else:
        print(f"❌ [NETWORK] Connection failed, code: {rc}")

def on_disconnect(client, userdata, rc):
    global is_connected
    is_connected = False
    print("⚠️ [NETWORK] Disconnected from Broker! Local Buffering Activation...")

# --- SETUP CLIENT ---
client = mqtt.Client(client_id="Smart_Gateway_Python")
client.on_connect = on_connect
client.on_disconnect = on_disconnect

# Initial connection attempt (non-blocking for the demo)
try:
    client.connect(BROKER_ADDRESS, 1883)
    client.loop_start() # Start the network thread in the background
except Exception as e:
    print(f"⚠️ [STARTUP] Broker not found ({e}). Starting in OFFLINE mode.")

# --- UTILITY FUNCTIONS ---
def get_simulation_mode():
    """Reads the command.txt file to determine whether to simulate an accident."""
    if not os.path.exists(COMMAND_FILE):
        return "NORMAL"
    try:
        with open(COMMAND_FILE, "r") as f:
            return f.read().strip().upper()
    except:
        return "NORMAL"

# --- MAIN LOOP (THE HEART OF THE GATEWAY) ---
print(f"\n🚀 Smart Gateway started for Sensor {SENSOR_ID}")
print(f"📄 Check '{COMMAND_FILE}' to change status (write CRASH or NORMAL)")
print("----------------------------------------------------------------\n")

current_speed = 50.0

try:
    while True:
        # 1. DATA GENERATION (Physical Simulation)
        mode = get_simulation_mode()
        
        if mode == "CRASH":
            # Decelerate rapidly to 0
            current_speed = max(0, current_speed - 15)
            status = "BLOCKED"
        else:
            # Normal traffic with slight fluctuations
            current_speed = random.uniform(40.0, 60.0)
            status = "FLOWING"

        # Creation of the data package (Payload)
        payload = {
            "sensor_id": SENSOR_ID,
            "timestamp": datetime.utcnow().isoformat(),
            "location": {"lat": 41.90, "lon": 12.50},
            "speed_kmh": round(current_speed, 2),
            "status": status,
            "buffered": False # Flag to indicate whether data is live or historical
        }
        
        json_payload = json.dumps(payload)

        # 2. SMART GATEWAY LOGIC (Resilience)
        if is_connected:
            # PHASE A: Emptying the Buffer (If there was data saved while we were offline)
            if len(buffer) > 0:
                print(f"🔄 [RECOVERY] Sending {len(buffer)} buffered messages to the Cloud...")
                # We send all accumulated messages
                for old_msg in buffer:
                    # note that these data are ‘old’ but have been recovered.
                    old_data = json.loads(old_msg)
                    old_data["buffered"] = True 
                    client.publish(TOPIC, json.dumps(old_data))
                    time.sleep(0.05) # Small delay to avoid clogging everything up instantly
                
                buffer.clear() # Clear the buffer
                print("✅ [RECOVERY] Buffer emptied. Synchronisation complete..")

            # PHASE B: Real-Time Data Transmission
            client.publish(TOPIC, json_payload)
            print(f"📡 [LIVE] Sent: {payload['speed_kmh']} km/h | Status: {status}")
        
        else:
            # PHASE C: Offline Mode (Buffer Saving)
            buffer.append(json_payload)
            print(f"💾 [BUFFER] Broker Offline. Message saved locally. (Size: {len(buffer)})")

        # Sampling frequency (1 second)
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Gateway arrested.")
    client.loop_stop()
    client.disconnect()