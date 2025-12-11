import time
import json
import random
import os
from datetime import datetime
import paho.mqtt.client as mqtt

# --- CONFIGURATION ---
BROKER_ADDRESS = "localhost"
TOPIC = "raw-traffic"
SENSOR_ID = "GW-01-S-101"
COMMAND_FILE = "command.txt"

# --- GLOBAL VARIABLES ---
buffer = []             # Local memory buffer
is_connected = False    # Connection state

# --- MQTT CALLBACKS ---
def on_connect(client, userdata, flags, rc):
    global is_connected
    if rc == 0:
        is_connected = True
        print(f"✅ [NETWORK] Connected to MQTT Broker ({BROKER_ADDRESS})")
    else:
        print(f"❌ [NETWORK] Connection failed, code: {rc}")

def on_disconnect(client, userdata, rc):
    global is_connected
    is_connected = False
    print("⚠️ [NETWORK] Disconnected! Switching to local buffering...")

# --- CLIENT SETUP ---
client = mqtt.Client(client_id="Smart_Gateway_Python")
client.on_connect = on_connect
client.on_disconnect = on_disconnect

try:
    client.connect(BROKER_ADDRESS, 1883)
    client.loop_start()
except Exception as e:
    print(f"⚠️ [STARTUP] Broker not found ({e}). Starting in OFFLINE mode.")

# --- UTILITY FUNCTIONS ---
def get_simulation_mode():
    """Reads command.txt to determine user intent"""
    if not os.path.exists(COMMAND_FILE):
        return "NORMAL"
    try:
        with open(COMMAND_FILE, "r") as f:
            return f.read().strip().upper()
    except:
        return "NORMAL"

# --- MAIN LOOP ---
print(f"\n🚀 Smart Gateway started for Sensor {SENSOR_ID}")
print(f"📄 Control via '{COMMAND_FILE}' (write CRASH or NORMAL)")
print("----------------------------------------------------------------\n")

# Initial physics state
current_speed = 50.0
target_speed = 50.0
ACCELERATION_RATE = 3.0   # km/h gained per second (Traffic clearing up)
DECELERATION_RATE = 8.0   # km/h lost per second (Braking)
BLOCKAGE_THRESHOLD = 5.0  # Speed under which we consider it BLOCKED

try:
    while True:
        # 1. DETERMINE INTENT (Target Speed)
        mode = get_simulation_mode()
        
        if mode == "CRASH":
            target_speed = 0.0
        else:
            # Normal traffic fluctuates between 40 and 60
            target_speed = random.uniform(40.0, 60.0)

        # 2. APPLY PHYSICS (Gradual Change)
        if current_speed > target_speed:
            # Decelerating (Braking)
            current_speed -= DECELERATION_RATE
            if current_speed < target_speed: current_speed = target_speed # Clamp
        elif current_speed < target_speed:
            # Accelerating (Restarting)
            current_speed += ACCELERATION_RATE
            if current_speed > target_speed: current_speed = target_speed # Clamp

        # Ensure speed never goes below 0
        current_speed = max(0.0, current_speed)

        # 3. DERIVE STATUS FROM REALITY (Not from command)
        # The status is BLOCKED only if the cars are actually stopped/crawling.
        if current_speed < BLOCKAGE_THRESHOLD:
            status = "BLOCKED"
        else:
            status = "FLOWING"

        # 4. CREATE PAYLOAD
        payload = {
            "sensor_id": SENSOR_ID,
            "timestamp": datetime.utcnow().isoformat(),
            "location": {"lat": 41.90, "lon": 12.50},
            "speed_kmh": round(current_speed, 2),
            "status": status,
            "buffered": False
        }
        
        json_payload = json.dumps(payload)

        # 5. SMART GATEWAY LOGIC (Resilience)
        if is_connected:
            # PHASE A: Flush Buffer
            if len(buffer) > 0:
                print(f"🔄 [RECOVERY] Resending {len(buffer)} buffered messages...")
                for old_msg in buffer:
                    old_data = json.loads(old_msg)
                    old_data["buffered"] = True 
                    client.publish(TOPIC, json.dumps(old_data))
                    time.sleep(0.05)
                buffer.clear()
                print("✅ [RECOVERY] Buffer cleared.")

            # PHASE B: Send Real-Time Data
            client.publish(TOPIC, json_payload)
            
            # Print output with arrows to show acceleration/deceleration
            trend = "=="
            if mode == "CRASH" and current_speed > 0: trend = "⏬"
            if mode == "NORMAL" and current_speed < 40: trend = "⏫"
            
            print(f"📡 [LIVE] Speed: {payload['speed_kmh']:05.2f} km/h {trend} | Status: {status}")
        
        else:
            # PHASE C: Offline Mode
            buffer.append(json_payload)
            print(f"💾 [BUFFER] Broker Offline. Saved locally. (Size: {len(buffer)})")

        # Sampling rate (1 second)
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Gateway stopped.")
    client.loop_stop()
    client.disconnect()