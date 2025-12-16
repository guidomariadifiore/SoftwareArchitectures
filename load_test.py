import threading
import time
import json
import random
import os
import paho.mqtt.client as mqtt

# --- CONFIGURATION ---
TOTAL_SENSORS = 5000     
WORKER_THREADS = 50      
EVENTS_PER_MIN = 100000  
BROKER_ADDRESS = "localhost"
TOPIC = "raw-traffic"
COMMAND_FILE = "command.txt"

# Physics Constants (Same as smart_gateway.py)
ACCELERATION_RATE = 3.0   
DECELERATION_RATE = 8.0   
BLOCKAGE_THRESHOLD = 5.0  

# Global Command State
# Format: {"target_id": "GW-BATCH-X", "mode": "CRASH"} or None
current_command = {"target_id": None, "mode": "NORMAL"}

# --- COMMAND LISTENER THREAD ---
def command_listener():
    """Reads command.txt every second to update simulation state"""
    last_known = ""
    print(f"📄 Listening for commands in '{COMMAND_FILE}'...")
    print("👉 Format: 'CRASH GW-BATCH-10' or 'NORMAL'")
    
    while True:
        try:
            if os.path.exists(COMMAND_FILE):
                with open(COMMAND_FILE, "r") as f:
                    content = f.read().strip()
                
                if content != last_known:
                    parts = content.split()
                    mode = parts[0].upper()
                    
                    if mode == "CRASH" and len(parts) > 1:
                        target = parts[1]
                        current_command["target_id"] = target
                        current_command["mode"] = "CRASH"
                        print(f"\n⚠️  COMMAND RECEIVED: CRASH target set to {target}")
                    elif mode == "NORMAL":
                        current_command["target_id"] = None
                        current_command["mode"] = "NORMAL"
                        print(f"\n✅ COMMAND RECEIVED: All systems NORMAL")
                    
                    last_known = content
        except Exception as e:
            print(f"Command read error: {e}")
        
        time.sleep(1)

# --- WORKER THREAD ---
def worker_task(worker_id):
    # 1. Setup Sensors
    sensors_per_worker = TOTAL_SENSORS // WORKER_THREADS
    start_id = worker_id * sensors_per_worker
    end_id = start_id + sensors_per_worker
    
    # Initialize state for my batch of sensors
    # sensor_state = { "id": current_speed }
    sensor_states = {}
    for i in range(start_id, end_id):
        s_id = f"GW-BATCH-{i}"
        sensor_states[s_id] = random.uniform(40.0, 60.0) # Start with random normal speed

    # 2. Setup MQTT
    client = mqtt.Client(client_id=f"LoadWorker-{worker_id}")
    try:
        client.connect(BROKER_ADDRESS, 1883)
        client.loop_start()
    except:
        return

    # 3. Calculate Timing
    # Target rate: 100k/min total -> ~33 msg/sec per worker
    target_events_per_sec = EVENTS_PER_MIN / 60
    events_per_worker_sec = target_events_per_sec / WORKER_THREADS
    delay = 1.0 / events_per_worker_sec

    while True:
        # Pick a random sensor to update
        # (Optimized: We don't update ALL sensors every loop, we pick one randomly 
        # to simulate the aggregate stream of data)
        s_id = random.choice(list(sensor_states.keys()))
        current_speed = sensor_states[s_id]
        
        # --- PHYSICS ENGINE ---
        target_speed = random.uniform(40.0, 60.0) # Default normal flow

        # Check for CRASH command
        if current_command["mode"] == "CRASH" and current_command["target_id"] == s_id:
            target_speed = 0.0

        # Apply Acceleration/Deceleration
        if current_speed > target_speed:
            current_speed -= DECELERATION_RATE
            if current_speed < target_speed: current_speed = target_speed
        elif current_speed < target_speed:
            current_speed += ACCELERATION_RATE
            if current_speed > target_speed: current_speed = target_speed
        
        current_speed = max(0.0, current_speed)
        sensor_states[s_id] = current_speed # Update memory

        # Determine Status
        status = "BLOCKED" if current_speed < BLOCKAGE_THRESHOLD else "FLOWING"

        # --- SEND PAYLOAD ---
        payload = {
            "sensor_id": s_id,
            "timestamp": time.time(),
            "location": {"lat": 41.90, "lon": 12.50},
            "speed_kmh": round(current_speed, 2),
            "status": status,
            "buffered": False
        }
        
        client.publish(TOPIC, json.dumps(payload))
        time.sleep(delay)

# --- MAIN ---
# Start Command Listener
cmd_thread = threading.Thread(target=command_listener, daemon=True)
cmd_thread.start()

# Start Workers
print(f"🚀 STARTING PHYSICS SIMULATION")
print(f"🎯 Controlling {TOTAL_SENSORS} sensors. Write to 'command.txt' to interact.")

for i in range(WORKER_THREADS):
    t = threading.Thread(target=worker_task, args=(i,))
    t.daemon = True
    t.start()
    time.sleep(0.05)

try:
    while True: time.sleep(1)
except KeyboardInterrupt:
    print("Stopping...")