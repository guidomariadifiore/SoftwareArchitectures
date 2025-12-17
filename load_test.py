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

# Physics Constants
ACCELERATION_RATE = 3.0   
DECELERATION_RATE = 8.0   
BLOCKAGE_THRESHOLD = 5.0  

# Global Shared State
current_command = {"target_id": None, "mode": "NORMAL"}
# Track stats: [ { "connected": False, "buffer_size": 0 }, ... ]
worker_stats = [{"connected": False, "buffer_size": 0} for _ in range(WORKER_THREADS)]

# --- MONITORING THREAD (UPDATED FOR VISUALS) ---
def monitor_system():
    print(f"📊 MONITOR: Tracking {WORKER_THREADS} workers...")
    
    last_buffer_total = 0
    
    while True:
        current_buffer_total = sum(w["buffer_size"] for w in worker_stats)
        active_workers = sum(1 for w in worker_stats if w["connected"])
        
        # CASE A: CRITICAL - BUFFERING
        if current_buffer_total > 0 and current_buffer_total >= last_buffer_total:
             print(f"⚠️  [NETWORK DOWN] Buffered Events: {current_buffer_total} | Active Workers: {active_workers}/{WORKER_THREADS}")
        
        # CASE B: RECOVERY - DRAINING
        elif current_buffer_total > 0 and current_buffer_total < last_buffer_total:
            drained = last_buffer_total - current_buffer_total
            print(f"♻️  [RECOVERING] Flushing Buffer... ({drained} sent now) | Remaining: {current_buffer_total}")

        # CASE C: SUCCESS - JUST FINISHED
        elif current_buffer_total == 0 and last_buffer_total > 0:
            print(f"✅ [RECOVERY COMPLETE] All {last_buffer_total} buffered events sent! System Nominal.")

        last_buffer_total = current_buffer_total
        time.sleep(1)

# --- COMMAND LISTENER ---
def command_listener():
    last_known = ""
    while True:
        try:
            if os.path.exists(COMMAND_FILE):
                with open(COMMAND_FILE, "r") as f:
                    content = f.read().strip()
                if content != last_known:
                    parts = content.split()
                    mode = parts[0].upper()
                    if mode == "CRASH" and len(parts) > 1:
                        current_command["target_id"] = parts[1]
                        current_command["mode"] = "CRASH"
                        print(f"\n💥 COMMAND: CRASH {parts[1]}")
                    elif mode == "NORMAL":
                        current_command["target_id"] = None
                        current_command["mode"] = "NORMAL"
                        print(f"\n✅ COMMAND: NORMAL")
                    last_known = content
        except: pass
        time.sleep(1)

# --- WORKER THREAD ---
def worker_task(worker_id):
    # 1. Local State
    sensors_per_worker = TOTAL_SENSORS // WORKER_THREADS
    start_id = worker_id * sensors_per_worker
    end_id = start_id + sensors_per_worker
    
    my_sensors = {}
    for i in range(start_id, end_id):
        my_sensors[f"GW-BATCH-{i}"] = random.uniform(40.0, 60.0)

    local_buffer = []
    
    # 2. MQTT Callbacks
    def on_connect(client, userdata, flags, rc):
        if rc == 0: 
            worker_stats[worker_id]["connected"] = True
            # Debug log only for Worker 0 to avoid spam
            if worker_id == 0: print(f"⚡ [WORKER 0] Reconnected to Broker!")

    def on_disconnect(client, userdata, rc):
        worker_stats[worker_id]["connected"] = False
        if worker_id == 0: print(f"🔌 [WORKER 0] Connection Lost! Code: {rc}")

    client = mqtt.Client(client_id=f"LoadWorker-{worker_id}")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    
    try:
        # keepalive=5 detects failure in 5 seconds instead of 60
        client.connect(BROKER_ADDRESS, 1883, keepalive=5) 
        client.loop_start()
    except:
        return 

    # 3. Timing
    target_events_per_sec = EVENTS_PER_MIN / 60
    events_per_worker_sec = target_events_per_sec / WORKER_THREADS
    delay = 1.0 / events_per_worker_sec

    while True:
        # Stats Update
        worker_stats[worker_id]["buffer_size"] = len(local_buffer)

        # Simulation Logic
        s_id = random.choice(list(my_sensors.keys()))
        current_speed = my_sensors[s_id]
        
        target_speed = random.uniform(40.0, 60.0)
        if current_command["mode"] == "CRASH" and current_command["target_id"] == s_id:
            target_speed = 0.0

        if current_speed > target_speed:
            current_speed -= DECELERATION_RATE
            if current_speed < target_speed: current_speed = target_speed
        elif current_speed < target_speed:
            current_speed += ACCELERATION_RATE
            if current_speed > target_speed: current_speed = target_speed
        
        current_speed = max(0.0, current_speed)
        my_sensors[s_id] = current_speed
        status = "BLOCKED" if current_speed < BLOCKAGE_THRESHOLD else "FLOWING"

        payload = {
            "sensor_id": s_id,
            "timestamp": time.time(),
            "location": {"lat": 41.90, "lon": 12.50},
            "speed_kmh": round(current_speed, 2),
            "status": status,
            "buffered": False
        }
        json_payload = json.dumps(payload)

        # --- RELIABILITY LOGIC ---
        if worker_stats[worker_id]["connected"]:
            # PHASE A: Flush Buffer
            if len(local_buffer) > 0:
                # Send ALL buffered messages immediately
                for old_msg in local_buffer:
                    old_data = json.loads(old_msg)
                    old_data["buffered"] = True
                    client.publish(TOPIC, json.dumps(old_data))
                local_buffer.clear()
            
            # PHASE B: Send Live
            client.publish(TOPIC, json_payload)
        else:
            # PHASE C: Buffer
            local_buffer.append(json_payload)

        time.sleep(delay)

# --- STARTUP ---
threading.Thread(target=command_listener, daemon=True).start()
threading.Thread(target=monitor_system, daemon=True).start()

print(f"🚀 RELIABLE LOAD TEST STARTED (Fast-Detect Mode)")
print(f"🎯 {TOTAL_SENSORS} Sensors | {WORKER_THREADS} Workers")

for i in range(WORKER_THREADS):
    t = threading.Thread(target=worker_task, args=(i,))
    t.daemon = True
    t.start()
    time.sleep(0.02)

try:
    while True: time.sleep(1)
except KeyboardInterrupt:
    print("Stopping...")