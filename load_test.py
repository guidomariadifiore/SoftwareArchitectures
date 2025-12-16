import threading
import time
import json
import random
import paho.mqtt.client as mqtt

# --- CONFIGURATION ---
TOTAL_SENSORS = 5000     # Total unique sensors to simulate
WORKER_THREADS = 50      # Number of real concurrent connections (Batching)
EVENTS_PER_MIN = 100000  # Target throughput
BROKER_ADDRESS = "localhost"
TOPIC = "raw-traffic"

# Sensors per worker
SENSORS_PER_WORKER = TOTAL_SENSORS // WORKER_THREADS

# Calculate delay
# We need 100,000 messages per minute total.
# That is ~1,666 messages per second.
# With 50 workers, each worker must send ~33 messages per second.
target_events_per_sec = EVENTS_PER_MIN / 60
events_per_worker_sec = target_events_per_sec / WORKER_THREADS
delay_per_worker = 1.0 / events_per_worker_sec

print(f"🚀 STARTING OPTIMIZED LOAD TEST")
print(f"🎯 Simulating {TOTAL_SENSORS} Sensors using {WORKER_THREADS} active connections.")
print(f"⚡ Target Rate: ~{int(target_events_per_sec)} msg/sec total")

def worker_task(worker_id):
    # Each worker simulates a batch of sensors
    start_id = worker_id * SENSORS_PER_WORKER
    end_id = start_id + SENSORS_PER_WORKER
    my_sensors = [f"GW-BATCH-{i}" for i in range(start_id, end_id)]
    
    client = mqtt.Client(client_id=f"LoadWorker-{worker_id}")
    try:
        client.connect(BROKER_ADDRESS, 1883)
        # We don't need loop_start() if we are just publishing in a loop
        # But loop_start handles reconnection, so we keep it.
        client.loop_start()
    except Exception as e:
        print(f"❌ Worker {worker_id} failed to connect: {e}")
        return

    while True:
        # Pick a random sensor from my batch to simulate
        # (Or iterate sequentially if you want strict round-robin)
        current_sensor = random.choice(my_sensors)

        payload = {
            "sensor_id": current_sensor,
            "timestamp": time.time(),
            "location": {"lat": 41.90, "lon": 12.50},
            "speed_kmh": random.uniform(0, 100),
            "status": "FLOWING", 
            "buffered": False
        }
        
        client.publish(TOPIC, json.dumps(payload))
        
        # Wait to match target rate
        time.sleep(delay_per_worker)

# --- SPAWN WORKERS ---
threads = []
print(f"🔥 Spawning {WORKER_THREADS} workers...")

for i in range(WORKER_THREADS):
    t = threading.Thread(target=worker_task, args=(i,))
    t.daemon = True
    t.start()
    threads.append(t)
    time.sleep(0.05) # Slight stagger to prevent connection spike

print("\n✅ Load Generator Running. Press Ctrl+C to stop.")
try:
    while True: time.sleep(1)
except KeyboardInterrupt:
    print("Stopping...")