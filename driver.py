import time
import random
import paho.mqtt.client as mqtt
from machines import Machine  


# === MQTT Broker Configuration ===
MQTT_BROKER_ADDRESS = "localhost"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
MQTT_TOPIC_PREFIX = "shopfloor/machine"


# === Initialize and Connect MQTT Client ===
mqtt_client = mqtt.Client()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Connected successfully to broker.")
    else:
        print(f"[MQTT] Connection failed with code {rc}.")


mqtt_client.on_connect = on_connect
mqtt_client.connect(MQTT_BROKER_ADDRESS, MQTT_PORT, MQTT_KEEPALIVE)
mqtt_client.loop_start()


# === Define Machines to Simulate ===
machine_types = [
    {"class_name": "A", "temp_base": 40, "temp_threshold": 85, "vib_base": 2, "vib_threshold": 8, "repair_time": 3},
    {"class_name": "B", "temp_base": 50, "temp_threshold": 90, "vib_base": 4, "vib_threshold": 12, "repair_time": 5},
    {"class_name": "C", "temp_base": 30, "temp_threshold": 80, "vib_base": 3, "vib_threshold": 10, "repair_time": 4}
]


machines = []
# Create two machines per type for example
for mt in machine_types:
    for idx in range(2):
        machine_id = f"{mt['class_name']}_{idx+1}"
        machines.append(Machine(
            class_name=mt["class_name"],
            machine_id=machine_id,
            temp_base=mt["temp_base"],
            temp_threshold=mt["temp_threshold"],
            vib_base=mt["vib_base"],
            vib_threshold=mt["vib_threshold"],
            repair_time=mt["repair_time"]
        ))


# Job Load Definitions (Will be changed once sreepathy's code is committed)
job_types = [
    {"name": "Light Load", "temp_increment": 5, "vib_increment": 1},
    {"name": "Heavy Load", "temp_increment": 8, "vib_increment": 2},
    {"name": "Moderate Load", "temp_increment": 4, "vib_increment": 1.5},
    {"name": "Stressful Load", "temp_increment": 6, "vib_increment": 2.5}
]


TOTAL_TIMESTEPS = 30  

#will be modified according to the designs made by preethi
try:
    for timestep in range(1, TOTAL_TIMESTEPS + 1):
        # Simulate receiving a job from an external system
        incoming_job = random.choice(job_types)
        print(f"\n[Timestep {timestep}] New job type: {incoming_job['name']}")

        # Process job on each machine and publish the status to MQTT broker
        for machine in machines:
            machine.process_job(incoming_job["temp_increment"], incoming_job["vib_increment"])
            topic = f"{MQTT_TOPIC_PREFIX}/{machine.machine_id}"
            status_message = machine.get_status(timestep)
            mqtt_client.publish(topic, status_message)
            print(f"Published to topic '{topic}': {status_message}")

        # Wait before next timestep
        time.sleep(1)

finally:
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("[MQTT] Disconnected gracefully.")
