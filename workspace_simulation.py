import time
import random
import json
import paho.mqtt.client as mqtt

from machines import Machine
from codes.jobs import Job

class WorkspaceSimulation:
    """
    Simulates a physical workspace with machines and sensor monitoring.
    Monitors sensor readings and sends abnormal readings to MQTT broker.
    """
    
    def __init__(self):
        # MQTT Configuration
        self.MQTT_BROKER_ADDRESS = "localhost"
        self.MQTT_PORT = 1883
        self.MQTT_KEEPALIVE = 60
        self.MQTT_TOPIC_PREFIX = "shopfloor/machine"
        self.MQTT_ALERT_TOPIC = "shopfloor/alerts"
        
        # Initialize MQTT client
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.connect(self.MQTT_BROKER_ADDRESS, self.MQTT_PORT, self.MQTT_KEEPALIVE)
        self.mqtt_client.loop_start()
        
        # Initialize machines and jobs
        self.machines = self.create_machines()
        self.jobs = self.create_jobs()
        self.current_jobs = {}  # Track which machine is processing which job
        self.completed_jobs = []
        self.timestep = 0
        
        # Random event tracking
        self.random_events = {
            'machine_failure': {'timestep': random.randint(8, 15), 'machine': random.choice(['A_1', 'B_1', 'C_1'])},
            'temperature_spike': {'timestep': random.randint(6, 12), 'machine': random.choice(['A_1', 'A_2', 'B_1'])},
            'high_load': {'timestep': random.randint(4, 10), 'machine': random.choice(['C_1', 'D_1'])}
        }
        
        print(f"Workspace Simulation initialized with {len(self.machines)} machines and {len(self.jobs)} jobs")
        print(f"Random events scheduled: {self.random_events}")
    
    def on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            print("[MQTT] Connected successfully to broker.")
        else:
            print(f"[MQTT] Connection failed with code {rc}.")
    
    def create_machines(self):
        """Create 5 machines of different classes"""
        machine_configs = [
            {"class_name": "A", "machine_id": "A_1", "temp_base": 40, "temp_threshold": 95, 
             "vib_base": 2, "vib_threshold": 12, "repair_time": 3},
            {"class_name": "A", "machine_id": "A_2", "temp_base": 42, "temp_threshold": 97, 
             "vib_base": 2.5, "vib_threshold": 13, "repair_time": 3},
            {"class_name": "B", "machine_id": "B_1", "temp_base": 50, "temp_threshold": 100, 
             "vib_base": 4, "vib_threshold": 16, "repair_time": 5},
            {"class_name": "C", "machine_id": "C_1", "temp_base": 30, "temp_threshold": 90, 
             "vib_base": 3, "vib_threshold": 14, "repair_time": 4},
            {"class_name": "D", "machine_id": "D_1", "temp_base": 35, "temp_threshold": 85, 
             "vib_base": 1.5, "vib_threshold": 10, "repair_time": 2}
        ]
        
        machines = []
        for config in machine_configs:
            machine = Machine(
                class_name=config["class_name"],
                machine_id=config["machine_id"],
                temp_base=config["temp_base"],
                temp_threshold=config["temp_threshold"],
                vib_base=config["vib_base"],
                vib_threshold=config["vib_threshold"],
                repair_time=config["repair_time"]
            )
            machines.append(machine)
        
        return machines
    
    def create_jobs(self):
        """Create 10 jobs with different machine requirements"""
        job_requirements = [
            ["A_1", "B_1"],           # Job 1: Requires A_1 and B_1
            ["A_2", "C_1"],           # Job 2: Requires A_2 and C_1
            ["B_1", "D_1"],           # Job 3: Requires B_1 and D_1
            ["A_1", "A_2", "C_1"],    # Job 4: Requires A_1, A_2, and C_1
            ["B_1", "C_1", "D_1"],    # Job 5: Requires B_1, C_1, and D_1
            ["A_1"],                  # Job 6: Requires only A_1
            ["A_2", "B_1"],           # Job 7: Requires A_2 and B_1
            ["C_1", "D_1"],           # Job 8: Requires C_1 and D_1
            ["A_1", "B_1", "C_1"],    # Job 9: Requires A_1, B_1, and C_1
            ["A_2", "D_1"]            # Job 10: Requires A_2 and D_1
        ]
        
        jobs = []
        for i, requirements in enumerate(job_requirements):
            job = Job(job_id=f"JOB_{i+1}", machine_requirement=requirements)
            jobs.append(job)
        
        return jobs
    
    def get_machine_by_id(self, machine_id):
        """Get machine object by ID"""
        for machine in self.machines:
            if machine.machine_id == machine_id:
                return machine
        return None
    
    def check_sensor_readings(self, machine):
        """Check if sensor readings are abnormal and send alerts"""
        alerts = []
        
        # Check temperature threshold
        if machine.temperature >= machine.temp_threshold:
            alert = {
                "type": "TEMPERATURE_THRESHOLD_EXCEEDED",
                "machine_id": machine.machine_id,
                "class_name": machine.class_name,
                "current_temperature": round(machine.temperature, 2),
                "threshold": machine.temp_threshold,
                "timestamp": self.timestep,
                "severity": "HIGH"
            }
            alerts.append(alert)
        
        # Check vibration threshold
        if machine.vibration >= machine.vib_threshold:
            alert = {
                "type": "VIBRATION_THRESHOLD_EXCEEDED",
                "machine_id": machine.machine_id,
                "class_name": machine.class_name,
                "current_vibration": round(machine.vibration, 2),
                "threshold": machine.vib_threshold,
                "timestamp": self.timestep,
                "severity": "HIGH"
            }
            alerts.append(alert)
        
        # Check if machine is not operational (failed)
        if not machine.operational:
            alert = {
                "type": "MACHINE_FAILURE",
                "machine_id": machine.machine_id,
                "class_name": machine.class_name,
                "temperature": round(machine.temperature, 2),
                "vibration": round(machine.vibration, 2),
                "repair_progress": f"{machine.repair_timer}/{machine.repair_time}",
                "timestamp": self.timestep,
                "severity": "CRITICAL"
            }
            alerts.append(alert)
        
        # Send alerts to MQTT
        for alert in alerts:
            self.send_alert(alert)
        
        return alerts
    
    def send_alert(self, alert):
        """Send alert to MQTT broker"""
        topic = f"{self.MQTT_ALERT_TOPIC}/{alert['machine_id']}"
        message = json.dumps(alert)
        self.mqtt_client.publish(topic, message)
        print(f"🚨 ALERT: {alert['type']} - Machine {alert['machine_id']}")
        print(f"   Published to topic '{topic}': {message}")
    
    def send_normal_reading(self, machine):
        """Send normal sensor reading to MQTT"""
        topic = f"{self.MQTT_TOPIC_PREFIX}/{machine.machine_id}"
        status_message = machine.get_status(self.timestep)
        self.mqtt_client.publish(topic, status_message)
        print(f"📊 Normal reading - Machine {machine.machine_id}: Temp={machine.temperature:.1f}°C, Vib={machine.vibration:.1f}")
    
    def simulate_machine_failure(self, machine_id):
        """Simulate a complete machine failure"""
        machine = self.get_machine_by_id(machine_id)
        if machine and machine.operational:
            # Force machine to fail by setting extreme values
            machine.temperature = machine.temp_threshold + 10
            machine.vibration = machine.vib_threshold + 5
            machine.operational = False
            print(f"💥 Simulating complete failure of machine {machine_id}")
    
    def assign_job_to_machine(self, job, machine_id):
        """Assign a job to a specific machine"""
        machine = self.get_machine_by_id(machine_id)
        if machine and machine.operational and machine_id not in self.current_jobs:
            self.current_jobs[machine_id] = job
            job.start()
            print(f"🔧 Job {job.job_id} assigned to machine {machine_id}")
            return True
        return False
    
    def complete_job_on_machine(self, machine_id):
        """Complete the job currently running on a machine"""
        if machine_id in self.current_jobs:
            job = self.current_jobs[machine_id]
            job.complete()
            self.completed_jobs.append(job)
            del self.current_jobs[machine_id]
            print(f"✅ Job {job.job_id} completed on machine {machine_id}")
    
    def process_timestep(self):
        """Process one timestep of the simulation"""
        self.timestep += 1
        print(f"\n{'='*60}")
        print(f"TIMESTEP {self.timestep}")
        print(f"{'='*60}")
        
        # Simulate random job processing on machines (genetic algorithm handles assignment)
        for machine in self.machines:
            # Random chance of machine processing a job (simulating genetic algorithm assignment)
            if random.random() < 0.4 and machine.operational:  # 40% chance of processing
                # Simulate job processing effects with smaller increments
                temp_increment = random.uniform(1, 4)  # Reduced from 3-8
                vib_increment = random.uniform(0.5, 2)  # Reduced from 1-3
                
                machine.process_job(temp_increment, vib_increment)
                print(f"🔧 Machine {machine.machine_id} processing job (Temp: +{temp_increment:.1f}°C, Vib: +{vib_increment:.1f})")
            else:
                # Machine is idle - gradual cooling
                machine.temperature = max(machine.temp_base, machine.temperature - 1.5)
                machine.vibration = max(machine.vib_base, machine.vibration - 0.3)
            
            # Check sensor readings and send alerts if abnormal
            alerts = self.check_sensor_readings(machine)
            
            # Send normal readings if no alerts
            if not alerts:
                self.send_normal_reading(machine)
        
        # Check for random events
        for event_type, event_data in self.random_events.items():
            if self.timestep == event_data['timestep']:
                if event_type == 'machine_failure':
                    self.simulate_machine_failure(event_data['machine'])
                elif event_type == 'temperature_spike':
                    machine = self.get_machine_by_id(event_data['machine'])
                    if machine and machine.operational:
                        spike_amount = random.uniform(8, 15)
                        machine.temperature += spike_amount
                        print(f"🔥 Temperature spike on machine {event_data['machine']}: +{spike_amount:.1f}°C")
                elif event_type == 'high_load':
                    machine = self.get_machine_by_id(event_data['machine'])
                    if machine and machine.operational:
                        load_temp = random.uniform(5, 10)
                        load_vib = random.uniform(2, 4)
                        machine.temperature += load_temp
                        machine.vibration += load_vib
                        print(f"⚡ High load on machine {event_data['machine']}: Temp +{load_temp:.1f}°C, Vib +{load_vib:.1f}")
        
        return True
    
    def run_simulation(self, max_timesteps=20):
        """Run the complete simulation"""
        print("🚀 Starting Workspace Simulation...")
        print(f"Machines: {[m.machine_id for m in self.machines]}")
        print(f"Jobs to process: {len(self.jobs)}")
        
        try:
            while self.timestep < max_timesteps:
                if not self.process_timestep():
                    break
                time.sleep(2)  # Wait 2 seconds between timesteps
        
        except KeyboardInterrupt:
            print("\n⏹️ Simulation interrupted by user")
        
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up MQTT connection"""
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        print("[MQTT] Disconnected gracefully.")
        
        # Print final statistics
        print(f"\n📊 SIMULATION SUMMARY:")
        print(f"   Total timesteps: {self.timestep}")
        print(f"   Jobs completed: {len(self.completed_jobs)}/{len(self.jobs)}")
        print(f"   Jobs in progress: {len(self.current_jobs)}")
        
        print(f"\n🔧 MACHINE STATUS:")
        for machine in self.machines:
            status = "Operational" if machine.operational else "Failed"
            print(f"   {machine.machine_id}: {status} (Temp: {machine.temperature:.1f}°C, Vib: {machine.vibration:.1f})")


if __name__ == "__main__":
    # Create and run the simulation
    simulation = WorkspaceSimulation()
    simulation.run_simulation(max_timesteps=15)
