import random
import json

class Machine:
    """
    Digital Twin Enabled Machine Class for Flexible Job Shop Simulation.

    Attributes:
        class_name (str): Category/type of the machine (e.g., 'A', 'B', 'C').
        machine_id (str): Unique identifier for the machine (e.g., 'A_1').
        temp_base (float): Baseline temperature when idle or after repair.
        temp_threshold (float): Temperature limit for fault detection.
        vib_base (float): Baseline vibration level.
        vib_threshold (float): Vibration limit for fault detection.
        repair_time (int): Number of timesteps required to repair after fault.

    State Variables:
        temperature (float): Current temperature of the machine.
        vibration (float): Current vibration level.
        operational (bool): Operational status (True = working, False = faulty).
        repair_timer (int): Current repair countdown timer.
    """

    def __init__(self, class_name, machine_id, temp_base, temp_threshold, vib_base, vib_threshold, repair_time):
        # Initialize machine identity and thresholds
        self.class_name = class_name
        self.machine_id = machine_id

        self.temp_base = temp_base
        self.temp_threshold = temp_threshold
        self.vib_base = vib_base
        self.vib_threshold = vib_threshold
        self.repair_time = repair_time

        # Initialize dynamic status values
        self.temperature = temp_base
        self.vibration = vib_base
        self.operational = True
        self.repair_timer = 0

    def process_job(self, job_temp_increment, job_vib_increment):
        """
        Simulates processing a job on the machine by affecting temperature and vibration.
        If machine is faulty, it counts down repair time and recovers when complete.

        Args:
            job_temp_increment (float): Temperature increase caused by current job.
            job_vib_increment (float): Vibration increase caused by current job.
        """
        if not self.operational:
            # Machine is under repair, increment repair timer
            self.repair_timer += 1
            if self.repair_timer >= self.repair_time:
                # Repair finished: reset machine state
                self.operational = True
                self.temperature = self.temp_base
                self.vibration = self.vib_base
                self.repair_timer = 0
            # If still repairing, no job processing occurs
            return

        # Simulate random fluctuation/noise in temp and vibration increments
        temp_change = job_temp_increment + random.uniform(-1.5, 1.5)
        vib_change = job_vib_increment + random.uniform(-1, 1)

        self.temperature += temp_change
        self.vibration += vib_change

        # Check if machine state exceeds fault thresholds
        if self.temperature >= self.temp_threshold:
            self.operational = False
        elif self.vibration >= self.vib_threshold:
            self.operational = False

    def get_status(self, current_timestep):
        """
        Prepares a JSON-serializable dictionary describing the current machine status.

        Args:
            current_timestep (int): Global simulation timestep for logging.

        Returns:
            str: JSON string representing machine status for MQTT.
        """
        status_str = "Operational" if self.operational else f"Faulty (Repair {self.repair_timer}/{self.repair_time})"
        status_dict = {
            "timestamp": current_timestep,
            "machine_id": self.machine_id,
            "class_name": self.class_name,
            "temperature": round(self.temperature, 2),
            "vibration": round(self.vibration, 2),
            "status": status_str
        }
        return json.dumps(status_dict)
#will be changed as per requirements and once the job classes are confirmed