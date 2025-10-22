# machines.py
import json
import random
from dataclasses import dataclass, field
from typing import Optional, Tuple
from jobs import Job

@dataclass
class Machine:
    """
    Digital Twin Enabled Machine: class-based routing + failure/repair.
    """
    class_name: str
    machine_id: str
    temp_base: float
    temp_threshold: float
    vib_base: float
    vib_threshold: float
    repair_time: int

    temperature: float = field(init=False)
    vibration: float = field(init=False)
    busy_with: Optional[Job] = field(default=None, init=False)
    repairing_left: int = field(default=0, init=False)
    total_power_kwh: float = field(default=0.0, init=False)
    

    def __post_init__(self):
        self.temperature = self.temp_base
        self.vibration = self.vib_base

    @property
    def operational(self) -> bool:
        return self.repairing_left == 0

    @property
    def idle(self) -> bool:
        return self.operational and self.busy_with is None

    def assign(self, job: Job) -> bool:
        if not self.idle:
            return False
        if job.required_class != self.class_name:
            return False
        # --- reduction logic ---
        # Compute difference from base values
        temp_diff = self.temperature - self.temp_base
        vib_diff  = self.vibration - self.vib_base

        reduction = getattr(job, "reduction", 0.0)  # fallback safe

        # Reduce current readings by that percentage of the excess
        if reduction > 0:
            self.temperature -= temp_diff * reduction
            self.vibration  -= vib_diff * reduction

        # Now assign the job
        self.busy_with = job
        return True

    # --- helpers ---
    def _cooldown(self):
        self.temperature = max(self.temp_base, self.temperature - 1.2)
        self.vibration  = max(self.vib_base,  self.vibration  - 0.25)

    def _maybe_spike(self):
        # small random spikes to occasionally cross thresholds
        if random.random() < 0.07:
            self.temperature += random.uniform(2.0, 6.0)
        if random.random() < 0.07:
            self.vibration  += random.uniform(0.8, 2.0)

    # --- tick ---
    def step(self) -> Tuple[Optional[str], Optional[Job]]:
        """
        Returns (event, data): event in {None, "FAILED", "STEP_DONE", "COMPLETED"}
        """
        if self.repairing_left > 0:
            self.repairing_left -= 1
            if self.repairing_left == 0:
                self.temperature = self.temp_base
                self.vibration  = self.vib_base
            return None, None

        if self.busy_with:
            j = self.busy_with
            # apply job load + noise
            self.temperature += j.temp_inc + random.uniform(-1.0, 1.4)
            self.vibration  += j.vib_inc  + random.uniform(-0.4, 0.6)
            power_kw = getattr(j, "current_power_kw", 0.0)
            self.total_power_kwh += power_kw * (1 / 60)  # assuming 1 tick = 1 minute
            self._maybe_spike()
            # thresholds
            if self.temperature >= self.temp_threshold or self.vibration >= self.vib_threshold:
                # Return the job so the simulation can requeue to FRONT of same-class queue
                failed_job = self.busy_with
                failed_job.put_back_unfinished_step_front()
                self.busy_with = None
                self.repairing_left = self.repair_time
                return "FAILED", failed_job

            before = j.remaining_ticks_on_step
            j.work_one_tick()

            if j.done:
                finished = j
                self.busy_with = None
                return "COMPLETED", finished

            if before == 1:  # just finished a step (moved to next step)
                step_done = j
                self.busy_with = None
                return "STEP_DONE", step_done

            return None, None

        # idle
        self._cooldown()
        return None, None

    def status_json(self, timestamp: int) -> str:
        if self.repairing_left > 0:
            status = f"Repairing ({self.repair_time - self.repairing_left}/{self.repair_time})"
            current_job = "REPAIR"
        elif self.busy_with:
            status = "Operational"
            current_job = self.busy_with.job_id
        else:
            status = "Operational"
            current_job = "IDLE"

        doc = {
            "timestamp": timestamp,
            "machine_id": self.machine_id,
            "class_name": self.class_name,
            "temperature": round(self.temperature, 2),
            "vibration": round(self.vibration, 2),
            "status": status,
            "current_job": current_job,  # never null
            "temp_threshold": self.temp_threshold,
            "vib_threshold": self.vib_threshold,
            "power_kwh_total": round(self.total_power_kwh, 3),
        }
        return json.dumps(doc)
