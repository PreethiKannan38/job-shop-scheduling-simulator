import time
import random
import json
from collections import deque, defaultdict
from typing import List, Dict
import os
import paho.mqtt.client as mqtt
import pandas as pd
import joblib

from machines import Machine
from jobs import Job

from iha_scheduler import run_iha


TOPIC_JOB_STATUS     = "job/status"
TOPIC_JOBSHOP        = "jobshop/status"
TOPIC_JOB_TELEMETRY  = "job/telemetry"

# --- Load ML model + threshold ---
MODEL_PATH = "failure_rf.pkl"
META_PATH  = "model_meta.json"

model = joblib.load(MODEL_PATH)
THRESHOLD = 0.5
if os.path.exists(META_PATH):
    try:
        with open(META_PATH, "r") as f:
            meta = json.load(f)
        THRESHOLD = float(meta.get("chosen_threshold", meta.get("best_f1_threshold", THRESHOLD)))
        print(f"[MODEL] Loaded threshold from meta: {THRESHOLD:.3f}")
    except Exception as e:
        print(f"[MODEL] Could not read {META_PATH}: {e}. Using default {THRESHOLD:.3f}")
else:
    print(f"[MODEL] {META_PATH} not found. Using default threshold {THRESHOLD:.3f}")

# optional conservative bump to reduce early preemptions on cold start
if THRESHOLD < 0.32:
    THRESHOLD = 0.32

# --- Warmup control (minimal change to reduce initial burst of failures) ---
WARMUP_TICKS = 3  # assign at most one job per class per tick during first few ticks
IHA_INTERVAL = 10
# --- Feature builder state ---
_prev = {}
_roll = defaultdict(lambda: {"temp": deque(maxlen=5), "vib": deque(maxlen=5)})

def build_features(m: Machine, t: int) -> pd.DataFrame:
    temp = m.temperature
    vib = m.vibration
    t_thresh = m.temp_threshold
    v_thresh = m.vib_threshold
    ts_ms = int(t * 1000)

    dt_s = d_temp = d_vib = None
    if m.machine_id in _prev:
        dt_s = (ts_ms - _prev[m.machine_id]["ts_ms"]) / 1000.0
        d_temp = temp - _prev[m.machine_id]["temp"]
        d_vib = vib - _prev[m.machine_id]["vib"]
    _prev[m.machine_id] = {"ts_ms": ts_ms, "temp": temp, "vib": vib}

    _roll[m.machine_id]["temp"].append(temp)
    _roll[m.machine_id]["vib"].append(vib)
    temp_vals = list(_roll[m.machine_id]["temp"])
    vib_vals = list(_roll[m.machine_id]["vib"])

    def _std(arr):
        n = len(arr)
        if n < 2: return 0.0
        m_ = sum(arr)/n
        return (sum((x-m_)**2 for x in arr)/(n-1))**0.5

    features = {
        "temperature_c": temp,
        "vibration_rms_mm_s": vib,
        "temp_threshold": t_thresh,
        "vib_threshold": v_thresh,
        "dt_seconds": dt_s,
        "d_temp": d_temp,
        "d_vibration": d_vib,
        "pct_of_temp_thresh": temp / t_thresh if t_thresh else None,
        "pct_of_vib_thresh":  vib / v_thresh if v_thresh else None,
        "temp_avg_win": sum(temp_vals)/len(temp_vals),
        "temp_std_win": _std(temp_vals),
        "vib_avg_win":  sum(vib_vals)/len(vib_vals),
        "vib_std_win":  _std(vib_vals),
        "class_name": m.class_name,
    }
    return pd.DataFrame([features])


class WorkspaceSimulation:
    """
    FIFO job shop with ML-based failure prediction.
    """

    def __init__(self,
                 broker="localhost",
                 port=1883,
                 keepalive=60,
                 tick_seconds=1.0,
                 seed_jobs=5):
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.connect(broker, port, keepalive)
        self.client.loop_start()

        self.t = 0
        self.tick_seconds = tick_seconds

        self.machines: List[Machine] = [
            Machine("A", "A_1", 40, 100, 2.0, 16.0, 3),
            Machine("A", "A_2", 41, 86, 2.2, 8.5, 3),
            Machine("A", "A_3", 42, 87, 2.1, 8.5, 3),
            Machine("B", "B_1", 50, 110, 4.0, 18.0, 5),
            Machine("B", "B_2", 49, 100, 3.8, 14.0, 5),
            Machine("C", "C_1", 30, 110, 3.0, 14.0, 4),
            Machine("C", "C_2", 31, 81, 3.2, 10.0, 4),
            Machine("D", "D_1", 35, 120, 1.5, 19.0, 6),
        ]

        self.class_queues: Dict[str, deque] = defaultdict(deque)
        for _ in range(seed_jobs):
            self.enqueue_new_job()

        self.completed_jobs = set()
        # self.initial_allocation()
    
    def initial_allocation(self):
        """
        Allocate all existing jobs to their corresponding class queues.
        This is called once after machines and jobs are created.
        """
        print("[INIT] Allocating initial jobs to class queues...")
        for _ in range(len(self.class_queues)):  # ensure old queues cleared
            for q in self.class_queues.values():
                q.clear()

        # Generate jobs and push them into their respective queues
        # for _ in range(len(self.machines)):
        #     job = Job.make_random()
        #     self.class_queues[job.required_class].append(job)
        #     print(f"[INIT] Queued job {job.job_id} into class {job.required_class}")

        print("[INIT] Initial job allocation complete.\n")


    # --- MQTT ---
    def _on_connect(self, client, userdata, flags, rc):
        print("[MQTT] Connected" if rc == 0 else f"[MQTT] Failed rc={rc}")

    def _publish_jobshop_event(self, event_type: str, payload: dict):
        msg = {"type": event_type, **payload}
        self.client.publish(TOPIC_JOBSHOP, json.dumps(msg))

    def _publish_job_status(self, machine: Machine):
        # RETAIN latest snapshot so late-joining UI immediately sees all machines
        self.client.publish(TOPIC_JOB_STATUS, machine.status_json(self.t), retain=True)

    def _publish_job_telemetry(self, machine: Machine):
        msg = {
            "timestamp": self.t,
            "class_name": machine.class_name,
            "machine_id": machine.machine_id,
            "temperature_c": getattr(machine, "temperature", machine.temp_base),
            "vibration_rms_mm_s": getattr(machine, "vibration", machine.vib_base),
            "seq": self.t,
        }
        self.client.publish(TOPIC_JOB_TELEMETRY, json.dumps(msg))
        return msg

    # --- Queue helpers ---
    def enqueue_new_job(self):
        job = Job.make_random()
        self.class_queues[job.required_class].append(job)

    def _enqueue_current_step_front(self, job: Job):
        self.class_queues[job.required_class].appendleft(job)

    def _enqueue_next_step(self, job: Job):
        if not job.done:
            self.class_queues[job.required_class].append(job)

    # --- Scheduling ---
    def _assign_tasks(self):
        """
        Assign jobs from class queues to idle machines every tick.
        Checks each machine's class, assigns next available job from its class queue if idle.
        """
        for m in self.machines:
            # Skip if machine is repairing or busy
            if getattr(m, "repairing_left", 0) > 0 or not m.idle:
                continue

            cls = m.class_name
            if not self.class_queues[cls]:
                continue  # no jobs waiting in this class queue

            # Take next job from this class queue
            job = self.class_queues[cls].popleft()

            if m.assign(job):
                self._publish_jobshop_event("STARTED", {
                    "timestamp": self.t,
                    "job_id": job.job_id,
                    "machine_id": m.machine_id,
                    "required_class": m.class_name,
                    "step_remaining": job.remaining_ticks_on_step,
                    "method": "IHA"
                })
                print(f"[ASSIGN] Job {job.job_id} assigned to {m.machine_id}")
            else:
                # Machine rejected (shouldn't normally happen) — put job back
                self.class_queues[cls].appendleft(job)
                print(f"[ASSIGN] {m.machine_id} unable to take {job.job_id}, requeued.")

    # --- Prediction ---
    def _maybe_predict_failure(self, m: Machine):
        if getattr(m, "repairing_left", 0) > 0 or m.busy_with is None:
            return

        X = build_features(m, self.t)
        try:
            prob = float(model.predict_proba(X)[0, 1])
        except Exception as e:
            print(f"[INFER] Predict failed for {m.machine_id}: {e}")
            return

        near_limit = (
            (m.temp_threshold and (m.temperature / m.temp_threshold) >= 0.80) or
            (m.vib_threshold and (m.vibration / m.vib_threshold) >= 0.80)
        )

        if prob >= THRESHOLD and near_limit:
            j = m.busy_with
            self._publish_jobshop_event("PREDICTION", {
                "timestamp": self.t,
                "machine_id": m.machine_id,
                "job_id": j.job_id if j else None,
                "reason": "will_fail",
                "risk_score": prob,
                "threshold": THRESHOLD,
            })
            if j:
                j.put_back_unfinished_step_front()
                self._enqueue_current_step_front(j)
                m.busy_with = None
            m.repairing_left = m.repair_time
            
    #-----------------------------------IHA----------------------------------------------
    
    def _run_iha_scheduler(self, target_class=None):
        """
        Run Improved Hungarian Algorithm (IHA) per class or for a specific class.

        Now supports predictive scheduling:
        - Runs even if all machines in a class are busy.
        - Uses both idle and busy machines' states (temp/vibration) to compute future order.
        - Reorders class queue to prepare optimal job sequence in advance.
        """
        print(f"\n[IHA] Scheduler called at tick={self.t}, target_class={target_class or 'ALL'}")

        unique_classes = list(self.class_queues.keys())
        if not unique_classes:
            print("[IHA] No class queues found — skipping scheduler.")
            return

        if target_class is not None:
            if target_class not in unique_classes:
                print(f"[IHA] Target class '{target_class}' not found — skipping.")
                return
            unique_classes = [target_class]

        for cls in unique_classes:
            # ✅ Include both idle and busy machines
            class_machines = [m for m in self.machines if m.class_name == cls]

            # All waiting jobs (including queued ones)
            ready_jobs = list(self.class_queues[cls])

            if not ready_jobs:
                print(f"[IHA] No ready jobs for class {cls}, skipping.")
                continue

            if not class_machines:
                print(f"[IHA] No machines found for class {cls}, skipping.")
                continue

            print(f"[IHA] Running predictive optimization for class {cls} "
                f"with {len(ready_jobs)} jobs and {len(class_machines)} machines.")

            # Run IHA using all machines (predictive mode)
            assignments = run_iha(ready_jobs, class_machines, weights=(0.6, 0.4))

            if not assignments:
                print(f"[IHA] No valid assignments found for class {cls}, keeping original queue.")
                continue

            # New queue ordering
            ordered_jobs = [job for job, _ in assignments]
            remaining_jobs = [j for j in ready_jobs if j not in ordered_jobs]
            final_order = ordered_jobs + remaining_jobs

            self.class_queues[cls] = deque(final_order)
            print(f"[IHA] Updated predictive order for class {cls}: {[j.job_id for j in final_order]}")

        print("[IHA] Predictive scheduler completed.\n")


    
    # --- Tick ---
    def tick(self):
        self.t += 1
        
        recoveries = []

        for m in self.machines:
            # Detect when a machine just finished repair (optional until RECOVERY event is added)
            if hasattr(m, "was_repairing"):
                repairing_left = getattr(m, "repairing_left", 0.0)
                currently_repairing = repairing_left > 0.0001
                if m.was_repairing and not currently_repairing:
                    recoveries.append(m)
                m.was_repairing = currently_repairing
            else:
                m.was_repairing = getattr(m, "repairing_left", 0.0) > 0.0001

        # Trigger IHA only for recoveries or periodically
        if self.t % IHA_INTERVAL == 1:
            print(f"[IHA] Generating new plan at tick={self.t}")
            self._run_iha_scheduler()
            
        self._assign_tasks()

        # Continue normal tick operations
        #self._assign_jobs()
        

        # --- Machine processing loop ---
        for m in self.machines:
            self._maybe_predict_failure(m)
            event, data = m.step()
            self._publish_job_status(m)
            self._publish_job_telemetry(m)

            # --- Handle machine events ---
            if event == "FAILED":
                j = data
                if j:
                    self._enqueue_current_step_front(j)
                    self._publish_jobshop_event("FAILED", {
                        "timestamp": self.t,
                        "machine_id": m.machine_id,
                        "class": m.class_name,
                        "job_id": j.job_id,
                        "reason": "threshold_exceeded",
                        "temperature": round(m.temperature, 2),
                        "vibration": round(m.vibration, 2),
                    })
                print(f"[EVENT] Machine {m.machine_id} FAILED — reordering queue for class {m.class_name}")
                self._run_iha_scheduler(m.class_name)  # reorder all queues so load is adjusted

            elif event == "STEP_DONE":
                j = data
                self._publish_jobshop_event("STEP_DONE", {
                    "timestamp": self.t,
                    "job_id": j.job_id,
                    "next_required_class": ("" if j.done else j.required_class),
                })
                
                self._enqueue_next_step(j)
                print(f"[EVENT] Job {j.job_id} STEP_DONE — updating next queue ({j.required_class})")
                # Reorder only the next required class queue
                self._run_iha_scheduler(j.required_class)

            elif event == "COMPLETED":
                j = data
                self.completed_jobs.add(j.job_id)
                self._publish_jobshop_event("COMPLETED", {
                    "timestamp": self.t,
                    "job_id": j.job_id,
                    "machine_id": m.machine_id,
                })
                
                print(f"[EVENT] Job {j.job_id} COMPLETED on {m.machine_id}")

        # --- End of tick checks ---
        all_idle = all(m.idle for m in self.machines)
        queues_empty = all(len(q) == 0 for q in self.class_queues.values())
        if all_idle and queues_empty:
            print(f"[SIM] All jobs completed at t={self.t}. Disconnecting…")
            self.client.loop_stop()
            self.client.disconnect()
            raise SystemExit

    def run(self, max_ticks=120):
        try:
            for _ in range(max_ticks):
                self.tick()
                time.sleep(self.tick_seconds)
        except SystemExit:
            pass
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            print("[MQTT] Disconnected")


if __name__ == "__main__":
    sim = WorkspaceSimulation(tick_seconds=2, seed_jobs=4)
    sim.run(max_ticks=1000000)