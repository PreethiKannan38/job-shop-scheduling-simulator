# simpy_simulation.py
import json
import os
from collections import deque, defaultdict
from typing import List, Dict

import simpy
import paho.mqtt.client as mqtt
import pandas as pd
import joblib

from machines import Machine
from jobs import Job
from iha_scheduler import run_iha

# ---- Topics (keep same as your current runner) ----
TOPIC_JOB_STATUS     = "job/status"
TOPIC_JOBSHOP        = "jobshop/status"
TOPIC_JOB_TELEMETRY  = "job/telemetry"

# ---- Load ML model + tuned threshold (same logic as your tick runner) ----
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

# optional conservative bump like your runner
if THRESHOLD < 0.32:
    THRESHOLD = 0.32

# ---- SimPy config ----
ASSIGN_PULSE_MIN     = 1      # how often we try to assign idle machines
IHA_INTERVAL_MIN     = 10     # reorder cadence
TELEMETRY_EVERY_MIN  = 1      # publish status/telemetry each simulated minute
IDLE_GRACE_MIN       = 30     # stop the sim if fully idle for this long

# ---- Local feature builder (mirrors your runner) ----
_prev = {}
_roll = defaultdict(lambda: {"temp": deque(maxlen=5), "vib": deque(maxlen=5)})

def build_features(m: Machine, t_seconds: int) -> pd.DataFrame:
    temp = m.temperature
    vib = m.vibration
    t_thresh = m.temp_threshold
    v_thresh = m.vib_threshold
    ts_ms = int(t_seconds * 1000)

    dt_s = d_temp = d_vib = None
    if m.machine_id in _prev:
        dt_s = (ts_ms - _prev[m.machine_id]["ts_ms"]) / 1000.0
        d_temp = temp - _prev[m.machine_id]["temp"]
        d_vib  = vib  - _prev[m.machine_id]["vib"]
    _prev[m.machine_id] = {"ts_ms": ts_ms, "temp": temp, "vib": vib}

    _roll[m.machine_id]["temp"].append(temp)
    _roll[m.machine_id]["vib"].append(vib)
    temp_vals = list(_roll[m.machine_id]["temp"])
    vib_vals  = list(_roll[m.machine_id]["vib"])

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
        "pct_of_vib_thresh":  vib  / v_thresh if v_thresh  else None,
        "temp_avg_win": sum(temp_vals)/len(temp_vals),
        "temp_std_win": _std(temp_vals),
        "vib_avg_win":  sum(vib_vals)/len(vib_vals),
        "vib_std_win":  _std(vib_vals),
        "class_name": m.class_name,
    }
    return pd.DataFrame([features])


class SimPyWorkspace:
    """
    Same shop as your tick-based runner, but driven by SimPy.
    Machines keep their own physics/thresholds (step); we advance time
    with yield env.timeout(1) and react to events.
    """

    def __init__(self, env: simpy.Environment,
                 broker="localhost", port=1883, keepalive=60,
                 seed_jobs=5):

        self.env = env
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.connect(broker, port, keepalive)
        self.client.loop_start()

        # Machines (copy of your runner’s defaults)
        self.machines: List[Machine] = [
            Machine("A", "A_1", 40, 100, 2.0, 16.0, 3),
            Machine("A", "A_2", 41, 86,  2.2, 8.5,  3),
            Machine("A", "A_3", 42, 87,  2.1, 8.5,  3),
            Machine("B", "B_1", 50, 110, 4.0, 18.0, 5),
            Machine("B", "B_2", 49, 100, 3.8, 14.0, 5),
            Machine("C", "C_1", 30, 110, 3.0, 14.0, 4),
            Machine("C", "C_2", 31, 81,  3.2, 10.0, 4),
            Machine("D", "D_1", 35, 120, 1.5, 19.0, 6),
        ]

        # Per-class queues like before
        self.class_queues: Dict[str, deque] = defaultdict(deque)
        for _ in range(seed_jobs):
            self.enqueue_new_job()

        self.completed_jobs = set()

        # Spin up background SimPy processes
        self.env.process(self._assigner_loop())
        self.env.process(self._iha_pulse())
        self.env.process(self._idle_shutdown_monitor(IDLE_GRACE_MIN))
        for m in self.machines:
            self.env.process(self._machine_driver(m))

    # ---------- MQTT ----------
    def _on_connect(self, client, userdata, flags, rc):
        print("[MQTT] Connected" if rc == 0 else f"[MQTT] Failed rc={rc}")

    def _publish_jobshop_event(self, event_type: str, payload: dict):
        self.client.publish(TOPIC_JOBSHOP, json.dumps({"type": event_type, **payload}))

    def _publish_job_status(self, machine: Machine, t_sec: int):
        doc_json = machine.status_json(t_sec)
        self.client.publish(TOPIC_JOB_STATUS, doc_json, retain=True)

    def _publish_job_telemetry(self, machine: Machine, t_sec: int):
        msg = {
            "timestamp": t_sec,
            "class_name": machine.class_name,
            "machine_id": machine.machine_id,
            "temperature_c": getattr(machine, "temperature", machine.temp_base),
            "vibration_rms_mm_s": getattr(machine, "vibration", machine.vib_base),
            "seq": t_sec,
        }
        self.client.publish(TOPIC_JOB_TELEMETRY, json.dumps(msg))
        return msg

    # ---------- Queue helpers ----------
    def enqueue_new_job(self):
        job = Job.make_random()
        self.class_queues[job.required_class].append(job)

    def _enqueue_current_step_front(self, job: Job):
        self.class_queues[job.required_class].appendleft(job)

    def _enqueue_next_step(self, job: Job):
        if not job.done:
            self.class_queues[job.required_class].append(job)

    # ---------- Completion / idle detection ----------
    def _all_done(self) -> bool:
        queues_empty = all(len(q) == 0 for q in self.class_queues.values())
        machines_idle = all(
            (m.busy_with is None) and (getattr(m, "repairing_left", 0) <= 0)
            for m in self.machines
        )
        return queues_empty and machines_idle

    def _idle_shutdown_monitor(self, idle_grace_min=30):
        """Stop the sim if everything is idle for 'idle_grace_min' simulated minutes."""
        idle_start = None
        while True:
            if self._all_done():
                if idle_start is None:
                    idle_start = self.env.now
                elif (self.env.now - idle_start) >= idle_grace_min:
                    print(f"[HALT] No work for {idle_grace_min} minutes. Stopping at t={int(self.env.now)}.")
                    self.env.exit()  # raises StopSimulation to end env.run()
                    return
            else:
                idle_start = None
            yield self.env.timeout(1)

    # ---------- Scheduling ----------
    def _assigner_loop(self):
        """Assign idle machines every ASSIGN_PULSE_MIN simulated minutes."""
        while True:
            for m in self.machines:
                if getattr(m, "repairing_left", 0) > 0 or not m.idle:
                    continue
                cls = m.class_name
                if not self.class_queues[cls]:
                    continue
                job = self.class_queues[cls].popleft()
                if m.assign(job):
                    t = int(self.env.now)
                    self._publish_jobshop_event("STARTED", {
                        "timestamp": t,
                        "job_id": job.job_id,
                        "machine_id": m.machine_id,
                        "required_class": m.class_name,
                        "step_remaining": job.remaining_ticks_on_step,
                        "method": "IHA"
                    })
                    print(f"[ASSIGN] Job {job.job_id} → {m.machine_id}")
                else:
                    self.class_queues[cls].appendleft(job)
                    print(f"[ASSIGN] {m.machine_id} unable to take {job.job_id}, requeued.")
            yield self.env.timeout(ASSIGN_PULSE_MIN)

    def _iha_pulse(self):
        """Reorder class queues periodically using your IHA scheduler."""
        while True:
            yield self.env.timeout(IHA_INTERVAL_MIN)

            # Skip if there is literally nothing to schedule
            if self._all_done():
                continue

            any_change = False
            for cls in list(self.class_queues.keys()):
                ready_jobs = list(self.class_queues[cls])
                class_machines = [m for m in self.machines if m.class_name == cls]
                if not ready_jobs or not class_machines:
                    continue

                assignments = run_iha(ready_jobs, class_machines, weights=(0.6, 0.4))
                if not assignments:
                    continue

                ordered_jobs = [j for j, _ in assignments]
                remaining    = [j for j in ready_jobs if j not in ordered_jobs]
                new_queue    = deque(ordered_jobs + remaining)

                if list(new_queue) != list(self.class_queues[cls]):
                    self.class_queues[cls] = new_queue
                    any_change = True

            if any_change:
                print(f"[IHA] Reordered at t={int(self.env.now)}")

    # ---------- Machine driver ----------
    def _maybe_predict_and_preempt(self, m: Machine):
        """Call your RF model; if risky & near-limit, preempt like original code."""
        if getattr(m, "repairing_left", 0) > 0 or m.busy_with is None:
            return
        X = build_features(m, int(self.env.now))
        try:
            prob = float(model.predict_proba(X)[0, 1])
        except Exception as e:
            print(f"[INFER] Predict failed for {m.machine_id}: {e}")
            return

        near_limit = (
            (m.temp_threshold and (m.temperature / m.temp_threshold) >= 0.80) or
            (m.vib_threshold  and (m.vibration  / m.vib_threshold)  >= 0.80)
        )
        if prob >= THRESHOLD and near_limit:
            j = m.busy_with
            self._publish_jobshop_event("PREDICTION", {
                "timestamp": int(self.env.now),
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

    def _machine_driver(self, m: Machine):
        """
        One SimPy process per machine.
        Each simulated minute:
          - run predictive check
          - call m.step() once (reuses your physics/thresholds)
          - publish status + telemetry (so infer.py keeps working)
        """
        while True:
            # 1) predictive preemption (RF model)
            self._maybe_predict_and_preempt(m)

            # 2) perform one "tick" of machine work using existing step()
            event, data = m.step()   # returns FAILED / STEP_DONE / COMPLETED / None

            # 3) publish status + telemetry every minute
            t = int(self.env.now)
            self._publish_job_status(m, t)
            self._publish_job_telemetry(m, t)

            # 4) handle events like your old tick loop
            if event == "FAILED":
                j = data
                if j:
                    self._enqueue_current_step_front(j)
                    self._publish_jobshop_event("FAILED", {
                        "timestamp": t,
                        "machine_id": m.machine_id,
                        "class": m.class_name,
                        "job_id": j.job_id,
                        "reason": "threshold_exceeded",
                        "temperature": round(m.temperature, 2),
                        "vibration": round(m.vibration, 2),
                    })
                print(f"[EVENT] {m.machine_id} FAILED — reordering class {m.class_name}")
                # reorder only this class
                ready_jobs = list(self.class_queues[m.class_name])
                class_machines = [mm for mm in self.machines if mm.class_name == m.class_name]
                if ready_jobs and class_machines:
                    assignments = run_iha(ready_jobs, class_machines, weights=(0.6, 0.4))
                    if assignments:
                        ordered = [jj for jj, _ in assignments]
                        remaining = [jj for jj in ready_jobs if jj not in ordered]
                        self.class_queues[m.class_name] = deque(ordered + remaining)

            elif event == "STEP_DONE":
                j = data
                self._publish_jobshop_event("STEP_DONE", {
                    "timestamp": t,
                    "job_id": j.job_id,
                    "next_required_class": ("" if j.done else j.required_class),
                })
                self._enqueue_next_step(j)
                print(f"[EVENT] {j.job_id} STEP_DONE → next {j.required_class}")

            elif event == "COMPLETED":
                j = data
                self.completed_jobs.add(j.job_id)
                self._publish_jobshop_event("COMPLETED", {
                    "timestamp": t,
                    "job_id": j.job_id,
                    "machine_id": m.machine_id,
                })
                print(f"[EVENT] {j.job_id} COMPLETED on {m.machine_id}")

            # 5) advance simulated time by one minute
            yield self.env.timeout(TELEMETRY_EVERY_MIN)


def main():
    env = simpy.Environment()
    SimPyWorkspace(env, seed_jobs=5)
    # run until idle monitor calls env.exit()
    env.run()


if __name__ == "__main__":
    main()