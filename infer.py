# infer.py
import json
import os
from datetime import datetime
from collections import defaultdict, deque

import joblib
import pandas as pd
import paho.mqtt.client as mqtt

STATUS_TOPIC = "job/status"
TELEM_TOPIC  = "job/telemetry"
ALERT_TOPIC  = "job/alerts"

MODEL_PATH = "failure_rf.pkl"
META_PATH  = "model_meta.json"

# ---- Load model and tuned threshold ----
model = joblib.load(MODEL_PATH)

THRESHOLD = 0.5  # default
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

# ---- Runtime state from status topic ----
status = {}  # machine_id -> {class_name, temp_threshold, vib_threshold}

# ---- Live feature state (match training features) ----
prev = {}  # machine_id -> {"ts_ms": int, "temp": float, "vib": float}
roll = defaultdict(lambda: {"temp": deque(maxlen=5), "vib": deque(maxlen=5)})

def std_of(buf):
    n = len(buf)
    if n < 2: return 0.0
    m = sum(buf)/n
    return (sum((x-m)**2 for x in buf)/(n-1))**0.5

def to_epoch_ms(ts):
    # Accept numeric tick or ISO8601
    try:
        return int(float(ts) * 1000)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        # fallback to now
        return int(datetime.utcnow().timestamp() * 1000)

# ---- MQTT callbacks (v2 API) ----
def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected" if rc == 0 else f"Failed rc={rc}")
    client.subscribe(STATUS_TOPIC, qos=0)
    client.subscribe(TELEM_TOPIC, qos=0)

def on_message(client, userdata, msg):
    topic = msg.topic
    try:
        o = json.loads(msg.payload.decode("utf-8", "replace"))
    except json.JSONDecodeError:
        return

    # 1) Cache status
    if topic == STATUS_TOPIC:
        m_id = o.get("machine_id")
        if not m_id:
            return
        status[m_id] = {
            "class_name":     o.get("class_name"),
            "temp_threshold": o.get("temp_threshold"),
            "vib_threshold":  o.get("vib_threshold"),
        }
        return

    # 2) Telemetry -> compute features -> predict -> publish alert
    if topic == TELEM_TOPIC:
        m_id = o.get("machine_id")
        if not m_id:
            return

        sc = status.get(m_id, {})
        t_thresh = sc.get("temp_threshold")
        v_thresh = sc.get("vib_threshold")

        # Raw signals (match training names)
        temp = o.get("temperature_c") or o.get("temperature") or o.get("temp")
        vib  = o.get("vibration_rms_mm_s") or o.get("vibration")

        # Timestamp / tick if present; else use now
        ts = o.get("timestamp", datetime.utcnow().isoformat() + "Z")
        epoch_ms = to_epoch_ms(ts)

        # Deltas and dt
        dt_s = d_temp = d_vib = None
        if m_id in prev:
            dt_s = (epoch_ms - prev[m_id]["ts_ms"]) / 1000.0
            if temp is not None and prev[m_id]["temp"] is not None:
                try: d_temp = float(temp) - float(prev[m_id]["temp"])
                except Exception: d_temp = None
            if vib is not None and prev[m_id]["vib"] is not None:
                try: d_vib = float(vib) - float(prev[m_id]["vib"])
                except Exception: d_vib = None
        prev[m_id] = {"ts_ms": epoch_ms, "temp": temp, "vib": vib}

        # Rolling stats (window=5 like training)
        if temp is not None:
            try: roll[m_id]["temp"].append(float(temp))
            except Exception: pass
        if vib is not None:
            try: roll[m_id]["vib"].append(float(vib))
            except Exception: pass
        temp_avg = (sum(roll[m_id]["temp"])/len(roll[m_id]["temp"])) if roll[m_id]["temp"] else None
        vib_avg  = (sum(roll[m_id]["vib"])/len(roll[m_id]["vib"]))   if roll[m_id]["vib"]  else None
        temp_std = std_of(roll[m_id]["temp"]) if roll[m_id]["temp"] else None
        vib_std  = std_of(roll[m_id]["vib"])  if roll[m_id]["vib"]  else None

        # Normalized to thresholds
        pct_temp = None
        if temp is not None and t_thresh not in (None, 0):
            try: pct_temp = float(temp)/float(t_thresh)
            except Exception: pct_temp = None

        pct_vib = None
        if vib is not None and v_thresh not in (None, 0):
            try: pct_vib = float(vib)/float(v_thresh)
            except Exception: pct_vib = None

        # Build the exact model row (same columns as training)
        row = {
            "temperature_c":        temp,
            "vibration_rms_mm_s":   vib,
            "temp_threshold":       t_thresh,
            "vib_threshold":        v_thresh,
            "dt_seconds":           dt_s,
            "d_temp":               d_temp,
            "d_vibration":          d_vib,
            "pct_of_temp_thresh":   pct_temp,
            "pct_of_vib_thresh":    pct_vib,
            "temp_avg_win":         temp_avg,
            "temp_std_win":         temp_std,
            "vib_avg_win":          vib_avg,
            "vib_std_win":          vib_std,
            "class_name":           sc.get("class_name") or o.get("class_name"),
        }

        # Predict probability
        X = pd.DataFrame([row])
        try:
            prob = float(model.predict_proba(X)[0, 1])
        except Exception as e:
            print(f"[INFER] Predict failed for {m_id}: {e}")
            return

        red_flag = prob >= THRESHOLD

        alert = {
            "timestamp":  datetime.utcnow().isoformat() + "Z",
            "machine_id": m_id,
            "model":      "rf_v1",
            "risk_score": prob,
            "threshold":  THRESHOLD,
            "red_flag":   red_flag
        }
        client.publish(ALERT_TOPIC, json.dumps(alert), qos=0)
        if red_flag:
            print(f"[ALERT] FAIL PREDICTED → machine={m_id} | risk={prob:.3f} (thr={THRESHOLD:.3f})")

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost", 1883, 60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[MQTT] Shutting down…")
        client.disconnect()

if __name__ == "__main__":
    main()
