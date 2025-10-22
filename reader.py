# reader.py
import argparse
import json
import sys
import csv
import os
from datetime import datetime
from collections import defaultdict, deque
import paho.mqtt.client as mqtt

STATUS_TOPIC = "job/status"
TELEM_TOPIC  = "job/telemetry"
EVENTS_TOPIC = "jobshop/status"

# ----------------- CSV helpers -----------------
def ensure_header(path, fieldnames):
    exists = os.path.exists(path) and os.path.getsize(path) > 0
    if not exists:
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()

def append_csv(path, fieldnames, row):
    out = {k: row.get(k, None) for k in fieldnames}
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow(out)

# ----------------- time utils -----------------
def to_epoch_ms(ts):
    # Accept ints (ticks) or ISO8601 strings
    try:
        return int(float(ts) * 1000)  # numeric tick => seconds → ms
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(datetime.utcnow().timestamp() * 1000)

def mk_iso(epoch_ms):
    return datetime.utcfromtimestamp(epoch_ms/1000).isoformat() + "Z"

def tick_from_ts(ts):
    # Prefer integer tick if provided; otherwise derive from epoch seconds
    try:
        return int(float(ts))
    except Exception:
        return int(to_epoch_ms(ts) // 1000)

# ----------------- stats -----------------
def std_of(buf):
    n = len(buf)
    if n < 2:
        return 0.0
    mean = sum(buf)/n
    var = sum((x - mean) ** 2 for x in buf) / (n - 1)
    return var ** 0.5

# ----------------- main -----------------
def main():
    parser = argparse.ArgumentParser(description="MQTT → single CSV with features + labels")
    parser.add_argument("--broker", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topics", nargs="*", default=[STATUS_TOPIC, TELEM_TOPIC, EVENTS_TOPIC],
                        help="Topics to subscribe (space-separated)")
    parser.add_argument("--csv", default="training_data.csv", help="Output CSV path")
    parser.add_argument("--window", type=int, default=5, help="Rolling window (samples) per machine")
    parser.add_argument("--flush_delay", type=int, default=1,
                        help="Ticks to wait before finalizing a telemetry row to catch 'FAILED at/after'")
    args = parser.parse_args()

    # Caches
    status_cache = {}        # machine_id -> {"class_name", "temp_threshold", "vib_threshold"}
    prev_point   = {}        # machine_id -> {"epoch_ms", "tick", "temp", "vib"}
    roll = defaultdict(lambda: {"temp": deque(maxlen=args.window), "vib": deque(maxlen=args.window)})

    # Pending telemetry rows (not flushed yet) to allow labeling with near-future events
    pending_rows = defaultdict(list)  # machine_id -> [row_dict,...]

    # Failure events by machine & tick
    failure_ticks = defaultdict(set)  # machine_id -> {tick_int, ...}

    # Final CSV schema
    fields = [
        "timestamp_iso","epoch_ms","tick","machine_id","class_name","seq",
        "temperature_c","vibration_rms_mm_s",
        "temp_threshold","vib_threshold",
        "dt_seconds","d_temp","d_vibration",
        "pct_of_temp_thresh","pct_of_vib_thresh",
        "temp_avg_win","temp_std_win","vib_avg_win","vib_std_win",
        "failure_flag"
    ]
    ensure_header(args.csv, fields)

    # -------- MQTT callbacks --------
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("[MQTT] Connected")
            for t in args.topics:
                client.subscribe(t, qos=0)
                print(f"[MQTT] Subscribed: {t}")
        else:
            print(f"[MQTT] Connect failed rc={rc}")

    def finalize_flushable_rows(machine_id, current_tick):
        """
        Flush rows whose tick is <= current_tick - flush_delay.
        Label as 1 if FAILED at same tick or the immediate next tick.
        """
        if machine_id not in pending_rows:
            return
        keep = []
        for row in pending_rows[machine_id]:
            row_tick = row["tick"]
            if row_tick <= (current_tick - args.flush_delay):
                fails = failure_ticks[machine_id]
                label = 1 if (row_tick in fails or (row_tick + 1) in fails) else 0
                row["failure_flag"] = label
                append_csv(args.csv, fields, row)
            else:
                keep.append(row)
        pending_rows[machine_id] = keep

    def on_message(client, userdata, msg):
        topic = msg.topic
        s = msg.payload.decode("utf-8", errors="replace")
        try:
            obj = json.loads(s)
        except json.JSONDecodeError:
            print(f"[WARN] Non-JSON on {topic}: {s[:120]}", file=sys.stderr)
            return

        # -------- STATUS --------
        if topic == STATUS_TOPIC:
            m_id = obj.get("machine_id")
            if not m_id:
                return
            status_cache[m_id] = {
                "class_name": obj.get("class_name"),
                "temp_threshold": obj.get("temp_threshold"),
                "vib_threshold": obj.get("vib_threshold"),
            }

        # -------- TELEMETRY --------
        elif topic == TELEM_TOPIC:
            m_id = obj.get("machine_id")
            if not m_id:
                return

            ts_raw = obj.get("timestamp", datetime.utcnow().isoformat()+"Z")
            epoch_ms = to_epoch_ms(ts_raw)
            tick = tick_from_ts(ts_raw)
            cls = obj.get("class_name") or (status_cache.get(m_id) or {}).get("class_name")
            seq = obj.get("seq")

            # Raw signals (only temp & vibration as requested)
            temp = obj.get("temperature_c") or obj.get("temperature") or obj.get("temp")
            vib  = obj.get("vibration_rms_mm_s") or obj.get("vibration")

            # Thresholds
            t_thresh = (status_cache.get(m_id) or {}).get("temp_threshold")
            v_thresh = (status_cache.get(m_id) or {}).get("vib_threshold")

            # Deltas & dt
            dt_s, d_temp, d_vib = None, None, None
            if m_id in prev_point:
                dt_s = (epoch_ms - prev_point[m_id]["epoch_ms"]) / 1000.0
                if temp is not None and prev_point[m_id]["temp"] is not None:
                    try: d_temp = float(temp) - float(prev_point[m_id]["temp"])
                    except Exception: d_temp = None
                if vib is not None and prev_point[m_id]["vib"] is not None:
                    try: d_vib = float(vib) - float(prev_point[m_id]["vib"])
                    except Exception: d_vib = None
            prev_point[m_id] = {"epoch_ms": epoch_ms, "tick": tick, "temp": temp, "vib": vib}

            # Rolling stats
            if temp is not None: roll[m_id]["temp"].append(float(temp))
            if vib  is not None: roll[m_id]["vib"].append(float(vib))
            temp_avg = (sum(roll[m_id]["temp"]) / len(roll[m_id]["temp"])) if roll[m_id]["temp"] else None
            vib_avg  = (sum(roll[m_id]["vib"])  / len(roll[m_id]["vib"]))  if roll[m_id]["vib"] else None
            temp_std = std_of(roll[m_id]["temp"]) if roll[m_id]["temp"] else None
            vib_std  = std_of(roll[m_id]["vib"])  if roll[m_id]["vib"] else None

            # Normalized to thresholds
            pct_temp = None
            if temp is not None and t_thresh not in (None, 0):
                try: pct_temp = float(temp) / float(t_thresh)
                except Exception: pct_temp = None

            pct_vib = None
            if vib is not None and v_thresh not in (None, 0):
                try: pct_vib = float(vib) / float(v_thresh)
                except Exception: pct_vib = None

            row = {
                "timestamp_iso": mk_iso(epoch_ms),
                "epoch_ms": epoch_ms,
                "tick": tick,
                "machine_id": m_id,
                "class_name": cls,
                "seq": seq,
                "temperature_c": temp,
                "vibration_rms_mm_s": vib,
                "temp_threshold": t_thresh,
                "vib_threshold": v_thresh,
                "dt_seconds": dt_s,
                "d_temp": d_temp,
                "d_vibration": d_vib,
                "pct_of_temp_thresh": pct_temp,
                "pct_of_vib_thresh": pct_vib,
                "temp_avg_win": temp_avg,
                "temp_std_win": temp_std,
                "vib_avg_win": vib_avg,
                "vib_std_win": vib_std,
                "failure_flag": None,  # set when flushing
            }

            # Buffer row (we'll label & flush after a small delay to catch 'FAILED right after')
            pending_rows[m_id].append(row)

            # Attempt to flush rows that are old enough
            finalize_flushable_rows(m_id, current_tick=tick)

        # -------- EVENTS (labels) --------
        elif topic == EVENTS_TOPIC:
            evt_type = obj.get("type")
            if evt_type == "FAILED":
                m_id = obj.get("machine_id")
                if m_id:
                    t = tick_from_ts(obj.get("timestamp"))
                    failure_ticks[m_id].add(t)

        # Optional console pretty-print
        now = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{now}] Topic: {topic}")
        print(json.dumps(obj, indent=2, sort_keys=True))
        sys.stdout.flush()

    # MQTT wiring
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(args.broker, args.port, keepalive=60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[MQTT] Shutting down…")
        # On shutdown, flush everything left in buffers with best-effort labels
        for m_id, rows in pending_rows.items():
            for row in rows:
                t = row["tick"]
                fails = failure_ticks[m_id]
                label = 1 if (t in fails or (t + 1) in fails) else 0
                row["failure_flag"] = label
                append_csv(args.csv, fields, row)
        client.disconnect()

if __name__ == "__main__":
    main()