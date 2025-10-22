import React, { useEffect, useMemo, useRef, useState } from "react";
import mqtt from "mqtt";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
  CartesianGrid,
  Scatter,
} from "recharts";

/**
 * MQTTDashboard.jsx
 *
 * npm i mqtt recharts
 */

const DEFAULT_BROKER =
  import.meta?.env?.VITE_MQTT_WS_URL || "ws://localhost:8083"; // normalized to /mqtt
const STATUS_TOPIC = "job/status";      // per-tick machine snapshot
const EVENT_TOPIC  = "jobshop/status";  // lifecycle events

// ---------- utils ----------
function fmtClock(ts) {
  const d = new Date(ts);
  return d.toLocaleTimeString();
}
function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}
function parseRepairProgress(statusText) {
  if (!statusText) return null;
  const m = statusText.match(/Repairing\s*\((\d+)\s*\/\s*(\d+)\)/i);
  if (!m) return null;
  const done = parseInt(m[1], 10);
  const total = parseInt(m[2], 10);
  return { done, total };
}
// nearest sample (by ts) in an array of { ts, ... }
function nearestSample(samples, ts) {
  if (!samples || samples.length === 0) return null;
  let best = samples[0];
  let bestD = Math.abs(samples[0].ts - ts);
  for (let i = 1; i < samples.length; i++) {
    const d = Math.abs(samples[i].ts - ts);
    if (d < bestD) {
      best = samples[i];
      bestD = d;
    }
  }
  return best;
}
// dedupe timestamps that are very close (avoid stacked markers)
function dedupeTimestamps(tsArray, minGapMs = 1200) {
  const out = [];
  let last = -Infinity;
  const sorted = [...tsArray].sort((a, b) => a - b);
  for (const t of sorted) {
    if (t - last >= minGapMs) {
      out.push(t);
      last = t;
    }
  }
  return out;
}
// stable color from id (HSL → hex-ish via inline hsl)
function hueFromString(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return h % 360;
}

// ---------- tiny atoms ----------
function StatusDot({ online }) {
  return (
    <span
      className={`inline-block h-3 w-3 rounded-full mr-2 ${
        online ? "bg-green-500" : "bg-rose-500"
      }`}
      title={online ? "Connected" : "Disconnected"}
    />
  );
}
function Chevron({ open }) {
  return (
    <svg
      className={`h-4 w-4 transition-transform ${open ? "rotate-90" : "rotate-0"}`}
      viewBox="0 0 20 20"
      fill="currentColor"
      aria-hidden="true"
    >
      <path d="M7.293 14.707a1 1 0 0 1 0-1.414L10.586 10 7.293 6.707a1 1 0 0 1 1.414-1.414l4 4a1 1 0 0 1 0 1.414l-4 4a1 1 0 0 1-1.414 0Z" />
    </svg>
  );
}

/* =========================
   QueueLine (single-line)
   =========================
   Visualizes global job queue as a single animated line of capsules.
   Events:
   - STARTED      => ensure present; move forward near front (or keep order)
   - STEP_DONE    => green pulse
   - PREDICTION   => blue pulse + send to back
   - FAILED       => red shake/glow + send to back
   - COMPLETED    => fade out then remove
*/
function QueueLine({ items, height = 90 }) {
  const containerRef = useRef(null);
  const [width, setWidth] = useState(0);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => setWidth(el.clientWidth));
    ro.observe(el);
    setWidth(el.clientWidth);
    return () => ro.disconnect();
  }, []);

  // Layout: fixed capsule width for predictable motion
  const capsuleW = 90; // px
  const gap = 12;       // px
  const linePad = 32;

  // Compute target x for each item by index
  const positions = useMemo(() => {
    const pos = new Map();
    items.forEach((it, idx) => {
      const x = linePad + idx * (capsuleW + gap);
      pos.set(it.id, x);
    });
    return pos;
  }, [items]);

  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded-2xl p-4">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-sm font-semibold text-neutral-200">Live Queue</h3>
          <p className="text-xs text-neutral-400">single line · jobs slide, pulse, and reorder on events</p>
        </div>
      </div>

      <div
        ref={containerRef}
        className="relative overflow-hidden"
        style={{ height }}
      >
        {/* the track */}
        <div className="absolute left-0 right-0 top-1/2 -translate-y-1/2 h-[2px] bg-neutral-800" />

        {/* capsules */}
        {items.map((it) => {
          const left = positions.get(it.id) ?? 0;
          const h = hueFromString(it.id);
          const base = `hsl(${h} 85% 60%)`;
          const dark = `hsl(${h} 75% 45%)`;

          // animation states
          const isPred = it.state === "prediction";
          const isFail = it.state === "failed";
          const isStep = it.state === "step";
          const isLeaving = it.state === "completed";

          return (
            <div
              key={it.key} // key changes to retrigger CSS when state toggles
              className={`absolute top-1/2 -translate-y-1/2 will-change-transform transition-all duration-600 ease-out
                ${isLeaving ? "opacity-0 scale-90" : "opacity-100"}
              `}
              style={{
                left,
                width: capsuleW,
                transform: `translateY(-50%)`,
              }}
            >
              <div
  className={`
    relative rounded-xl px-3 py-4 shadow-lg border
    text-sm font-medium truncate text-white
    transition-all duration-500
    ${isFail ? "animate-[shake_400ms_ease-in-out_1]" : ""}
  `}
  style={{
    background: `linear-gradient(180deg, ${base}, ${dark})`,
    borderColor: "rgba(255,255,255,0.1)",
    boxShadow: isPred
      ? "0 0 0 0 rgba(56,189,248,0.7)"
      : isFail
      ? "0 0 20px 2px rgba(239,68,68,0.45)"
      : isStep
      ? "0 0 12px 0 rgba(74,222,128,0.35)"
      : "0 1px 6px rgba(0,0,0,0.35)",
  }}
>

                <div className="flex items-center gap-2">
                  {/* status dot overlay */}
                  {isPred && (
                    <span className="inline-block h-2.5 w-2.5 rounded-full bg-sky-300 animate-ping" />
                  )}
                  {isFail && (
                    <span className="inline-block h-2.5 w-2.5 rounded-full bg-rose-300" />
                  )}
                  {isStep && (
                    <span className="inline-block h-2.5 w-2.5 rounded-full bg-emerald-300 animate-pulse" />
                  )}
                  <span className="truncate">{it.id}</span>
                </div>

                {/* machine tag */}
                <div className="absolute -top-1 left-2 text-[10px] px-1.5 py-0.5 rounded bg-neutral-950/70 border border-neutral-800 text-neutral-300">
                  {it.machine_id ?? "—"}
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* subtle legend */}
      <div className="mt-2 text-[11px] text-neutral-500 flex items-center gap-4">
        <span className="inline-flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-sky-300 inline-block" /> prediction → pushed back</span>
        <span className="inline-flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-rose-400 inline-block" /> failed → pushed back</span>
        <span className="inline-flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-emerald-300 inline-block" /> step done</span>
      </div>

      {/* keyframes */}
      <style>{`
        @keyframes shake {
          0%,100% { transform: translateY(-50%) translateX(0); }
          20% { transform: translateY(-50%) translateX(-4px); }
          40% { transform: translateY(-50%) translateX(4px); }
          60% { transform: translateY(-50%) translateX(-3px); }
          80% { transform: translateY(-50%) translateX(3px); }
        }
      `}</style>
    </div>
  );
}

// ---------- main dashboard ----------
export default function MQTTDashboard() {
  const clientRef = useRef(null);
  const [brokerUrl, setBrokerUrl] = useState(DEFAULT_BROKER);
  const [isConnected, setIsConnected] = useState(false);

  // machines snapshot (job/status)
  const [machines, setMachines] = useState({});
  const lastEventRef = useRef({}); // { [machine_id]: { type, ts } }

  // full history per machine (no cap, as requested)
  const historiesRef = useRef({}); // { mid: [{ ts, temperature, vibration }] }
  // event markers
  const predictionsRef = useRef({}); // { mid: number[] }
  const failuresRef    = useRef({}); // { mid: number[] }
  const [, forceTick] = useState(0);

  // activity log
  const [events, setEvents] = useState([]);
  const [logOpen, setLogOpen] = useState(false);

  // (replacing Gantt) queue state
  const queueRef = useRef([]); // [{ id, machine_id, state, key }]
  const [queue, setQueue] = useState([]); // render copy

  // selection
  const machineIds = useMemo(() => Object.keys(machines).sort(), [machines]);
  const [selectedMachine, setSelectedMachine] = useState("");
  useEffect(() => {
    if (!selectedMachine && machineIds.length) setSelectedMachine(machineIds[0]);
  }, [machineIds, selectedMachine]);

  // helpers to mutate queue with animation states
  const touch = () => setQueue([...queueRef.current]);

  function ensureJob(job_id, machine_id) {
    const arr = queueRef.current;
    let item = arr.find((j) => j.id === job_id);
    if (!item) {
      item = { id: job_id, machine_id, state: "idle", key: `${job_id}-${Date.now()}` };
      arr.push(item);
    } else if (machine_id) {
      item.machine_id = machine_id;
    }
    return item;
  }

  function moveToBack(job_id) {
    const arr = queueRef.current;
    const idx = arr.findIndex((j) => j.id === job_id);
    if (idx >= 0) {
      const [it] = arr.splice(idx, 1);
      arr.push(it);
    }
  }

  function removeJob(job_id) {
    const arr = queueRef.current;
    const idx = arr.findIndex((j) => j.id === job_id);
    if (idx >= 0) arr.splice(idx, 1);
  }

  // connect
  const connect = () => {
    try {
      if (clientRef.current) {
        try { clientRef.current.end(true); } catch {}
        clientRef.current = null;
      }

      // normalize to /mqtt if needed
      const raw = brokerUrl.trim();
      let finalUrl = raw;
      try {
        const u = new URL(raw);
        if (u.protocol.startsWith("ws") && (u.pathname === "/" || u.pathname === "")) {
          u.pathname = "/mqtt";
          finalUrl = u.toString();
        }
      } catch {}

      const client = mqtt.connect(finalUrl, {
        reconnectPeriod: 2000,
        keepalive: 30,
      });

      client.on("connect", () => {
        setIsConnected(true);
        client.subscribe(STATUS_TOPIC);
        client.subscribe(EVENT_TOPIC);
      });
      client.on("reconnect", () => setIsConnected(false));
      client.on("close", () => setIsConnected(false));
      client.on("offline", () => setIsConnected(false));
      client.on("error", (err) => console.error("MQTT error:", err));

      client.on("message", (topic, payload) => {
        let obj = null;
        try { obj = JSON.parse(payload.toString()); } catch {}
        if (!obj) return;

        const now = Date.now();

        if (topic === STATUS_TOPIC) {
          // machine.status_json()
          const mId = obj.machine_id;

          // snapshot
          setMachines((prev) => ({
            ...prev,
            [mId]: {
              machine_id: mId,
              class_name: obj.class_name,
              temperature: obj.temperature,
              vibration: obj.vibration,
              status: obj.status,
              current_job: obj.current_job,
              temp_threshold: obj.temp_threshold,
              vib_threshold: obj.vib_threshold,
              power_kwh_total: obj.power_kwh_total ?? 0,
              lastSeen: now,
            },
          }));

          // unlimited history (as requested). consider capping in production.
          if (!historiesRef.current[mId]) historiesRef.current[mId] = [];
          historiesRef.current[mId].push({
            ts: now, // numeric timestamp for proper X scaling
            temperature: obj.temperature,
            vibration: obj.vibration,
          });

          forceTick((x) => x + 1);
        }

        if (topic === EVENT_TOPIC) {
          const ts = obj.timestamp ? obj.timestamp * 1000 : now;
          const evt = {
            ts,
            type: obj.type,
            machine_id: obj.machine_id,
            job_id: obj.job_id,
            reason: obj.reason,
          };

          // activity log
          setEvents((prev) => [evt, ...prev].slice(0, 300));

          // last event for cooling UI
          if (evt.machine_id && evt.type) {
            lastEventRef.current[evt.machine_id] = { type: evt.type, ts: evt.ts };
          }

          // markers
          if (evt.machine_id) {
            if (evt.type === "PREDICTION") {
              if (!predictionsRef.current[evt.machine_id]) predictionsRef.current[evt.machine_id] = [];
              predictionsRef.current[evt.machine_id].push(ts);
            }
            if (evt.type === "FAILED") {
              if (!failuresRef.current[evt.machine_id]) failuresRef.current[evt.machine_id] = [];
              failuresRef.current[evt.machine_id].push(ts);
            }
          }

          // ----- queue mutations (animations) -----
          if (evt.job_id) {
            if (evt.type === "STARTED") {
              const it = ensureJob(evt.job_id, evt.machine_id);
              it.state = "idle";
              it.key = `${it.id}-${Date.now()}`; // force re-anim on re-entry
              // Bias STARTED jobs toward the front: pull to index 0 if not already close
              const arr = queueRef.current;
              const idx = arr.findIndex((j) => j.id === it.id);
              if (idx > 2) {
                arr.splice(idx, 1);
                arr.splice(1, 0, it);
              }
              touch();
            }

            if (evt.type === "STEP_DONE") {
              const it = ensureJob(evt.job_id, evt.machine_id);
              it.state = "step";
              it.key = `${it.id}-${Date.now()}`;
              touch();
              // clear pulse after a moment
              setTimeout(() => {
                it.state = "idle";
                touch();
              }, 900);
            }

            if (evt.type === "PREDICTION") {
              const it = ensureJob(evt.job_id, evt.machine_id);
              it.state = "prediction";
              it.key = `${it.id}-${Date.now()}`;
              moveToBack(it.id);
              touch();
              setTimeout(() => {
                it.state = "idle";
                touch();
              }, 1200);
            }

            if (evt.type === "FAILED") {
              const it = ensureJob(evt.job_id, evt.machine_id);
              it.state = "failed";
              it.key = `${it.id}-${Date.now()}`;
              moveToBack(it.id);
              touch();
              setTimeout(() => {
                it.state = "idle";
                touch();
              }, 900);
            }

            if (evt.type === "COMPLETED") {
              const it = ensureJob(evt.job_id, evt.machine_id);
              it.state = "completed";
              it.key = `${it.id}-${Date.now()}`;
              touch();
              setTimeout(() => {
                removeJob(it.id);
                touch();
              }, 450); // let fade animation play
            }
          }
          // ----------------------------------------
        }
      });

      clientRef.current = client;
    } catch (e) {
      console.error("Failed to connect:", e);
    }
  };

  useEffect(() => {
    connect();
    return () => {
      if (clientRef.current) {
        try { clientRef.current.end(true); } catch {}
        clientRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ---------- telemetry (full history) ----------
  const chartData = useMemo(() => {
    const arr = historiesRef.current[selectedMachine] || [];
    // keep sorted and unique by ts to avoid "zoomed in" jitter
    const sorted = arr.slice().sort((a, b) => a.ts - b.ts);
    const uniq = [];
    let lastTs = -1;
    for (const p of sorted) {
      if (p.ts === lastTs && uniq.length) {
        uniq[uniq.length - 1] = p;
      } else {
        uniq.push(p);
        lastTs = p.ts;
      }
    }
    return uniq;
  }, [selectedMachine, machines]);

  // X domain padding so the axis doesn't collapse when <2 points
  const xDomain = useMemo(() => {
    if (!chartData.length) {
      const now = Date.now();
      return [now - 2000, now + 2000];
    }
    const min = chartData[0].ts;
    const max = chartData[chartData.length - 1].ts;
    return max - min < 1500 ? [min - 750, max + 750] : [min, max];
  }, [chartData]);

  // markers aligned to nearest sample (so they sit on the curve)
  const predPoints = useMemo(() => {
    const times = dedupeTimestamps(predictionsRef.current[selectedMachine] || []);
    const hist = historiesRef.current[selectedMachine] || [];
    return times
      .map((t) => {
        const s = nearestSample(hist, t);
        if (!s) return null;
        return { ts: s.ts, temperature: s.temperature };
      })
      .filter(Boolean);
  }, [selectedMachine, machines]);

  const failPoints = useMemo(() => {
    const times = dedupeTimestamps(failuresRef.current[selectedMachine] || []);
    const hist = historiesRef.current[selectedMachine] || [];
    return times
      .map((t) => {
        const s = nearestSample(hist, t);
        if (!s) return null;
        return { ts: s.ts, temperature: s.temperature };
      })
      .filter(Boolean);
  }, [selectedMachine, machines]);

  return (
    <div className="min-h-screen bg-neutral-950 text-neutral-100">
      {/* Header */}
      <header className="sticky top-0 z-10 bg-neutral-900/70 backdrop-blur border-b border-neutral-800">
        <div className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <StatusDot online={isConnected} />
            <h1 className="text-lg font-semibold tracking-wide">MQTT Shopfloor Dashboard</h1>
          </div>
          <div className="flex items-center gap-2">
            <input
              className="bg-neutral-800 border border-neutral-700 rounded px-3 py-1 text-sm w-64"
              placeholder="ws://localhost:8083"
              value={brokerUrl}
              onChange={(e) => setBrokerUrl(e.target.value)}
            />
            <button
              onClick={connect}
              className="px-3 py-1 rounded bg-sky-600 hover:bg-sky-500 text-sm font-medium"
            >
              Connect
            </button>
          </div>
        </div>
      </header>

      {/* Main */}
      <main className="max-w-7xl mx-auto px-4 py-6 space-y-8">
        {/* Machines grid */}
        <section>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-base font-semibold text-neutral-200">Machines</h2>
            <span className="text-xs text-neutral-400">
              Topics: <code>job/status</code>, <code>jobshop/status</code>
            </span>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {machineIds.length === 0 && (
              <div className="col-span-full text-neutral-400 text-sm">
                No machines yet. Waiting for messages…
              </div>
            )}
            {machineIds.map((mId) => (
              <MachineCard
                key={mId}
                data={machines[mId]}
                lastEvent={lastEventRef.current[mId]}
                onSelect={() => setSelectedMachine(mId)}
                selected={selectedMachine === mId}
              />
            ))}
          </div>
        </section>

        {/* Single-line Queue (replaces Gantt) */}
        <QueueLine items={queue} />

        {/* Timeseries */}
        <section className="bg-neutral-900 border border-neutral-800 rounded-2xl p-4">
          <div className="flex items-center justify-between mb-3">
            <div>
              <h3 className="text-sm font-semibold text-neutral-200">Live Telemetry (Full History)</h3>
              <p className="text-xs text-neutral-400">
                Temperature &amp; Vibration over time · ▲ prediction · ✖ failure
              </p>
            </div>
            <select
              className="bg-neutral-800 border border-neutral-700 rounded px-2 py-1 text-sm"
              value={selectedMachine}
              onChange={(e) => setSelectedMachine(e.target.value)}
            >
              {machineIds.map((m) => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
          </div>
          <div className="h-80 relative">
            {chartData.length < 2 && (
              <div className="absolute top-2 right-3 text-[11px] text-neutral-500 z-10">
                waiting for more telemetry…
              </div>
            )}
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData} margin={{ top: 5, right: 20, bottom: 5, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#2a2a2a" />
                <XAxis
                  dataKey="ts"
                  type="number"
                  domain={xDomain}
                  tickFormatter={fmtClock}
                  tick={{ fill: "#b3b3b3", fontSize: 12 }}
                />
                <YAxis yAxisId="left" tick={{ fill: "#b3b3b3", fontSize: 12 }} />
                <YAxis yAxisId="right" orientation="right" tick={{ fill: "#b3b3b3", fontSize: 12 }} />
                <Tooltip
                  labelFormatter={(v) => fmtClock(v)}
                  contentStyle={{ background: "#0a0a0a", border: "1px solid #333", color: "#fff" }}
                />
                <Legend wrapperStyle={{ color: "#d4d4d4" }} />
                {/* Temperature (orange), Vibration (purple); linear to preserve exact ups/downs */}
                <Line
                  yAxisId="left"
                  type="linear"
                  dataKey="temperature"
                  dot={false}
                  stroke="#f59e0b"
                  strokeWidth={2}
                  name="Temperature"
                  connectNulls
                />
                <Line
                  yAxisId="right"
                  type="linear"
                  dataKey="vibration"
                  dot={false}
                  stroke="#a78bfa"
                  strokeWidth={3}
                  name="Vibration"
                  connectNulls
                />
                {/* Prediction markers: ▲ sky blue */}
                <Scatter
                  yAxisId="left"
                  data={predPoints}
                  name="Prediction"
                  shape={(props) => {
                    const { cx, cy } = props;
                    const s = 7;
                    return (
                      <path
                        d={`M ${cx} ${cy - s} L ${cx - s} ${cy + s} L ${cx + s} ${cy + s} Z`}
                        stroke="#60a5fa"
                        fill="#60a5fa"
                      />
                    );
                  }}
                />
                {/* Failure markers: ✖ red */}
                <Scatter
                  yAxisId="left"
                  data={failPoints}
                  name="Failure"
                  shape={(props) => {
                    const { cx, cy } = props;
                    const s = 7;
                    return (
                      <g>
                        <line x1={cx - s} y1={cy - s} x2={cx + s} y2={cy + s} stroke="#ef4444" strokeWidth="2" />
                        <line x1={cx - s} y1={cy + s} x2={cx + s} y2={cy - s} stroke="#ef4444" strokeWidth="2" />
                      </g>
                    );
                  }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </section>

        {/* Activity Log */}
        <section className="bg-neutral-900 border border-neutral-800 rounded-2xl">
          <button
            type="button"
            onClick={() => setLogOpen((o) => !o)}
            className="w-full flex items-center justify-between px-4 py-3"
            aria-expanded={logOpen}
            aria-controls="activity-log-panel"
          >
            <div className="flex items-center gap-2">
              <Chevron open={logOpen} />
              <div>
                <h3 className="text-sm font-semibold text-neutral-200">Activity Log</h3>
                <p className="text-xs text-neutral-400">
                  {events.length
                    ? `${events.length} event${events.length > 1 ? "s" : ""} · latest at ${fmtClock(events[0]?.ts)}`
                    : "No events yet"}
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-[11px] text-neutral-500 hidden sm:inline">
                click to {logOpen ? "collapse" : "expand"}
              </span>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  setEvents([]);
                }}
                className="text-xs px-2 py-1 rounded border border-neutral-700 hover:bg-neutral-800"
              >
                Clear
              </button>
            </div>
          </button>

          <div
            id="activity-log-panel"
            className={`transition-[max-height] duration-300 ease-in-out overflow-hidden ${
              logOpen ? "max-h-[28rem]" : "max-h-0"
            }`}
          >
            <div className="p-4 pt-0">
              <div className="max-h-96 overflow-auto border-t border-neutral-800">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-neutral-400">
                      <th className="py-2 pr-2">Time</th>
                      <th className="py-2 pr-2">Type</th>
                      <th className="py-2 pr-2">Machine</th>
                      <th className="py-2 pr-2">Job</th>
                      <th className="py-2 pr-2">Reason</th>
                    </tr>
                  </thead>
                  <tbody>
                    {events.length === 0 && (
                      <tr>
                        <td className="py-3 text-neutral-500" colSpan={5}>
                          No events yet…
                        </td>
                      </tr>
                    )}
                    {events.map((e, idx) => (
                      <tr key={idx} className="border-t border-neutral-900">
                        <td className="py-2 pr-2 whitespace-nowrap">{fmtClock(e.ts)}</td>
                        <td className="py-2 pr-2">
                          <span
                            className={`px-2 py-0.5 rounded text-xs font-medium ${
                              e.type === "FAILED"
                                ? "bg-rose-900/40 text-rose-300 border border-rose-800/60"
                                : e.type === "COMPLETED"
                                ? "bg-emerald-900/40 text-emerald-300 border border-emerald-800/60"
                                : e.type === "STEP_DONE"
                                ? "bg-amber-900/40 text-amber-300 border border-amber-800/60"
                                : e.type === "PREDICTION"
                                ? "bg-sky-900/40 text-sky-300 border border-sky-800/60"
                                : "bg-neutral-800/40 text-neutral-300 border border-neutral-700/60"
                            }`}
                          >
                            {e.type || "EVENT"}
                          </span>
                        </td>
                        <td className="py-2 pr-2">{e.machine_id || "-"}</td>
                        <td className="py-2 pr-2">{e.job_id || "-"}</td>
                        <td className="py-2 pr-2">{e.reason || "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </section>
      </main>
    </div>
  );
}

function MachineCard({ data, lastEvent, onSelect, selected }) {
  if (!data) return null;

  const pctTemp = clamp(
    Math.round((data.temperature / data.temp_threshold) * 100),
    0,
    999
  );
  const pctVib = clamp(
    Math.round((data.vibration / data.vib_threshold) * 100),
    0,
    999
  );

  // Cooling vs Repairing
  const isRepairText = typeof data.status === "string" && data.status.startsWith("Repairing");
  const lastType = lastEvent?.type;
  const isCooling = isRepairText && lastType === "PREDICTION";
  const isRepairing = isRepairText && lastType !== "PREDICTION";

  const progress = parseRepairProgress(data.status);
  const progressText =
    progress ? `${progress.done}/${progress.total}` : isCooling || isRepairing ? "…" : "";

  const bg =
    isCooling
      ? "bg-sky-900/40 border-sky-700"
      : isRepairing
      ? "bg-rose-900/40 border-rose-700"
      : "bg-neutral-900 border-neutral-800";

  const badge = isCooling
    ? { text: `❄️ Cooling ${progressText}`, cls: "text-sky-300" }
    : isRepairing
    ? { text: `Repairing ${progressText}`, cls: "text-rose-300" }
    : { text: "Operational", cls: "text-emerald-300" };

  const ring = selected ? "ring-2 ring-sky-500" : "ring-0";

  return (
    <button
      onClick={onSelect}
      className={`text-left ${bg} ${ring} rounded-2xl p-4 w-full border hover:border-neutral-700 transition`}
    >
      <div className="flex items-center justify-between mb-2">
        <div>
          <div className="text-sm text-neutral-400">{data.class_name}</div>
          <div className="text-lg font-semibold tracking-wide">{data.machine_id}</div>
        </div>
        <div className={`text-xs font-medium ${badge.cls}`}>{badge.text}</div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <Metric label="Temperature" value={`${data.temperature.toFixed(1)}°C`} percent={pctTemp} />
        <Metric label="Vibration" value={data.vibration.toFixed(2)} percent={pctVib} />
      </div>

      <div className="mt-3 text-xs text-neutral-400">
        Current Job:{" "}
        <span className="text-neutral-200 font-medium">{data.current_job}</span>
      </div>

      <div className="mt-2 text-[10px] text-neutral-500">
        T-thresh: {data.temp_threshold} · V-thresh: {data.vib_threshold}
      </div>

      <div className="mt-1 text-xs font-medium text-amber-300">
        ⚡ Total Energy: {data.power_kwh_total?.toFixed(2)} kWh
      </div>

      <div className="mt-2 text-[10px] text-neutral-600">
        Last seen: {fmtClock(data.lastSeen)}
      </div>
    </button>
  );
}

function Metric({ label, value, percent }) {
  const bar = clamp(percent, 0, 100);
  const barColor = bar >= 90 ? "bg-rose-500" : bar >= 70 ? "bg-amber-400" : "bg-emerald-500";
  return (
    <div>
      <div className="text-xs text-neutral-400 mb-1">{label}</div>
      <div className="text-sm font-semibold mb-1">{value}</div>
      <div className="h-2 rounded bg-neutral-800 overflow-hidden">
        <div className={`h-full ${barColor}`} style={{ width: `${Math.max(5, bar)}%` }} />
      </div>
      <div className="text-[10px] text-neutral-500 mt-1">{percent}% of threshold</div>
    </div>
  );
}
