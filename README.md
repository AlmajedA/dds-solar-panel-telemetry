# Solar Fleet — Phase 1: Distributed Telemetry Pipeline

A lightweight distributed system that ingests real-time telemetry from solar panel emulators, transports it through a central message broker, and stores it for analysis.

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│  EDGE SIDE                                       │
│                                                  │
│  solar_panel_telemetry.py ──JSONL──► edge_       │
│     (N panels, stdout)               collector   │
│                                       │          │
│                          disk buffer  │ TCP 9000 │
│                          if offline   │          │
└───────────────────────────────────────┼──────────┘
                                        │ PUBLISH
                              ┌─────────▼─────────┐
                              │     broker.py     │
                              │  pub port : 9000  │
                              │  sub port : 9001  │
                              │  fan-out to all   │
                              │  subscribers      │
                              └─────────┬─────────┘
                                        │ SUBSCRIBE
                              ┌─────────▼─────────┐
                              │   collector.py    │
                              │  persists JSONL   │
                              │  live KPI stats   │
                              └───────────────────┘
```

### Components

| File | Role |
|------|------|
| `solar_panel_telemetry.py` | Emulates N solar panels → JSONL on stdout |
| `broker.py` | Central pub/sub broker over TCP |
| `edge_collector.py` | Edge agent: reads panel output, forwards to broker (with store-and-forward) |
| `collector.py` | Central sink: subscribes to broker, saves data, prints stats |
| `protocol.py` | Shared wire protocol (length-prefixed JSON + optional zlib compression) |
| `test_phase1.py` | End-to-end integration test |
| `benchmark_phase1.py` | Throughput and latency stress test |

---

## Design Justification

### Why TCP Sockets over alternatives?

We evaluated four communication options for the edge-to-central transport:

| Option | Pros | Cons |
|--------|------|------|
| **Raw TCP sockets** | Zero dependencies; works on any Python 3.8+ install; full control over framing, compression, and backpressure; trivial to deploy in rural/constrained environments | More boilerplate; no built-in auth |
| HTTP REST | Ubiquitous; easy debugging with curl | Per-request overhead; not ideal for continuous streaming; requires a web framework |
| MQTT (Paho) | Purpose-built for IoT telemetry; QoS levels | External broker dependency (Mosquitto); additional install steps; overkill for a single-site pipeline |
| gRPC | Strong typing; bi-directional streaming | Heavy protobuf toolchain; complex setup; poor fit for constrained edge devices |

**Decision**: Raw TCP sockets. The project constraints explicitly require operation in rural environments with no guaranteed internet and no external dependencies. A custom TCP protocol gives us the smallest deployment footprint (zero pip installs), full control over compression and framing, and the ability to implement store-and-forward without fighting a framework's assumptions. The wire format (4-byte length prefix + JSON + optional zlib) is simple enough to debug with standard tools, yet efficient enough to sustain >5k messages/minute on commodity hardware.

### Why Publish/Subscribe?

The broker implements a fan-out pub/sub pattern rather than point-to-point messaging:

- **Scalability**: Multiple edge collectors can publish simultaneously (e.g., multiple field sites), and multiple collectors can subscribe (e.g., one for storage, one for alerting, one for the dashboard in Phase 2) without any code changes.
- **Decoupling**: Publishers and subscribers are unaware of each other. Adding a new consumer requires zero changes to the edge or broker.
- **Resilience**: If a subscriber disconnects, the broker drops messages for that subscriber only; other subscribers are unaffected. Subscribers reconnect and re-subscribe transparently.

### Why Store-and-Forward on the Edge?

Rural solar sites face intermittent connectivity. Our edge collector addresses this with a two-tier buffering strategy:

1. **In-memory queue** (10,000 messages): Absorbs short bursts when the sender thread is temporarily behind.
2. **Disk buffer** (JSONL file): When the broker is unreachable OR when the in-memory queue is full, messages spill to a local file. On reconnect, the sender thread replays all buffered messages before resuming live forwarding.

This guarantees **zero message loss** during network outages of any duration, bounded only by available disk space.

### Wire Protocol Design

```
┌──────────────┬──────┬─────────────────────┐
│ 4-byte length│ flag │   JSON payload      │
│ (big-endian) │ 0/1  │  (raw or zlib)      │
└──────────────┴──────┴─────────────────────┘
```

- **Length prefix**: Enables reliable message framing over TCP's byte stream. The receiver always knows exactly how many bytes to read.
- **Compression flag** (1 byte): `\x00` = raw JSON, `\x01` = zlib-compressed. Compression is optional and only applied to payloads >256 bytes, avoiding overhead on small messages.
- **2 MB frame limit**: Protects against memory exhaustion from malformed or malicious frames.
- **Message types**: `TELEMETRY`, `SUBSCRIBE`, `ACK`, `PING`, `PONG` — minimal set sufficient for the pub/sub pattern plus keep-alive health checking.

### Data Flow

```
1. solar_panel_telemetry.py generates JSONL records on stdout
2. edge_collector.py reads each line, wraps it as {"type":"TELEMETRY", ...}
3. sender_thread sends to broker via TCP (or buffers to disk if offline)
4. broker.py fans out each TELEMETRY message to all subscribed collectors
5. collector.py appends each message to a JSONL file and updates live KPIs
```

---

## Setup

Python 3.8+ only — **no pip installs required**.

```bash
git clone <your-repo>
cd solar_fleet
```

---

## Running

Open **three terminals** in the `solar_fleet/` folder:

### Terminal 1 — Broker
```bash
python3 broker.py
```

### Terminal 2 — Collector
```bash
python3 collector.py
# Data saved to telemetry_store.jsonl
# Live stats printed every 10s
```

### Terminal 3 — Edge Collector
```bash
# 10 panels, 30-second steps, 2-hour daytime sim
python3 edge_collector.py --panels 10 --step 30 \
  --start "2025-06-01T09:00:00" --hours 2

# 100 panels with compression (for bandwidth-limited links)
python3 edge_collector.py --panels 100 --step 60 --compress
```

### Expected output (collector terminal)
```
[09:00:40] panels=10  strings=1  power=3.21kW  faults=0  rate=2.1msg/s  total=21
[09:00:50] panels=10  strings=1  power=3.18kW  faults=1  rate=2.0msg/s  total=41
```

---

## Testing

### Integration Test
```bash
python3 test_phase1.py
```
Runs all components, checks message flow, validates stored data, and tests store-and-forward recovery. Takes ~35 seconds.

### Store-and-Forward Manual Test
```bash
# 1. Start all three components
# 2. Kill the broker (Ctrl+C in broker terminal)
# 3. Watch edge_collector log: "Broker unreachable — retry in 3s"
#    and "Drained N messages to disk buffer"
# 4. Restart the broker
# 5. Watch edge_collector log: "Replaying N buffered messages..."
```

### Performance Benchmark
```bash
# Default: 100 panels, 60-minute sim, 1 subscriber
python3 benchmark_phase1.py

# Heavy load: 500 panels
python3 benchmark_phase1.py --panels 500

# Fan-out test: 3 subscribers
python3 benchmark_phase1.py --subscribers 3

# With compression
python3 benchmark_phase1.py --panels 200 --compress
```

The benchmark measures:
- **Throughput**: messages/second and messages/minute through the full pipeline (target: ≥5,000 msg/min)
- **End-to-end latency**: time from edge send to collector write (target: <5s at p99)
- **Message loss**: percentage of sent messages not received by each collector
- **Fan-out overhead**: how throughput scales with multiple subscribers

---

## CLI Reference

### broker.py
| Flag | Default | Description |
|------|---------|-------------|
| `--pub-port` | 9000 | Port for publisher connections |
| `--sub-port` | 9001 | Port for subscriber connections |
| `--host` | 0.0.0.0 | Bind address |

### edge_collector.py
| Flag | Default | Description |
|------|---------|-------------|
| `--panels` | 10 | Number of panels to simulate |
| `--step` | 30 | Seconds between readings |
| `--start` | 2025-06-01T09:00:00 | Sim start time (daytime for power) |
| `--hours` / `--minutes` | — | Simulation duration |
| `--broker-host` | 127.0.0.1 | Broker address |
| `--broker-port` | 9000 | Broker publish port |
| `--buffer` | /tmp/solar_buffer.jsonl | Disk buffer path |
| `--compress` | off | Enable zlib compression |

### collector.py
| Flag | Default | Description |
|------|---------|-------------|
| `--broker-host` | 127.0.0.1 | Broker address |
| `--broker-port` | 9001 | Broker subscribe port |
| `--out` | telemetry_store.jsonl | Output file |
| `--stats-interval` | 10 | Stats print interval (seconds) |

### benchmark_phase1.py
| Flag | Default | Description |
|------|---------|-------------|
| `--panels` | 100 | Number of panels to simulate |
| `--step` | 1 | Seconds between readings (lower = more load) |
| `--duration` | 60 | Simulation duration in minutes |
| `--subscribers` | 1 | Number of collector subscribers |
| `--compress` | off | Enable zlib compression |

---

## Data Format (stored JSONL)

Each line is a JSON object:
```json
{
  "type": "TELEMETRY",
  "timestamp_utc": "2025-06-01T09:00:00Z",
  "panel_id": "P00001",
  "string_id": "S01",
  "status": "OK",
  "fault": "NONE",
  "power_w": 161.28,
  "voltage_v": 33.25,
  "current_a": 4.851,
  "irradiance_wm2": 434.7,
  "ambient_temp_c": 33.73,
  "cell_temp_c": 44.59,
  "orientation_deg": 205.9,
  "tilt_deg": 28.1
}
```

---

## Project Structure

```
solar_fleet/
├── solar_panel_telemetry.py   # Provided panel emulator (unchanged)
├── broker.py                  # Central pub/sub broker
├── edge_collector.py          # Edge agent (with store-and-forward)
├── collector.py               # Central data sink
├── protocol.py                # Shared wire protocol
├── test_phase1.py             # End-to-end integration test
├── benchmark_phase1.py        # Throughput & latency benchmark
└── README.md
```
