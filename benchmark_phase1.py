#!/usr/bin/env python3

import argparse, json, os, socket, subprocess, sys, tempfile, time, threading
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
PY   = sys.executable

# ── Helpers ────────────────────────────────────────────────────────────────

def kill(p):
    if p and p.poll() is None:
        p.terminate()
        try: p.wait(timeout=3)
        except: p.kill()


def count_jsonl(path):
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return sum(1 for line in f if line.strip())


def tail_jsonl_timestamps(path, last_n=200):
    """Return list of (send_ts, receive_ts) from the last N records."""
    if not os.path.exists(path):
        return []
    pairs = []
    with open(path) as f:
        lines = f.readlines()
    for line in lines[-last_n:]:
        try:
            obj = json.loads(line)
            if "_bench_send_epoch" in obj and "_bench_recv_epoch" in obj:
                pairs.append((obj["_bench_send_epoch"], obj["_bench_recv_epoch"]))
        except Exception:
            pass
    return pairs


# ── Latency-instrumented collector ─────────────────────────────────────────

def instrumented_collector(broker_host, broker_port, out_path, stop_event):
    """A lightweight collector that stamps receive time on each message."""
    from protocol import send_msg, recv_msg
    import socket as _socket

    while not stop_event.is_set():
        try:
            s = _socket.create_connection((broker_host, broker_port), timeout=5)
            s.setsockopt(_socket.IPPROTO_TCP, _socket.TCP_NODELAY, 1)
            send_msg(s, {"type": "SUBSCRIBE", "topic": "telemetry"})
            recv_msg(s)  # ACK

            with open(out_path, "a") as f:
                while not stop_event.is_set():
                    try:
                        msg = recv_msg(s)
                    except Exception:
                        break
                    if msg.get("type") == "PING":
                        send_msg(s, {"type": "PONG"})
                        continue
                    if msg.get("type") != "TELEMETRY":
                        continue
                    msg["_bench_recv_epoch"] = time.time()
                    f.write(json.dumps(msg) + "\n")
                    f.flush()
            s.close()
        except OSError:
            if not stop_event.is_set():
                time.sleep(1)


# ── Latency-instrumented edge sender ───────────────────────────────────────

def instrumented_edge(panels, step, broker_host, broker_port, duration_min, compress):
    """Edge that injects _bench_send_epoch into every message before sending."""
    from protocol import send_msg, recv_msg
    import queue

    cmd = [PY, os.path.join(ROOT, "solar_panel_telemetry.py"),
           "--panels", str(panels), "--step", str(step),
           "--format", "jsonl", "--seed", "42",
           "--start", "2025-06-01T09:00:00",
           "--minutes", str(duration_min)]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True)

    sock = None
    for attempt in range(30):
        try:
            sock = socket.create_connection((broker_host, broker_port), timeout=5)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            break
        except OSError:
            time.sleep(1)
    if sock is None:
        proc.kill()
        return 0

    count = 0
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        msg = {"type": "TELEMETRY", "_bench_send_epoch": time.time(), **data}
        try:
            send_msg(sock, msg, compress=compress)
            count += 1
        except OSError:
            break

    proc.wait()
    sock.close()
    return count


# ── Main benchmark ─────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Phase 1 benchmark")
    ap.add_argument("--panels",      type=int, default=100)
    ap.add_argument("--step",        type=int, default=1,
                    help="Telemetry step in seconds (lower = more msgs)")
    ap.add_argument("--duration",    type=int, default=60,
                    help="Simulation duration in minutes")
    ap.add_argument("--subscribers", type=int, default=1,
                    help="Number of collector subscribers for fan-out test")
    ap.add_argument("--compress",    action="store_true")
    ap.add_argument("--pub-port",    type=int, default=18000)
    ap.add_argument("--sub-port",    type=int, default=18001)
    args = ap.parse_args()

    print("\n" + "=" * 60)
    print("  Phase 1 — Performance Benchmark")
    print("=" * 60)
    print(f"  Panels: {args.panels}  Step: {args.step}s  "
          f"Duration: {args.duration}min  Subscribers: {args.subscribers}")
    print(f"  Compression: {'ON' if args.compress else 'OFF'}")
    print("=" * 60)

    broker = None
    stop = threading.Event()

    with tempfile.TemporaryDirectory() as tmp:
        store_paths = [os.path.join(tmp, f"store_{i}.jsonl")
                       for i in range(args.subscribers)]

        try:
            # Start broker
            print("\n▸ Starting broker...")
            broker = subprocess.Popen(
                [PY, os.path.join(ROOT, "broker.py"),
                 "--pub-port", str(args.pub_port),
                 "--sub-port", str(args.sub_port)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1.5)

            # Start instrumented collectors
            print(f"▸ Starting {args.subscribers} collector(s)...")
            coll_threads = []
            for i in range(args.subscribers):
                t = threading.Thread(
                    target=instrumented_collector,
                    args=("127.0.0.1", args.sub_port, store_paths[i], stop),
                    daemon=True)
                t.start()
                coll_threads.append(t)
            time.sleep(1.5)

            # Run instrumented edge
            print(f"▸ Sending telemetry ({args.panels} panels × {args.duration}min)...")
            t_start = time.time()
            sent = instrumented_edge(
                args.panels, args.step,
                "127.0.0.1", args.pub_port,
                args.duration, args.compress)
            elapsed = time.time() - t_start

            # Allow pipeline to drain
            time.sleep(3)
            stop.set()
            time.sleep(1)

            # ── Results ────────────────────────────────────────────────────
            print("\n" + "─" * 60)
            print("  RESULTS")
            print("─" * 60)

            print(f"\n  Messages sent by edge:    {sent}")
            print(f"  Wall-clock time:          {elapsed:.1f}s")
            if elapsed > 0:
                send_rate = sent / elapsed
                send_rate_min = send_rate * 60
                print(f"  Send throughput:          {send_rate:.1f} msg/s "
                      f"({send_rate_min:.0f} msg/min)")

                target = 5000  # msg/min target from spec
                status = "✓ PASS" if send_rate_min >= target else "✗ BELOW TARGET"
                print(f"  vs. 5k msg/min target:    {status}")

            for i, path in enumerate(store_paths):
                received = count_jsonl(path)
                loss = 0.0
                if sent > 0:
                    loss = (1 - received / sent) * 100
                print(f"\n  Collector #{i}: received={received}  "
                      f"loss={loss:.2f}%")

                # Latency analysis
                pairs = tail_jsonl_timestamps(path)
                if pairs:
                    latencies = [r - s for s, r in pairs]
                    avg_ms = sum(latencies) / len(latencies) * 1000
                    p50 = sorted(latencies)[len(latencies) // 2] * 1000
                    p99 = sorted(latencies)[int(len(latencies) * 0.99)] * 1000
                    mx  = max(latencies) * 1000
                    print(f"    Latency (edge→collector):  "
                          f"avg={avg_ms:.1f}ms  p50={p50:.1f}ms  "
                          f"p99={p99:.1f}ms  max={mx:.1f}ms")
                    e2e_ok = "✓ PASS" if p99 < 5000 else "✗ ABOVE 5s"
                    print(f"    vs. <5s e2e target (p99):  {e2e_ok}")

            print("\n" + "=" * 60 + "\n")

        finally:
            stop.set()
            kill(broker)


if __name__ == "__main__":
    main()
