#!/usr/bin/env python3

import argparse, json, logging, os, queue, socket, subprocess, sys
import tempfile, threading, time
from protocol import send_msg, recv_msg

log = logging.getLogger("edge")
SCRIPT = os.path.join(os.path.dirname(__file__), "solar_panel_telemetry.py")


# ── CLI ────────────────────────────────────────────────────────────────────

def parse_args():
    ap = argparse.ArgumentParser(description="Edge collector")
    ap.add_argument("--panels",      type=int,   default=10)
    ap.add_argument("--step",        type=int,   default=30)
    ap.add_argument("--start",       default="2025-06-01T09:00:00")
    ap.add_argument("--hours",       type=float, default=None)
    ap.add_argument("--minutes",     type=float, default=None)
    ap.add_argument("--seed",        type=int,   default=42)
    ap.add_argument("--broker-host", default="127.0.0.1")
    ap.add_argument("--broker-port", type=int,   default=9000)
    ap.add_argument("--buffer",
                    default=os.path.join(tempfile.gettempdir(), "solar_buffer.jsonl"),
                    help="Disk buffer file for store-and-forward")
    ap.add_argument("--compress",    action="store_true")
    return ap.parse_args()


# ── Disk buffer (store-and-forward) ───────────────────────────────────────

class DiskBuffer:
    """Append-only JSONL file. Used when broker is unreachable."""

    def __init__(self, path):
        self.path  = path
        self._lock = threading.Lock()

    def write(self, msg):
        with self._lock:
            with open(self.path, "a") as f:
                f.write(json.dumps(msg) + "\n")

    def write_batch(self, msgs):
        """Write multiple messages atomically."""
        with self._lock:
            with open(self.path, "a") as f:
                for msg in msgs:
                    f.write(json.dumps(msg) + "\n")

    def read_and_clear(self):
        """Return all buffered messages and delete the file."""
        with self._lock:
            if not os.path.exists(self.path):
                return []
            with open(self.path) as f:
                lines = f.readlines()
            os.remove(self.path)
        msgs = []
        for line in lines:
            try:
                msgs.append(json.loads(line.strip()))
            except Exception:
                pass
        return msgs

    def count(self):
        with self._lock:
            if not os.path.exists(self.path):
                return 0
            return sum(1 for _ in open(self.path))


# ── Broker connection ──────────────────────────────────────────────────────

def connect(host, port, retries=9999, delay=3.0):
    """Keep trying to connect until the broker is up."""
    for attempt in range(1, retries + 1):
        try:
            s = socket.create_connection((host, port), timeout=5)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            log.info("Connected to broker %s:%d", host, port)
            return s
        except OSError as e:
            log.warning("Broker unreachable (attempt %d): %s — retry in %.0fs", attempt, e, delay)
            time.sleep(delay)
    raise ConnectionError("Could not reach broker")


def sender_thread(host, port, buf: DiskBuffer, q: queue.Queue, compress: bool):
    """
    Background thread: maintains broker connection and sends messages.
    On disconnect — drains in-memory queue to disk buffer, then reconnects.
    """
    sock = None
    while True:
        # (Re)connect
        sock = connect(host, port)

        # Replay anything buffered on disk first
        buffered = buf.read_and_clear()
        if buffered:
            log.info("Replaying %d buffered messages...", len(buffered))
            try:
                for msg in buffered:
                    send_msg(sock, msg, compress=compress)
                log.info("Replay complete.")
            except OSError as e:
                log.warning("Replay failed mid-way: %s — re-buffering", e)
                for msg in buffered:
                    buf.write(msg)
                sock.close()
                continue

        # Live send loop
        while True:
            try:
                msg = q.get(timeout=5)
                send_msg(sock, msg, compress=compress)
            except queue.Empty:
                # Keep-alive ping
                try:
                    send_msg(sock, {"type": "PING"})
                    recv_msg(sock)
                except OSError:
                    break        # go reconnect
            except OSError as e:
                log.warning("Send error: %s — buffering queue", e)
                # Drain in-memory queue to disk
                items = [msg]  # the one that failed
                while not q.empty():
                    try: items.append(q.get_nowait())
                    except queue.Empty: break
                buf.write_batch(items)
                log.info("Drained %d messages to disk buffer", len(items))
                break           # go reconnect

        sock.close()
        time.sleep(2)


# ── Panel process → queue ──────────────────────────────────────────────────

def run_telemetry(args, q: queue.Queue, buf: DiskBuffer):
    """Spawn solar_panel_telemetry.py and push each JSONL record onto the queue."""
    cmd = [sys.executable, SCRIPT,
           "--panels", str(args.panels),
           "--step",   str(args.step),
           "--format", "jsonl",
           "--seed",   str(args.seed),
           "--start",  args.start]

    if args.hours is not None:
        cmd += ["--hours",   str(args.hours)]
    elif args.minutes is not None:
        cmd += ["--minutes", str(args.minutes)]
    else:
        cmd += ["--hours", "12"]   # default: full half-day

    log.info("Starting telemetry: %d panels, step=%ds", args.panels, args.step)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True)
    count = 0
    buffered_direct = 0
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        msg = {"type": "TELEMETRY", **data}
        try:
            q.put_nowait(msg)
        except queue.Full:
            # Queue full: spill to disk buffer instead of dropping
            buf.write(msg)
            buffered_direct += 1
            if buffered_direct % 100 == 1:
                log.warning("Queue full — spilling to disk buffer (%d so far)",
                            buffered_direct)
        count += 1
        if count % 500 == 0:
            log.info("%d readings enqueued", count)

    proc.wait()
    log.info("Telemetry process done. Total readings: %d (disk-spilled: %d)",
             count, buffered_direct)


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [EDGE] %(message)s",
                        datefmt="%H:%M:%S")

    buf = DiskBuffer(args.buffer)
    q   = queue.Queue(maxsize=10_000)

    threading.Thread(
        target=sender_thread,
        args=(args.broker_host, args.broker_port, buf, q, args.compress),
        daemon=True
    ).start()

    try:
        run_telemetry(args, q, buf)
    except KeyboardInterrupt:
        log.info("Edge collector stopped.")

    # Wait briefly for queue to flush
    time.sleep(3)
    remaining = buf.count()
    if remaining:
        log.warning("%d messages still in disk buffer (broker offline?)", remaining)


if __name__ == "__main__":
    main()