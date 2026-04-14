#!/usr/bin/env python3


import argparse, json, logging, os, socket, sys, threading, time
from collections import defaultdict
from datetime import datetime
from protocol import send_msg, recv_msg

log = logging.getLogger("collector")


# ── CLI ────────────────────────────────────────────────────────────────────

def parse_args():
    ap = argparse.ArgumentParser(description="Central collector")
    ap.add_argument("--broker-host",    default="127.0.0.1")
    ap.add_argument("--broker-port",    type=int, default=9001)
    ap.add_argument("--out",            default="telemetry_store.jsonl")
    ap.add_argument("--stats-interval", type=int, default=10)
    return ap.parse_args()


# ── Live stats tracker ─────────────────────────────────────────────────────

class Stats:
    def __init__(self):
        self._lock    = threading.Lock()
        self.total    = 0
        self.latest   = {}         # panel_id → last message
        self._since   = time.time()
        self._window  = 0

    def record(self, msg):
        with self._lock:
            self.total   += 1
            self._window += 1
            self.latest[msg.get("panel_id", "?")] = msg

    def summary(self):
        with self._lock:
            elapsed = max(time.time() - self._since, 1)
            rate    = self._window / elapsed
            self._window = 0
            self._since  = time.time()

            n_panels = len(self.latest)
            live_kw  = sum(m.get("power_w", 0) for m in self.latest.values()) / 1000.0
            faults   = sum(1 for m in self.latest.values() if m.get("status") != "OK")
            strings  = {m.get("string_id") for m in self.latest.values()}

        return (f"panels={n_panels}  strings={len(strings)}  "
                f"power={live_kw:.2f}kW  faults={faults}  "
                f"rate={rate:.1f}msg/s  total={self.total}")


# ── Broker connection ──────────────────────────────────────────────────────

def connect(host, port):
    while True:
        try:
            s = socket.create_connection((host, port), timeout=5)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return s
        except OSError as e:
            log.warning("Cannot reach broker: %s — retrying in 3s", e)
            time.sleep(3)


# ── Main collect loop ──────────────────────────────────────────────────────

def collect(args, stats: Stats):
    while True:
        sock = None
        try:
            sock = connect(args.broker_host, args.broker_port)

            # Subscribe handshake
            send_msg(sock, {"type": "SUBSCRIBE", "topic": "telemetry"})
            ack = recv_msg(sock)
            sid = ack.get("subscriber_id", "?")
            log.info("Subscribed as #%s to broker %s:%d → writing to %s",
                     sid, args.broker_host, args.broker_port, args.out)

            with open(args.out, "a") as f:
                while True:
                    msg = recv_msg(sock)

                    if msg.get("type") == "PING":
                        send_msg(sock, {"type": "PONG"})
                        continue

                    if msg.get("type") != "TELEMETRY":
                        continue

                    # Persist and track
                    f.write(json.dumps(msg) + "\n")
                    f.flush()
                    stats.record(msg)

        except (ConnectionError, OSError) as e:
            log.warning("Disconnected: %s — reconnecting in 5s", e)
            if sock: sock.close()
            time.sleep(5)


# ── Stats printer ──────────────────────────────────────────────────────────

def print_stats(stats: Stats, interval: int):
    while True:
        time.sleep(interval)
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] {stats.summary()}")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [COLLECTOR] %(message)s",
                        datefmt="%H:%M:%S")

    stats = Stats()
    threading.Thread(target=print_stats, args=(stats, args.stats_interval),
                     daemon=True).start()

    try:
        collect(args, stats)
    except KeyboardInterrupt:
        log.info("Stopped. Total messages: %d", stats.total)


if __name__ == "__main__":
    main()
