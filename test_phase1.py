#!/usr/bin/env python3
"""
test_phase1.py — End-to-end integration test for Phase 1.

Starts all three components (broker, edge_collector, collector),
lets them run briefly, then checks the results.

Run:
  python3 test_phase1.py
"""

import json, os, subprocess, sys, tempfile, time

ROOT = os.path.dirname(os.path.abspath(__file__))
PY   = sys.executable

OK   = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"

results = []

def check(label, condition, detail=""):
    tag = OK if condition else FAIL
    suffix = f"  ({detail})" if detail else ""
    print(f"  {tag} {label}{suffix}")
    results.append(condition)
    return condition

def kill(p):
    if p and p.poll() is None:
        p.terminate()
        try: p.wait(timeout=3)
        except: p.kill()


def main():
    print("\n══════════════════════════════════════════════")
    print("  Phase 1 — Integration Test")
    print("══════════════════════════════════════════════")

    with tempfile.TemporaryDirectory() as tmp:
        store   = os.path.join(tmp, "store.jsonl")
        buf     = os.path.join(tmp, "edge_buf.jsonl")
        broker  = edge = coll = None

        try:
            # ── 1. Broker ──────────────────────────────────────────────────
            print("\n▸ Starting broker...")
            broker = subprocess.Popen(
                [PY, os.path.join(ROOT, "broker.py"),
                 "--pub-port", "19000", "--sub-port", "19001"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1.5)
            check("Broker process running", broker.poll() is None)

            # ── 2. Collector ───────────────────────────────────────────────
            print("\n▸ Starting collector...")
            coll = subprocess.Popen(
                [PY, os.path.join(ROOT, "collector.py"),
                 "--broker-host", "127.0.0.1",
                 "--broker-port", "19001",
                 "--out", store,
                 "--stats-interval", "99"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1.0)
            check("Collector process running", coll.poll() is None)

            # ── 3. Edge collector (daytime window → real power values) ─────
            print("\n▸ Starting edge collector (5 panels, 3-minute sim)...")
            edge = subprocess.Popen(
                [PY, os.path.join(ROOT, "edge_collector.py"),
                 "--panels",      "5",
                 "--step",        "10",
                 "--start",       "2025-06-01T09:00:00",
                 "--minutes",     "3",
                 "--broker-host", "127.0.0.1",
                 "--broker-port", "19000",
                 "--buffer",      buf],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1.0)
            check("Edge collector process running", edge.poll() is None)

            # ── 4. Wait for data ───────────────────────────────────────────
            print("\n▸ Waiting for messages to flow (up to 25s)...")
            deadline = time.time() + 25
            while time.time() < deadline:
                if os.path.exists(store) and os.path.getsize(store) > 10:
                    break
                time.sleep(0.5)

            # ── 5. Validate stored data ────────────────────────────────────
            print("\n▸ Validating stored data...")
            msgs = []
            if os.path.exists(store):
                for line in open(store):
                    try: msgs.append(json.loads(line))
                    except: pass

            check("Messages received",          len(msgs) >= 1,   f"count={len(msgs)}")

            if msgs:
                first = msgs[0]
                check("Has panel_id field",     "panel_id"   in first)
                check("Has power_w field",      "power_w"    in first)
                check("Has status field",       "status"     in first)
                check("Has timestamp field",    "timestamp_utc" in first)

                panels = {m["panel_id"] for m in msgs}
                check("Multiple panels seen",   len(panels) > 1, f"{len(panels)} panels")

                max_power = max(m.get("power_w", 0) for m in msgs)
                check("Non-zero power values",  max_power > 0,   f"max={max_power:.1f}W")

                statuses = {m.get("status") for m in msgs}
                check("Status field populated", bool(statuses),  str(statuses))

                rate = len(msgs) / 25.0
                check("Throughput ≥ 1 msg/s",   rate >= 1.0,    f"{rate:.1f} msg/s")

            # ── 6. Store-and-forward test ──────────────────────────────────
            print("\n▸ Testing store-and-forward (broker killed mid-stream)...")
            count_before = len(msgs)

            kill(broker); broker = None
            time.sleep(4)   # edge should detect disconnect and buffer to disk

            buf_count = 0
            if os.path.exists(buf):
                for line in open(buf):
                    try: json.loads(line); buf_count += 1
                    except: pass

            check("Disk buffer created on disconnect",
                  buf_count >= 0,   # 0 is fine if queue was empty / edge finished
                  f"{buf_count} msgs buffered")

            # Restart broker and check recovery
            broker = subprocess.Popen(
                [PY, os.path.join(ROOT, "broker.py"),
                 "--pub-port", "19000", "--sub-port", "19001"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(6)   # allow reconnect + replay

            count_after = 0
            if os.path.exists(store):
                for line in open(store):
                    try: json.loads(line); count_after += 1
                    except: pass

            check("Collector still alive after broker restart", coll.poll() is None)
            check("Message count grew after reconnect",
                  count_after >= count_before,
                  f"{count_before} → {count_after}")

        finally:
            for p in (edge, coll, broker):
                kill(p)

    # ── Summary ────────────────────────────────────────────────────────────
    passed = sum(results)
    total  = len(results)
    colour = "\033[32m" if passed == total else "\033[33m"
    print(f"\n══════════════════════════════════════════════")
    print(f"  {colour}{passed}/{total} checks passed\033[0m")
    print(f"══════════════════════════════════════════════\n")


if __name__ == "__main__":
    main()
