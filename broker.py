#!/usr/bin/env python3


import argparse, logging, queue, socket, threading, time
from protocol import send_msg, recv_msg

log = logging.getLogger("broker")

# ── Shared subscriber registry ─────────────────────────────────────────────

_lock        = threading.Lock()
_subscribers = {}          # id → queue.Queue
_next_id     = 0
_stats = {
    "received": 0, 
    "forwarded": 0,
    "queue_full_drops": 0,      
    "subscriber_errors": 0       
}

def _add_sub():
    global _next_id
    with _lock:
        sid = _next_id; _next_id += 1
        q = queue.Queue(maxsize=5_000)
        _subscribers[sid] = q
    return sid, q


def _remove_sub(sid):
    with _lock:
        _subscribers.pop(sid, None)


def _fanout(msg):
    """Deliver msg to every subscriber (drop if their queue is full)."""
    with _lock:
        subs = list(_subscribers.items())
    _stats["received"] += 1
    for sid, q in subs:
        try:
            q.put_nowait(msg)
            _stats["forwarded"] += 1
        except queue.Full:
            log.warning("Subscriber %d queue full — message dropped", sid)
            _stats["queue_full_drops"] += 1

# ── Connection handlers ────────────────────────────────────────────────────

def handle_publisher(sock, addr):
    log.info("Publisher connected: %s", addr)
    try:
        while True:
            msg = recv_msg(sock)
            if msg.get("type") == "PING":
                send_msg(sock, {"type": "PONG"})
            elif msg.get("type") == "TELEMETRY":
                _fanout(msg)
    except ValueError as e:
        log.error("Protocol error in publisher %s: %s", addr, e)
        _stats["subscriber_errors"] += 1
    finally:
        sock.close()


def handle_subscriber(sock, addr):
    sid = None
    try:
        # Expect a SUBSCRIBE handshake first
        msg = recv_msg(sock)
        if msg.get("type") != "SUBSCRIBE":
            log.warning("Bad handshake from %s: %s", addr, msg)
            return

        sid, q = _add_sub()
        send_msg(sock, {"type": "ACK", "subscriber_id": sid})
        log.info("Subscriber #%d connected: %s", sid, addr)

        while True:
            try:
                out = q.get(timeout=5)
            except queue.Empty:
                try:
                    send_msg(sock, {"type": "PING"})   # keep-alive
                except (BrokenPipeError, OSError):
                    log.info("Subscriber #%d disconnected (broken pipe)", sid)
                    break
                continue
            send_msg(sock, out, compress=False) # why recompress in broker?
    except (ConnectionError, OSError) as e:
        log.info("Subscriber #%d gone: %s", sid, e)
    except ValueError as e:
        log.error("Protocol error in subscriber #%d: %s", sid, e)
        _stats["subscriber_errors"] += 1
    finally:
        if sid is not None:
            _remove_sub(sid)
        sock.close()


# ── Accept loops ───────────────────────────────────────────────────────────

def accept_loop(server_sock, handler):
    while True:
        try:
            client, addr = server_sock.accept()
            threading.Thread(target=handler, args=(client, addr), daemon=True).start()
        except OSError:
            break


def stats_loop(interval=10):
    while True:
        time.sleep(interval)
        with _lock:
            n = len(_subscribers)
        log.info(
            "stats - received=%d forwarded=%d subscribers=%d "
            "queue_drops=%d errors=%d",
            _stats["received"],
            _stats["forwarded"],
            n,
            _stats["queue_full_drops"],
            _stats["subscriber_errors"]
        )

# ── Main ───────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Solar fleet broker")
    ap.add_argument("--pub-port", type=int, default=9000)
    ap.add_argument("--sub-port", type=int, default=9001)
    ap.add_argument("--host",     default="127.0.0.1")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [BROKER] %(message)s",
                        datefmt="%H:%M:%S")

    def make_server(port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((args.host, port)); s.listen(64)
        return s

    pub_srv = make_server(args.pub_port)
    sub_srv = make_server(args.sub_port)

    log.info("Broker ready — publishers :%d  subscribers :%d",
             args.pub_port, args.sub_port)

    threading.Thread(target=accept_loop, args=(pub_srv, handle_publisher), daemon=True).start()
    threading.Thread(target=accept_loop, args=(sub_srv, handle_subscriber), daemon=True).start()
    threading.Thread(target=stats_loop, daemon=True).start()

    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()
