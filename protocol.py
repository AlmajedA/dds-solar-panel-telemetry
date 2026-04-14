import json, socket, struct, zlib

_HEADER = struct.Struct("!I")   # 4-byte unsigned int, big-endian


def send_msg(sock: socket.socket, msg: dict, compress=False) -> None:
    """Serialize dict and send as a length-prefixed frame."""
    body = json.dumps(msg).encode()
    if compress and len(body) > 256:
        body = b"\x01" + zlib.compress(body, level=1)
    else:
        body = b"\x00" + body
    sock.sendall(_HEADER.pack(len(body)) + body)


def recv_msg(sock: socket.socket) -> dict:
    """Receive one complete frame and return as dict."""
    header = _recv_exactly(sock, _HEADER.size)
    if not header:
        raise ConnectionError("Connection closed")
    (length,) = _HEADER.unpack(header)
    if length > 2_000_000:
        raise ValueError(f"Frame too large: {length}")
    body = _recv_exactly(sock, length)
    flag, payload = body[0:1], body[1:]
    if flag == b"\x01":
        payload = zlib.decompress(payload)
    return json.loads(payload)


def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return b""
        buf += chunk
    return buf
