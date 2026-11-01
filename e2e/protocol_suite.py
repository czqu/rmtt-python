"""Raw-wire protocol conformance suite.

Drives each server implementation with hand-built frames to check the
protocol-level behaviours that an echo round trip does not cover: CONNACK
codes, keepalive negotiation, heartbeat, framing violations, session takeover
and authentication rejection.

Usage:
    python e2e/protocol_suite.py [--out e2e/results]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import struct
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from matrix import (
    RUST_SERVER,
    ServerProcess,
    _free_port,
)

MAGIC = 0x637A7175


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        digit = n % 128
        n //= 128
        if n:
            digit |= 0x80
        out.append(digit)
        if not n:
            return bytes(out)


def frame(msg_type: int, body: bytes) -> bytes:
    return bytes([msg_type << 4]) + _varint(len(body)) + body


def connect_frame(credential: str, keepalive: int = 30, version: int = 1, magic: int = MAGIC) -> bytes:
    body = struct.pack(">I", magic) + bytes([version, 0]) + struct.pack(">H", keepalive)
    body += struct.pack(">H", len(credential.encode())) + credential.encode()
    return frame(1, body)


async def read_packet(reader: asyncio.StreamReader, timeout: float = 5.0):
    first = await asyncio.wait_for(reader.readexactly(1), timeout)
    remaining = 0
    multiplier = 1
    while True:
        digit = (await asyncio.wait_for(reader.readexactly(1), timeout))[0]
        remaining += (digit & 0x7F) * multiplier
        if not digit & 0x80:
            break
        multiplier *= 128
    body = await asyncio.wait_for(reader.readexactly(remaining), timeout) if remaining else b""
    return first[0] >> 4, body


@dataclass
class Check:
    server: str
    name: str
    status: str
    observed: str


async def _open(port: int):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    return reader, writer


async def check_connect_and_keepalive(port: int):
    reader, writer = await _open(port)
    try:
        writer.write(connect_frame("probe-kp", keepalive=45))
        await writer.drain()
        msg_type, body = await read_packet(reader)
        assert msg_type == 2, f"expected CONNACK, got type {msg_type}"
        rc = body[0]
        server_kp = struct.unpack(">H", body[1:3])[0]
        return rc == 0 and 30 <= server_kp <= 600, f"rc={rc} server_kp={server_kp}"
    finally:
        writer.close()


async def check_bad_version(port: int):
    reader, writer = await _open(port)
    try:
        writer.write(connect_frame("probe-ver", version=2))
        await writer.drain()
        msg_type, body = await read_packet(reader)
        return msg_type == 2 and body[0] == 0x01, f"type={msg_type} rc={body[0] if body else None}"
    finally:
        writer.close()


async def check_bad_magic(port: int):
    reader, writer = await _open(port)
    try:
        writer.write(connect_frame("probe-magic", magic=0xDEADBEEF))
        await writer.drain()
        try:
            msg_type, body = await read_packet(reader, timeout=2)
            return False, f"unexpected reply type={msg_type} body={body.hex()}"
        except (asyncio.IncompleteReadError, ConnectionError):
            return True, "closed without reply"
        except asyncio.TimeoutError:
            return True, "no reply (connection left open)"
    finally:
        writer.close()


async def check_ping(port: int):
    reader, writer = await _open(port)
    try:
        writer.write(connect_frame("probe-ping"))
        await writer.drain()
        await read_packet(reader)
        writer.write(frame(5, b""))
        await writer.drain()
        msg_type, _ = await read_packet(reader)
        return msg_type == 6, f"type={msg_type}"
    finally:
        writer.close()


async def check_protocol_violation(port: int):
    reader, writer = await _open(port)
    try:
        writer.write(connect_frame("probe-violation"))
        await writer.drain()
        await read_packet(reader)
        writer.write(bytes([0x31, 0x00]))  # PUSH with non-zero flags
        await writer.drain()
        try:
            msg_type, body = await read_packet(reader, timeout=2)
        except (asyncio.IncompleteReadError, ConnectionError):
            return True, "connection closed without DISCONNECT"
        if msg_type == 14:
            return True, f"DISCONNECT reason=0x{body[0]:02x}"
        return False, f"unexpected type={msg_type}"
    finally:
        writer.close()


async def check_unknown_type(port: int):
    reader, writer = await _open(port)
    try:
        writer.write(connect_frame("probe-unknown"))
        await writer.drain()
        await read_packet(reader)
        writer.write(bytes([0x00, 0x00]))  # reserved packet type 0
        await writer.drain()
        try:
            msg_type, body = await read_packet(reader, timeout=2)
        except (asyncio.IncompleteReadError, ConnectionError):
            return True, "connection closed without DISCONNECT"
        if msg_type == 14:
            return True, f"DISCONNECT reason=0x{body[0]:02x}"
        return False, f"unexpected type={msg_type}"
    finally:
        writer.close()


async def check_keepalive_timeout(port: int):
    reader, writer = await _open(port)
    try:
        writer.write(connect_frame("probe-timeout", keepalive=1))
        await writer.drain()
        msg_type, body = await read_packet(reader)
        server_kp = struct.unpack(">H", body[1:3])[0]
        deadline = time.monotonic() + max(6.0, server_kp * 1.5 + 3)
        saw = None
        while time.monotonic() < deadline:
            try:
                msg_type, body = await read_packet(reader, timeout=deadline - time.monotonic())
            except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError):
                break
            if msg_type == 14:
                saw = body[0]
                break
        return saw == 0x05, f"server_kp={server_kp} disconnect=0x{saw:02x}" if saw is not None else f"server_kp={server_kp} no DISCONNECT"
    finally:
        writer.close()


async def check_takeover(port: int):
    first_r, first_w = await _open(port)
    second_r, second_w = await _open(port)
    try:
        first_w.write(connect_frame("probe-same"))
        await first_w.drain()
        await read_packet(first_r)
        second_w.write(connect_frame("probe-same"))
        await second_w.drain()
        msg_type, body = await read_packet(second_r)
        assert msg_type == 2 and body[0] == 0, "second connection not accepted"
        reason = None
        try:
            msg_type, body = await read_packet(first_r, timeout=5)
            if msg_type == 14:
                reason = body[0]
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.TimeoutError):
            pass
        return reason == 0x02, f"first connection disconnect=0x{reason:02x}" if reason is not None else "first connection got no DISCONNECT"
    finally:
        first_w.close()
        second_w.close()


async def check_auth_reject(port: int):
    reader, writer = await _open(port)
    try:
        writer.write(connect_frame("rejected-device"))
        await writer.drain()
        msg_type, body = await read_packet(reader)
        return msg_type == 2 and body[0] == 0x03, f"type={msg_type} rc={body[0] if body else None}"
    finally:
        writer.close()


CHECKS = [
    ("connect+keepalive clamp", check_connect_and_keepalive, []),
    ("bad protocol version", check_bad_version, []),
    ("bad magic", check_bad_magic, []),
    ("PINGREQ->PINGRESP", check_ping, []),
    ("framing violation", check_protocol_violation, []),
    ("unknown packet type", check_unknown_type, []),
    ("session takeover", check_takeover, []),
    ("auth reject", check_auth_reject, ["--reject", "rejected-device"]),
    ("keepalive timeout", check_keepalive_timeout, ["--kp-min", "1", "--kp-max", "5"]),
]


def _server(language: str, port: int, extra: list[str]) -> ServerProcess:
    if language == "rust":
        kp_min, kp_max, reject = 30, 600, ""
        if "--kp-min" in extra:
            kp_min = int(extra[extra.index("--kp-min") + 1])
            kp_max = int(extra[extra.index("--kp-max") + 1])
        if "--reject" in extra:
            reject = extra[extra.index("--reject") + 1]
        proc = ServerProcess("rust", "tcp", port)
        proc._cmd = lambda: [RUST_SERVER, str(port), str(kp_min), str(kp_max), reject]  # type: ignore[method-assign]
        return proc
    if language == "python":
        mapped: list[str] = []
        if "--kp-min" in extra:
            mapped += ["--keepalive-min", extra[extra.index("--kp-min") + 1]]
            mapped += ["--keepalive-max", extra[extra.index("--kp-max") + 1]]
        if "--reject" in extra:
            mapped += ["--reject", extra[extra.index("--reject") + 1]]
        return ServerProcess("python", "tcp", port, mapped)
    if language == "java":
        return ServerProcess("java", "tcp", port, extra)
    return ServerProcess("go", "tcp", port, extra)


async def run_check(language: str, name: str, func, extra: list[str]) -> Check:
    port = _free_port()
    server = _server(language, port, extra)
    try:
        server.start()
        if not server.wait_ready():
            return Check(language, name, "FAIL", f"server not ready: {server.log[-300:]}")
        try:
            ok, observed = await func(port)
        except Exception as exc:  # noqa: BLE001
            ok, observed = False, f"{type(exc).__name__}: {exc}"
        return Check(language, name, "PASS" if ok else "FAIL", observed)
    finally:
        server.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(Path(__file__).resolve().parent / "results"))
    parser.add_argument("--servers", default="go,java,python,rust")
    args = parser.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    checks: list[Check] = []
    for language in args.servers.split(","):
        for name, func, extra in CHECKS:
            check = asyncio.run(run_check(language, name, func, extra))
            checks.append(check)
            print(f"{check.status:4} {check.server:6} {check.name:24} {check.observed}", flush=True)

    (out_dir / "protocol.json").write_text(json.dumps([c.__dict__ for c in checks], indent=2))
    failures = [c for c in checks if c.status != "PASS"]
    print(f"\n{len(checks) - len(failures)}/{len(checks)} protocol checks passed")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
