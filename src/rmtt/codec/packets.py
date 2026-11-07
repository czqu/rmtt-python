"""RMTT control packets.

Every packet knows how to encode itself into bytes. Decoding lives in
:mod:`rmtt.codec.codec`, which first parses the fixed header and then hands the
body to the corresponding packet class.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .constants import (
    CONNACK,
    CONNECT,
    DISCONNECT,
    MAGIC_NUMBER,
    MAX_REMAINING_LENGTH,
    PINGREQ,
    PINGRESP,
    PROTOCOL_VERSION,
    PUSH,
)


@dataclass
class FixedHeader:
    """Frame header: message type, flags and remaining (body) length."""

    message_type: int
    flags: int = 0
    remaining_length: int = 0


class ControlPacket:
    """Base class for all RMTT packets."""

    message_type: int = 0

    def encode(self) -> bytes:  # pragma: no cover - overridden
        raise NotImplementedError


def _encode_length(length: int) -> bytes:
    out = bytearray()
    while True:
        digit = length % 128
        length //= 128
        if length > 0:
            digit |= 0x80
        out.append(digit)
        if length == 0:
            break
    return bytes(out)


def _frame(message_type: int, body: bytes) -> bytes:
    if len(body) > MAX_REMAINING_LENGTH:
        raise ValueError(
            f"frame body too large: {len(body)} bytes (limit {MAX_REMAINING_LENGTH})"
        )
    return bytes([(message_type << 4) & 0xFF]) + _encode_length(len(body)) + body


def _encode_string(value: str) -> bytes:
    raw = value.encode("utf-8")
    if len(raw) > 0xFFFF:
        raise ValueError("credential exceeds the 65535-byte wire limit")
    return len(raw).to_bytes(2, "big") + raw


@dataclass
class ConnectPacket(ControlPacket):
    """First packet sent by a client: magic, version, flags, keepalive, credential."""

    message_type: int = field(default=CONNECT, init=False)
    magic_number: int = MAGIC_NUMBER
    protocol_version: int = PROTOCOL_VERSION
    flags: int = 0
    keepalive: int = 0
    credential: str = ""

    def encode(self) -> bytes:
        body = (
            self.magic_number.to_bytes(4, "big")
            + bytes([self.protocol_version, self.flags])
            + self.keepalive.to_bytes(2, "big")
            + _encode_string(self.credential)
        )
        return _frame(CONNECT, body)

    @classmethod
    def decode(cls, body: bytes) -> "ConnectPacket":
        if len(body) < 10:
            raise ValueError("CONNECT body too short")
        magic = int.from_bytes(body[0:4], "big")
        version = body[4]
        flags = body[5]
        keepalive = int.from_bytes(body[6:8], "big")
        cred_len = int.from_bytes(body[8:10], "big")
        if len(body) < 10 + cred_len:
            raise ValueError("CONNECT credential truncated")
        credential = body[10 : 10 + cred_len].decode("utf-8")
        return cls(
            magic_number=magic,
            protocol_version=version,
            flags=flags,
            keepalive=keepalive,
            credential=credential,
        )


@dataclass
class ConnackPacket(ControlPacket):
    """Server reply to CONNECT: return code plus the negotiated keepalive."""

    message_type: int = field(default=CONNACK, init=False)
    return_code: int = 0
    server_keepalive: int = 0

    def encode(self) -> bytes:
        body = bytes([self.return_code]) + self.server_keepalive.to_bytes(2, "big")
        return _frame(CONNACK, body)

    @classmethod
    def decode(cls, body: bytes) -> "ConnackPacket":
        if len(body) < 3:
            raise ValueError("CONNACK body too short")
        return cls(return_code=body[0], server_keepalive=int.from_bytes(body[1:3], "big"))


@dataclass
class PushPacket(ControlPacket):
    """Application payload in either direction."""

    message_type: int = field(default=PUSH, init=False)
    reserved: int = 0
    payload: bytes = b""

    def encode(self) -> bytes:
        return _frame(PUSH, bytes([self.reserved]) + self.payload)

    @classmethod
    def decode(cls, body: bytes) -> "PushPacket":
        if not body:
            raise ValueError("PUSH body missing reserved byte")
        return cls(reserved=body[0], payload=body[1:])


@dataclass
class PingreqPacket(ControlPacket):
    """Heartbeat request."""

    message_type: int = field(default=PINGREQ, init=False)

    def encode(self) -> bytes:
        return _frame(PINGREQ, b"")

    @classmethod
    def decode(cls, body: bytes) -> "PingreqPacket":
        return cls()


@dataclass
class PingrespPacket(ControlPacket):
    """Heartbeat response."""

    message_type: int = field(default=PINGRESP, init=False)

    def encode(self) -> bytes:
        return _frame(PINGRESP, b"")

    @classmethod
    def decode(cls, body: bytes) -> "PingrespPacket":
        return cls()


@dataclass
class DisconnectPacket(ControlPacket):
    """Graceful close carrying a reason code."""

    message_type: int = field(default=DISCONNECT, init=False)
    return_code: int = 0

    def encode(self) -> bytes:
        return _frame(DISCONNECT, bytes([self.return_code & 0xFF]))

    @classmethod
    def decode(cls, body: bytes) -> "DisconnectPacket":
        if not body:
            raise ValueError("DISCONNECT body too short")
        return cls(return_code=body[0])
