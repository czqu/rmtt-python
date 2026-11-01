"""Packing and parsing of RMTT frames."""

from __future__ import annotations

from typing import Optional, Protocol, Tuple

from .constants import CONNACK, CONNECT, DISCONNECT, PINGREQ, PINGRESP, PUSH
from .errors import ProtocolViolationError, UnknownPacketTypeError
from .packets import (
    ConnackPacket,
    ConnectPacket,
    ControlPacket,
    DisconnectPacket,
    PingreqPacket,
    PingrespPacket,
    PushPacket,
)

#: Hard cap on a single frame body (16 MiB), matching the Rust/C++ implementations.
MAX_REMAINING_LENGTH = 16 * 1024 * 1024

_PACKET_TYPES = {
    CONNACK: ConnackPacket,
    PUSH: PushPacket,
    PINGREQ: PingreqPacket,
    PINGRESP: PingrespPacket,
    DISCONNECT: DisconnectPacket,
}


def encode_packet(packet: ControlPacket) -> bytes:
    """Serialize a control packet, including its fixed header."""
    return packet.encode()


class PacketReader(Protocol):
    """The minimal async byte source the codec needs (a stream or a transport)."""

    async def readexactly(self, n: int) -> bytes: ...


async def read_packet(reader: PacketReader) -> ControlPacket:
    """Read one complete frame from an async byte source."""
    first = await reader.readexactly(1)
    remaining, header_size = await _read_remaining_length(reader)
    if remaining > MAX_REMAINING_LENGTH:
        raise ProtocolViolationError(f"frame body too large: {remaining} bytes")
    body = await reader.readexactly(remaining) if remaining else b""
    return _build(first[0], body)


async def _read_remaining_length(reader: PacketReader) -> Tuple[int, int]:
    value = 0
    multiplier = 1
    size = 0
    while True:
        digit = (await reader.readexactly(1))[0]
        size += 1
        value += (digit & 0x7F) * multiplier
        if digit & 0x80 == 0:
            return value, size
        multiplier *= 128
        if size >= 4:
            raise ProtocolViolationError("malformed remaining length")


def _build(first_byte: int, body: bytes) -> ControlPacket:
    message_type = first_byte >> 4
    flags = first_byte & 0x0F
    if flags != 0:
        raise ProtocolViolationError(f"non-zero fixed header flags: 0x{flags:02x}")
    if message_type == CONNECT:
        try:
            return ConnectPacket.decode(body)
        except (ValueError, UnicodeDecodeError) as exc:
            raise ProtocolViolationError(str(exc)) from exc
    packet_cls = _PACKET_TYPES.get(message_type)
    if packet_cls is None:
        raise UnknownPacketTypeError(message_type)
    try:
        return packet_cls.decode(body)
    except ValueError as exc:
        raise ProtocolViolationError(str(exc)) from exc


class Decoder:
    """Incremental decoder for framed byte streams (used by WebSocket transports).

    ``feed()`` appends bytes and ``next_packet()`` pops one complete packet at a
    time, returning ``None`` while the frame is still incomplete.
    """

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> None:
        self._buf.extend(data)

    @property
    def pending(self) -> int:
        return len(self._buf)

    def next_packet(self) -> Optional[ControlPacket]:
        parsed = _parse_header(self._buf)
        if parsed is None:
            return None
        message_type, body_offset, remaining = parsed
        end = body_offset + remaining
        if len(self._buf) < end:
            return None
        if remaining > MAX_REMAINING_LENGTH:
            raise ProtocolViolationError(f"frame body too large: {remaining} bytes")
        body = bytes(self._buf[body_offset:end])
        del self._buf[:end]
        first = message_type << 4
        return _build(first, body)


def _parse_header(buf: bytearray) -> Optional[Tuple[int, int, int]]:
    if not buf:
        return None
    message_type = buf[0] >> 4
    if buf[0] & 0x0F != 0:
        raise ProtocolViolationError(f"non-zero fixed header flags: 0x{buf[0] & 0x0F:02x}")
    value = 0
    multiplier = 1
    index = 1
    for _ in range(4):
        if index >= len(buf):
            return None
        digit = buf[index]
        index += 1
        value += (digit & 0x7F) * multiplier
        if digit & 0x80 == 0:
            return message_type, index, value
        multiplier *= 128
    raise ProtocolViolationError("malformed remaining length")
