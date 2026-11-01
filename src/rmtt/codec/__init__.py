"""RMTT wire protocol codec.

Pure, I/O-free encoding and decoding of RMTT control packets. The layout
matches the Go / Java / Rust / C++ implementations byte for byte:

    fixed header: (type << 4 | flags) | variable-length remaining length
    CONNECT:      u32 magic | u8 version | u8 flags | u16 keepalive | string credential
    CONNACK:      u8 return code | u16 server keepalive
    PUSH:         u8 reserved | payload bytes
    PINGREQ:      -
    PINGRESP:     -
    DISCONNECT:   u8 reason code
"""

from .codec import Decoder, encode_packet, read_packet
from .constants import (
    ACCEPTED,
    CONNACK,
    CONNECT,
    DISCONNECT,
    DISCONNECT_REASON_TEXT,
    ERR_NETWORK_ERROR,
    ERR_PROTOCOL_VIOLATION,
    ERR_REFUSED_BAD_PROTOCOL_VERSION,
    ERR_REFUSED_NOT_AUTHORISED,
    ERR_REFUSED_SERVER_UNAVAILABLE,
    MAGIC_NUMBER,
    PINGREQ,
    PINGRESP,
    PROTOCOL_VERSION,
    PUSH,
    RETURN_CODE_TEXT,
    Disc,
)
from .errors import (
    CodecError,
    ConnRefusedError,
    ProtocolViolationError,
    UnknownPacketTypeError,
)
from .packets import (
    ConnackPacket,
    ConnectPacket,
    ControlPacket,
    DisconnectPacket,
    FixedHeader,
    PingreqPacket,
    PingrespPacket,
    PushPacket,
)

__all__ = [
    "ACCEPTED",
    "CONNACK",
    "CONNECT",
    "DISCONNECT",
    "DISCONNECT_REASON_TEXT",
    "ERR_NETWORK_ERROR",
    "ERR_PROTOCOL_VIOLATION",
    "ERR_REFUSED_BAD_PROTOCOL_VERSION",
    "ERR_REFUSED_NOT_AUTHORISED",
    "ERR_REFUSED_SERVER_UNAVAILABLE",
    "MAGIC_NUMBER",
    "PINGREQ",
    "PINGRESP",
    "PROTOCOL_VERSION",
    "PUSH",
    "RETURN_CODE_TEXT",
    "CodecError",
    "ConnRefusedError",
    "ConnackPacket",
    "ConnectPacket",
    "ControlPacket",
    "Decoder",
    "DisconnectPacket",
    "Disc",
    "FixedHeader",
    "PingreqPacket",
    "PingrespPacket",
    "ProtocolViolationError",
    "PushPacket",
    "UnknownPacketTypeError",
    "encode_packet",
    "read_packet",
]
