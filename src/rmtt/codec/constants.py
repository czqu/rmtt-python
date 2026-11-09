"""Wire-level constants for the RMTT protocol."""

#: Hard cap on a single frame body (16 MiB), matching the Rust/C++ implementations.
MAX_REMAINING_LENGTH = 16 * 1024 * 1024

# Control packet types (high nibble of the fixed header).
CONNECT = 1
CONNACK = 2
PUSH = 3
PINGREQ = 5
PINGRESP = 6
DISCONNECT = 14

# The CONNECT magic number spells "czqu".
MAGIC_NUMBER = 0x637A7175
PROTOCOL_VERSION = 1

# CONNACK return codes.
ACCEPTED = 0x00
ERR_REFUSED_BAD_PROTOCOL_VERSION = 0x01
ERR_REFUSED_SERVER_UNAVAILABLE = 0x02
ERR_REFUSED_NOT_AUTHORISED = 0x03
ERR_NETWORK_ERROR = 0xFE
ERR_PROTOCOL_VIOLATION = 0xFF

RETURN_CODE_TEXT = {
    ACCEPTED: "Connection Accepted",
    ERR_REFUSED_BAD_PROTOCOL_VERSION: "Connection Refused: Bad Protocol Version",
    ERR_REFUSED_SERVER_UNAVAILABLE: "Connection Refused: Server Unavailable",
    ERR_REFUSED_NOT_AUTHORISED: "Connection Refused: Not Authorised",
    ERR_NETWORK_ERROR: "Connection Error",
    ERR_PROTOCOL_VIOLATION: "Connection Refused: Protocol Violation",
}


class Disc:
    """DISCONNECT reason codes."""

    NORMAL = 0x00
    CREDENTIAL_EXPIRED = 0x01
    SESSION_TAKEN_OVER = 0x02
    SERVER_SHUTDOWN = 0x03
    PROTOCOL_VIOLATION = 0x04
    KEEPALIVE_TIMEOUT = 0x05
    KICKED_BY_ADMIN = 0x06
    RATE_LIMITED = 0x07
    CREDENTIAL_REJECTED = 0x08
    UNKNOWN = 0xFE


DISCONNECT_REASON_TEXT = {
    Disc.NORMAL: "normal disconnect",
    Disc.CREDENTIAL_EXPIRED: "credential expired",
    Disc.SESSION_TAKEN_OVER: "session taken over",
    Disc.SERVER_SHUTDOWN: "server shutdown",
    Disc.PROTOCOL_VIOLATION: "protocol violation",
    Disc.KEEPALIVE_TIMEOUT: "keepalive timeout",
    Disc.KICKED_BY_ADMIN: "kicked by admin",
    Disc.RATE_LIMITED: "rate limited",
    Disc.CREDENTIAL_REJECTED: "credential rejected",
    Disc.UNKNOWN: "unknown error",
}
