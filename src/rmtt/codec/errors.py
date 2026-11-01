"""Codec error types."""

from .constants import RETURN_CODE_TEXT


class CodecError(Exception):
    """Raised when a frame cannot be encoded or decoded."""


class UnknownPacketTypeError(CodecError):
    """The fixed header carries a packet type this implementation does not know."""

    def __init__(self, packet_type: int) -> None:
        super().__init__(f"unsupported packet type 0x{packet_type:02x}")
        self.packet_type = packet_type


class ProtocolViolationError(CodecError):
    """The peer sent bytes that violate the RMTT framing rules."""


class ConnRefusedError(ConnectionError):
    """The server answered CONNECT with a non-zero CONNACK return code."""

    def __init__(self, return_code: int) -> None:
        self.return_code = return_code
        super().__init__(
            RETURN_CODE_TEXT.get(return_code, f"Connection Refused (0x{return_code:02x})")
        )
