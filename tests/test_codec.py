"""Byte-level codec tests using vectors from the Go/Rust reference suites."""

import pytest

from rmtt.codec import (
    ConnackPacket,
    ConnectPacket,
    Decoder,
    DisconnectPacket,
    PingreqPacket,
    PingrespPacket,
    ProtocolViolationError,
    PushPacket,
    UnknownPacketTypeError,
    encode_packet,
)
from rmtt.codec.codec import _build


def test_connect_vector_matches_reference():
    packet = ConnectPacket(protocol_version=1, keepalive=30, credential="credential")
    assert encode_packet(packet) == bytes(
        [0x10, 0x14, 0x63, 0x7A, 0x71, 0x75, 0x01, 0x00, 0x00, 0x1E, 0x00, 0x0A]
    ) + b"credential"


def test_connack_vector():
    assert encode_packet(ConnackPacket(return_code=0, server_keepalive=30)) == bytes(
        [0x20, 0x03, 0x00, 0x00, 0x1E]
    )


def test_push_vector():
    assert encode_packet(PushPacket(payload=b"abc")) == bytes([0x30, 0x04, 0x00]) + b"abc"


def test_ping_vectors():
    assert encode_packet(PingreqPacket()) == bytes([0x50, 0x00])
    assert encode_packet(PingrespPacket()) == bytes([0x60, 0x00])


def test_disconnect_vector():
    assert encode_packet(DisconnectPacket(return_code=0x06)) == bytes([0xE0, 0x01, 0x06])


def test_multi_byte_remaining_length():
    packet = PushPacket(payload=b"x" * 200)
    encoded = encode_packet(packet)
    assert encoded[0] == 0x30
    assert encoded[1:3] == bytes([0xC9, 0x01])
    decoder = Decoder()
    decoder.feed(encoded)
    decoded = decoder.next_packet()
    assert isinstance(decoded, PushPacket)
    assert decoded.payload == b"x" * 200


def test_decoder_handles_partial_frames():
    encoded = encode_packet(ConnackPacket(return_code=0, server_keepalive=42))
    decoder = Decoder()
    decoder.feed(encoded[:2])
    assert decoder.next_packet() is None
    decoder.feed(encoded[2:])
    packet = decoder.next_packet()
    assert packet.server_keepalive == 42


def test_decoder_multiple_packets_in_one_buffer():
    data = encode_packet(PingreqPacket()) + encode_packet(DisconnectPacket(return_code=0))
    decoder = Decoder()
    decoder.feed(data)
    assert isinstance(decoder.next_packet(), PingreqPacket)
    assert isinstance(decoder.next_packet(), DisconnectPacket)
    assert decoder.next_packet() is None


def test_non_zero_flags_rejected():
    with pytest.raises(ProtocolViolationError):
        _build(0x31, b"")


def test_unknown_packet_type_rejected():
    with pytest.raises(UnknownPacketTypeError):
        _build(0x00, b"")


def test_malformed_remaining_length_rejected():
    decoder = Decoder()
    decoder.feed(bytes([0x30, 0x80, 0x80, 0x80, 0x80, 0x00]))
    with pytest.raises(ProtocolViolationError):
        decoder.next_packet()


def test_truncated_connect_rejected():
    with pytest.raises(ProtocolViolationError):
        _build(0x10, b"\x00\x00")


def test_connect_credential_roundtrip_utf8():
    packet = ConnectPacket(keepalive=15, credential="设备-001")
    decoder = Decoder()
    decoder.feed(encode_packet(packet))
    decoded = decoder.next_packet()
    assert decoded.credential == "设备-001"
    assert decoded.keepalive == 15


def test_oversized_frame_rejected_on_encode():
    from rmtt.codec.codec import MAX_REMAINING_LENGTH

    with pytest.raises(ValueError):
        encode_packet(PushPacket(payload=b"x" * (MAX_REMAINING_LENGTH + 1)))


def test_decoder_rejects_oversized_before_body_arrives():
    from rmtt.codec.codec import MAX_REMAINING_LENGTH
    from rmtt.codec.packets import _encode_length

    decoder = Decoder()
    decoder.feed(bytes([0x30]) + _encode_length(MAX_REMAINING_LENGTH + 1))
    with pytest.raises(ProtocolViolationError):
        decoder.next_packet()
    # The declared body must never be buffered.
    assert decoder.pending <= 5
