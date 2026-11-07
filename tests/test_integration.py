"""End-to-end tests for the Python client and server (all transports)."""

import asyncio
import os
import ssl
import time

import pytest

from rmtt import AsyncClient, AsyncServer, ClientOptions, KeepalivePolicy, ServerOptions
from rmtt.client import ConnectionState
from rmtt.codec import Disc
from rmtt.server import AuthResult
from rmtt.transport import listeners as listeners_mod

CERT = "/tmp/rmtt-test-cert.pem"
KEY = "/tmp/rmtt-test-key.pem"
HAVE_CERTS = os.path.exists(CERT) and os.path.exists(KEY)


class EchoListener:
    """Server lifecycle listener that records events for assertions."""

    def __init__(self):
        self.established = []
        self.closed = []

    def on_connection_established(self, device_id):
        self.established.append(device_id)

    def on_connection_closed(self, device_id, reason):
        self.closed.append((device_id, reason))


async def _wait_for(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


def _listener_for(transport, port=0):
    if transport == "tcp":
        return listeners_mod.StreamListener("127.0.0.1", port)
    if transport == "tls":
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(CERT, KEY)
        return listeners_mod.StreamListener("127.0.0.1", port, ssl_context=ctx)
    if transport == "kcp":
        return listeners_mod.KCPListener("127.0.0.1", port)
    if transport == "ws":
        return listeners_mod.WSListener("127.0.0.1", port)
    if transport == "quic":
        return listeners_mod.QUICListener(
            "127.0.0.1", port, certfile=CERT, keyfile=KEY, alpn=["rmtt"]
        )
    raise ValueError(transport)


async def _make_pair(transport="tcp", policy=None, handler=None, auth=None, heartbeat=30):
    events = EchoListener()
    options = ServerOptions(
        port=0,
        authenticator=auth,
        message_handler=handler,
        connection_listener=events,
        keepalive_policy=policy or KeepalivePolicy(),
    )
    options.listeners = [_listener_for(transport, 0)]
    server = AsyncServer(options)
    await server.start()

    client_options = ClientOptions(connect_retry=False, auto_reconnect=False, heartbeat=heartbeat)
    client_options.add_server(f"{transport}://127.0.0.1:{server.port}")
    client_options.set_credential("dev-1")
    if transport == "tls":
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        client_options.set_tls_context(ctx)
    if transport == "quic":
        # No explicit quic_alpn: the dialer must offer the standard "rmtt" ALPN
        # by default (QUICListener negotiates it), matching the WS subprotocol.
        client_options.quic_insecure = True
    client = AsyncClient(client_options)
    token = await client.connect()
    assert token.error is None, token.error
    return server, client, events


@pytest.mark.parametrize("transport", ["tcp", "tls", "ws", "kcp", "quic"])
async def test_bidirectional_push(transport):
    if transport in ("tls", "quic") and not HAVE_CERTS:
        pytest.skip("TLS certificate fixture missing")
    received = []

    async def handler(device_id, payload):
        received.append((device_id, payload))

    server, client, events = await _make_pair(transport, handler=handler)
    try:
        echo = []
        got = asyncio.Event()

        def on_message(message):
            echo.append(message.payload)
            got.set()

        client.add_payload_handler_last(on_message)
        token = await client.push("hello")
        assert token.error is None
        assert await _wait_for(lambda: received == [("dev-1", b"hello")])
        await server.push("dev-1", b"down")
        await asyncio.wait_for(got.wait(), 5)
        assert echo == [b"down"]
        assert await _wait_for(lambda: events.established == ["dev-1"])
    finally:
        await client.disconnect()
        await server.close()


async def test_server_keepalive_negotiation():
    policy = KeepalivePolicy(min_seconds=5, max_seconds=60, default_seconds=20)
    options = ServerOptions(port=0, keepalive_policy=policy)
    options.listeners = [listeners_mod.StreamListener("127.0.0.1", 0)]
    server = AsyncServer(options)
    await server.start()
    client = AsyncClient(ClientOptions(connect_retry=False, auto_reconnect=False, heartbeat=2))
    client.options.add_server(f"tcp://127.0.0.1:{server.port}")
    client.options.set_credential("dev-1")
    try:
        token = await client.connect()
        assert token.error is None
        assert client.server_keepalive == 5
    finally:
        await client.disconnect()
        await server.close()


async def test_auth_reject():
    options = ServerOptions(port=0, authenticator=lambda cred: AuthResult.reject())
    options.listeners = [listeners_mod.StreamListener("127.0.0.1", 0)]
    server = AsyncServer(options)
    await server.start()
    client = AsyncClient(ClientOptions(connect_retry=False, auto_reconnect=False))
    client.options.add_server(f"tcp://127.0.0.1:{server.port}")
    client.options.set_credential("nope")
    try:
        token = await client.connect()
        assert token.error is not None
        assert getattr(token.error, "return_code", None) == 0x03
        assert server.online_count == 0
    finally:
        await server.close()


async def test_kick_sends_reason():
    server, client, events = await _make_pair("tcp")
    try:
        await server.kick("dev-1", Disc.KICKED_BY_ADMIN)
        assert await _wait_for(lambda: not client.is_connected)
        assert client.disconnect_reason == Disc.KICKED_BY_ADMIN
        assert await _wait_for(lambda: any("kicked" in reason for _, reason in events.closed))
    finally:
        await client.disconnect()
        await server.close()


async def test_session_takeover():
    server, client, events = await _make_pair("tcp")
    second = AsyncClient(ClientOptions(connect_retry=False, auto_reconnect=False))
    second.options.add_server(f"tcp://127.0.0.1:{server.port}")
    second.options.set_credential("dev-1")
    try:
        token = await second.connect()
        assert token.error is None
        assert await _wait_for(lambda: not client.is_connected)
        assert await _wait_for(lambda: second.is_connected)
        assert server.online_count == 1
    finally:
        await second.disconnect()
        await client.disconnect()
        await server.close()


async def test_keepalive_timeout_reaps_silent_client():
    policy = KeepalivePolicy(min_seconds=1, max_seconds=1, default_seconds=1)
    received = []
    options = ServerOptions(
        port=0, keepalive_policy=policy, message_handler=lambda d, p: received.append(p)
    )
    options.listeners = [listeners_mod.StreamListener("127.0.0.1", 0)]
    server = AsyncServer(options)
    await server.start()

    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    try:
        from rmtt.codec import ConnectPacket, encode_packet, read_packet

        writer.write(encode_packet(ConnectPacket(keepalive=1, credential="silent")))
        await writer.drain()
        connack = await read_packet(reader)
        assert connack.return_code == 0
        assert connack.server_keepalive == 1
        disconnect = await asyncio.wait_for(read_packet(reader), 5)
        assert disconnect.return_code == Disc.KEEPALIVE_TIMEOUT
    finally:
        writer.close()
        await server.close()


async def test_pingreq_pingresp_with_short_heartbeat():
    policy = KeepalivePolicy(min_seconds=1, max_seconds=5, default_seconds=1)
    server, client, _ = await _make_pair("tcp", policy=policy, heartbeat=1)
    try:
        assert client.server_keepalive == 1
        # The client pings every second; staying connected for >2s proves the
        # PINGREQ/PINGRESP round trip keeps the server watchdog happy.
        await asyncio.sleep(2.5)
        assert client.is_connected
        assert server.is_online("dev-1")
    finally:
        await client.disconnect()
        await server.close()


async def test_large_and_binary_payloads():
    received = []

    async def handler(device_id, payload):
        received.append(payload)

    server, client, _ = await _make_pair("tcp", handler=handler)
    try:
        blob = bytes(range(256)) * 1024  # 256 KiB, exercises multi-byte lengths
        token = await client.push(blob)
        assert token.error is None
        assert await _wait_for(lambda: received == [blob], timeout=10)
        assert await _wait_for(lambda: True)
    finally:
        await client.disconnect()
        await server.close()


async def test_offline_push_raises():
    server, client, _ = await _make_pair("tcp")
    from rmtt.server import DeviceOfflineError

    try:
        with pytest.raises(DeviceOfflineError):
            await server.push("ghost", b"x")
    finally:
        await client.disconnect()
        await server.close()


async def test_protocol_violation_gets_disconnect():
    from rmtt.codec import ConnectPacket, encode_packet, read_packet

    options = ServerOptions(port=0)
    options.listeners = [listeners_mod.StreamListener("127.0.0.1", 0)]
    server = AsyncServer(options)
    await server.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    try:
        writer.write(encode_packet(ConnectPacket(keepalive=30, credential="x")))
        await writer.drain()
        await read_packet(reader)
        writer.write(bytes([0x31, 0x00]))  # PUSH with non-zero flags
        await writer.drain()
        packet = await asyncio.wait_for(read_packet(reader), 5)
        assert packet.return_code == Disc.PROTOCOL_VIOLATION
    finally:
        writer.close()
        await server.close()


async def test_bad_magic_closed_silently():
    from rmtt.codec import ConnectPacket, encode_packet, read_packet

    options = ServerOptions(port=0)
    options.listeners = [listeners_mod.StreamListener("127.0.0.1", 0)]
    server = AsyncServer(options)
    await server.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    try:
        packet = encode_packet(ConnectPacket(keepalive=30, credential="x"))
        packet = packet[:2] + b"\x00\x00\x00\x00" + packet[6:]
        writer.write(packet)
        await writer.drain()
        with pytest.raises((asyncio.IncompleteReadError, ConnectionError)):
            await asyncio.wait_for(read_packet(reader), 5)
    finally:
        writer.close()
        await server.close()


async def test_graceful_disconnect_clears_store():
    server, client, events = await _make_pair("tcp")
    try:
        assert server.is_online("dev-1")
        await client.disconnect()
        assert await _wait_for(lambda: not server.is_online("dev-1"))
        assert await _wait_for(lambda: events.closed)
    finally:
        await server.close()


async def test_sync_client_and_server_facade():
    """The blocking facades must interoperate with the async core."""
    from rmtt import Client as SyncClient
    from rmtt.server import Server as SyncServer

    holder = {}
    options = ServerOptions(port=0)
    options.listeners = [_listener_for("tcp", 0)]
    options.set_authenticator(lambda credential: AuthResult.allow(credential))
    options.set_message_handler(lambda device_id, payload: holder["server"].push(device_id, payload))
    server = SyncServer(options)
    holder["server"] = server
    server.start()
    try:
        client = SyncClient(ClientOptions(connect_retry=False, auto_reconnect=False, heartbeat=30))
        client.options.add_server(f"tcp://127.0.0.1:{server.port}")
        client.options.set_credential("sync-1")
        received = []
        client.add_payload_handler_last(lambda msg: received.append(msg.payload))
        token = client.connect()
        assert token.wait(5) and token.error is None
        assert await _wait_for(lambda: server.is_online("sync-1"))
        assert client.push("hi").wait(5)
        assert await _wait_for(lambda: received == [b"hi"])
        client.disconnect()
        client.shutdown()
    finally:
        server.close()


async def test_client_reconnects_after_server_restart():
    port = 0
    options = ServerOptions(port=0)
    options.listeners = [_listener_for("tcp", 0)]
    server = AsyncServer(options)
    await server.start()
    port = server.port

    client_options = ClientOptions(
        connect_retry=False, auto_reconnect=True, reconnect_base=0.2, heartbeat=30
    )
    client_options.add_server(f"tcp://127.0.0.1:{port}")
    client_options.set_credential("dev-reconnect")
    client = AsyncClient(client_options)
    token = await client.connect()
    assert token.error is None

    await server.close()
    # is_connected intentionally stays true while reconnecting (Go semantics);
    # wait for the state machine to enter the reconnect phase instead.
    assert await _wait_for(
        lambda: client.state is ConnectionState.RECONNECTING, timeout=5
    )

    restarted = ServerOptions(port=port)
    restarted.listeners = [_listener_for("tcp", port)]
    server2 = AsyncServer(restarted)
    await server2.start()

    assert await _wait_for(lambda: client.state is ConnectionState.CONNECTED, timeout=10)
    assert await _wait_for(lambda: server2.is_online("dev-reconnect"), timeout=5)
    await client.disconnect()
    await server2.close()


async def test_adaptive_heartbeat_stays_connected():
    policy = KeepalivePolicy(min_seconds=1, max_seconds=5, default_seconds=1)
    events = EchoListener()
    options = ServerOptions(port=0, keepalive_policy=policy, connection_listener=events)
    options.listeners = [_listener_for("tcp", 0)]
    server = AsyncServer(options)
    await server.start()

    client_options = ClientOptions(connect_retry=False, auto_reconnect=False)
    client_options.add_server(f"tcp://127.0.0.1:{server.port}")
    client_options.set_credential("dev-adaptive")
    client_options.set_adaptive_heartbeat(1, 2)
    client = AsyncClient(client_options)
    try:
        token = await client.connect()
        assert token.error is None
        assert client.server_keepalive == 2
        await asyncio.sleep(3.0)
        assert client.is_connected
        assert server.is_online("dev-adaptive")
    finally:
        await client.disconnect()
        await server.close()


async def test_push_before_connect_reports_error():
    from rmtt.client import NotConnectedError

    client = AsyncClient(ClientOptions(auto_reconnect=False))
    token = await client.push("nope")
    assert isinstance(token.error, NotConnectedError)


# --------------------------------------------------------------------------
# Regression tests for lifecycle / protocol-semantics bugs.
# --------------------------------------------------------------------------


async def test_cancellation_propagates_from_connect():
    client = AsyncClient(ClientOptions(connect_retry=True, connect_retry_interval=60))
    client.options.add_server("tcp://127.0.0.1:1")  # refused, then long retry sleep
    task = asyncio.create_task(client.connect())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.state is ConnectionState.DISCONNECTED
    await client.shutdown()


class _AsyncCallable:
    """Handler object with ``async __call__`` (not a coroutine function)."""

    def __init__(self):
        self.called = False

    async def __call__(self, *args):
        self.called = True


async def test_async_callable_object_handlers_run():
    server_handler = _AsyncCallable()
    server, client, _ = await _make_pair("tcp", handler=server_handler)
    client_handler = _AsyncCallable()
    client.add_payload_handler_last(client_handler)
    try:
        await client.push("hi")
        await server.push("dev-1", b"down")
        assert await _wait_for(lambda: server_handler.called)
        assert await _wait_for(lambda: client_handler.called)
    finally:
        await client.disconnect()
        await server.close()


async def test_kick_notifies_closed_exactly_once():
    server, client, events = await _make_pair("tcp")
    try:
        await server.kick("dev-1", Disc.KICKED_BY_ADMIN)
        assert await _wait_for(lambda: len(events.closed) >= 1)
        await asyncio.sleep(0.3)
        assert len(events.closed) == 1
        assert "kicked" in events.closed[0][1]
    finally:
        await client.disconnect()
        await server.close()


async def test_disconnect_during_connect_aborts_session():
    options = ServerOptions(port=0)
    options.listeners = [listeners_mod.StreamListener("127.0.0.1", 0)]
    server = AsyncServer(options)
    await server.start()

    async def slow_auth(cred):
        await asyncio.sleep(0.5)
        return AuthResult.allow(cred)

    server.options.authenticator = slow_auth
    client = AsyncClient(ClientOptions(connect_retry=False, auto_reconnect=False))
    client.options.add_server(f"tcp://127.0.0.1:{server.port}")
    client.options.set_credential("z1")
    task = asyncio.create_task(client.connect())
    await asyncio.sleep(0.1)
    await client.disconnect()  # aborts the in-flight handshake
    token = await task
    assert token.error is not None
    await asyncio.sleep(0.3)
    assert not client.is_connected
    assert not server.is_online("z1")
    await server.close()


async def test_server_keepalive_zero_disables_client_heartbeats():
    from rmtt.codec import ConnackPacket, ConnectPacket, encode_packet, read_packet

    seen = []

    async def handle(reader, writer):
        try:
            assert isinstance(await read_packet(reader), ConnectPacket)
            writer.write(encode_packet(ConnackPacket(return_code=0, server_keepalive=0)))
            await writer.drain()
            while True:
                seen.append(type(await read_packet(reader)).__name__)
        except Exception:
            pass
        finally:
            writer.close()

    raw = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = raw.sockets[0].getsockname()[1]
    client = AsyncClient(
        ClientOptions(connect_retry=False, auto_reconnect=False, heartbeat=1)
    )
    client.options.add_server(f"tcp://127.0.0.1:{port}")
    client.options.set_credential("k1")
    token = await client.connect()
    assert token.error is None
    assert client.server_keepalive == 0
    try:
        await asyncio.sleep(3.0)
        assert seen == []  # negotiated 0 == disabled: no PINGREQ at all
        assert client.is_connected  # and no client-side receive timeout
    finally:
        await client.shutdown()
        raw.close()
        await raw.wait_closed()


async def test_ws_listener_rejects_wrong_path():
    import websockets.asyncio.client as wsclient

    options = ServerOptions(port=0)
    options.listeners = [listeners_mod.WSListener("127.0.0.1", 0, path="/rmtt")]
    server = AsyncServer(options)
    await server.start()
    try:
        with pytest.raises(Exception):
            await wsclient.connect(
                f"ws://127.0.0.1:{server.port}/wrong-path",
                subprotocols=["rmtt"],
                open_timeout=5,
            )
    finally:
        await server.close()


async def test_oversized_push_fails_locally_and_link_survives():
    from rmtt.codec.codec import MAX_REMAINING_LENGTH

    server, client, _ = await _make_pair("tcp")
    try:
        token = await client.push(b"x" * (MAX_REMAINING_LENGTH + 1))
        assert isinstance(token.error, ValueError)
        assert client.is_connected  # the rejected push must not kill the link
    finally:
        await client.disconnect()
        await server.close()


async def test_reconnect_after_keepalive_timeout_backs_off():
    from rmtt.codec import (
        ConnackPacket,
        ConnectPacket,
        Disc,
        DisconnectPacket,
        encode_packet,
        read_packet,
    )

    async def handle(reader, writer):
        try:
            assert isinstance(await read_packet(reader), ConnectPacket)
            writer.write(encode_packet(ConnackPacket(return_code=0, server_keepalive=30)))
            await writer.drain()
            writer.write(encode_packet(DisconnectPacket(return_code=Disc.KEEPALIVE_TIMEOUT)))
            await writer.drain()
        finally:
            writer.close()

    raw = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = raw.sockets[0].getsockname()[1]
    attempts = []
    client_options = ClientOptions(
        connect_retry=False, auto_reconnect=True, reconnect_base=1.0, heartbeat=30
    )
    client_options.add_server(f"tcp://127.0.0.1:{port}")
    client_options.set_credential("dev-retry")
    client_options.on_connect_attempt = lambda server: attempts.append(server)
    client = AsyncClient(client_options)
    token = await client.connect()
    assert token.error is None
    raw.close()
    await raw.wait_closed()
    try:
        await asyncio.sleep(3.0)
        # With exponential backoff at most a handful of attempts fit in 3 s;
        # a reason-driven backoff reset on every retry spins thousands.
        assert len(attempts) <= 6, f"reconnect hot loop: {len(attempts)} attempts"
    finally:
        await client.shutdown()


def test_sync_push_after_shutdown_reports_error():
    from rmtt import Client as SyncClient

    client = SyncClient(ClientOptions(connect_retry=False, auto_reconnect=False))
    client.shutdown()
    token = client.push("x")
    assert token.wait(5)
    assert token.error is not None
