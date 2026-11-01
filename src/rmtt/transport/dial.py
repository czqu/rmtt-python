"""Client-side transport dialling for every supported URL scheme."""

from __future__ import annotations

import asyncio
import ssl
from typing import Optional
from urllib.parse import urlparse

from ..codec.errors import CodecError

from .base import Transport
from .quic import RmttQuicProtocol
from .stream import StreamTransport
from .ws import DEFAULT_WS_PATH, WS_SUBPROTOCOL, WebSocketTransport

SUPPORTED_SCHEMES = ("tcp", "tls", "kcp", "ws", "wss", "quic")


def normalize_server(server: str) -> str:
    """Apply the same defaults as the Go client: bare addresses are TCP and a
    leading colon means localhost."""
    if server.startswith(":"):
        server = "127.0.0.1" + server
    if "://" not in server:
        server = "tcp://" + server
    return server


async def dial(
    server: str,
    *,
    ssl_context: Optional[ssl.SSLContext] = None,
    ws_path: str = DEFAULT_WS_PATH,
    quic_alpn: Optional[list[str]] = None,
    quic_ca_file: Optional[str] = None,
    quic_insecure: bool = True,
    connect_timeout: float = 30.0,
) -> Transport:
    """Open a transport to ``server`` (``scheme://host:port[/path]``)."""
    uri = urlparse(normalize_server(server))
    scheme = uri.scheme
    if scheme not in SUPPORTED_SCHEMES:
        raise CodecError(f"unknown protocol: {scheme}")
    host = uri.hostname or "127.0.0.1"
    port = uri.port
    if port is None:
        raise CodecError(f"missing port in server address: {server}")

    if scheme == "tcp":
        return await _dial_stream(host, port, None, connect_timeout)
    if scheme == "tls":
        return await _dial_stream(
            host, port, ssl_context or ssl.create_default_context(), connect_timeout
        )
    if scheme == "kcp":
        return await _dial_kcp(host, port, connect_timeout)
    if scheme in ("ws", "wss"):
        return await _dial_ws(
            host, port, uri.path or ws_path, scheme, ssl_context, connect_timeout
        )
    return await _dial_quic(
        host, port, quic_alpn, quic_ca_file, quic_insecure, connect_timeout
    )


async def _dial_stream(
    host: str, port: int, ssl_ctx: Optional[ssl.SSLContext], timeout: float
) -> StreamTransport:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(
            host, port, ssl=ssl_ctx, server_hostname=host if ssl_ctx else None
        ),
        timeout,
    )
    return StreamTransport(reader, writer)


async def _dial_kcp(host: str, port: int, timeout: float) -> StreamTransport:
    import aiokcp

    reader, writer = await asyncio.wait_for(aiokcp.open_connection(host, port), timeout)
    return StreamTransport(reader, writer)


async def _dial_ws(
    host: str,
    port: int,
    path: str,
    scheme: str,
    ssl_ctx: Optional[ssl.SSLContext],
    timeout: float,
) -> WebSocketTransport:
    from websockets.asyncio.client import connect

    uri = f"{scheme}://{host}:{port}{path}"
    ws = await asyncio.wait_for(
        connect(
            uri,
            subprotocols=[WS_SUBPROTOCOL],
            ssl=ssl_ctx if scheme == "wss" else None,
            max_size=None,
            open_timeout=timeout,
        ),
        timeout,
    )
    transport = WebSocketTransport(ws)
    transport.start()
    return transport


async def _dial_quic(
    host: str,
    port: int,
    alpn: Optional[list[str]],
    ca_file: Optional[str],
    insecure: bool,
    timeout: float,
) -> Transport:
    from aioquic.asyncio.client import connect
    from aioquic.quic.configuration import QuicConfiguration

    configuration = QuicConfiguration(
        is_client=True, alpn_protocols=alpn or ["rmtt"]
    )
    if insecure:
        configuration.verify_mode = ssl.CERT_NONE
    elif ca_file:
        configuration.load_verify_locations(ca_file)
    else:
        configuration.load_default_certs()

    manager = connect(
        host,
        port,
        configuration=configuration,
        create_protocol=RmttQuicProtocol,
        wait_connected=True,
    )
    protocol = await asyncio.wait_for(manager.__aenter__(), timeout)
    transport = protocol.open_transport()

    async def _close_connection() -> None:
        try:
            await manager.__aexit__(None, None, None)
        except Exception:
            pass

    transport.set_close_hook(_close_connection)
    return transport
