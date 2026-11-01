"""Server-side listeners for every supported transport."""

from __future__ import annotations

import asyncio
import ssl
from typing import Any, Awaitable, Callable, Optional

from .base import Transport
from .quic import QuicStreamTransport, RmttQuicProtocol
from .stream import StreamTransport
from .ws import DEFAULT_WS_PATH, WS_SUBPROTOCOL, WebSocketTransport

ConnectionHandler = Callable[[Transport], Awaitable[None]]


class Listener:
    """Accepts connections and hands each one to ``on_connection``."""

    scheme = "tcp"

    def __init__(self, host: str = "0.0.0.0", port: int = 0) -> None:
        self.host = host
        self._requested_port = port
        self.port = port

    async def start(self, on_connection: ConnectionHandler) -> None:  # pragma: no cover
        raise NotImplementedError

    async def close(self) -> None:  # pragma: no cover
        raise NotImplementedError


class StreamListener(Listener):
    """TCP listener; pass an ``ssl_context`` to get TLS."""

    def __init__(
        self, host: str = "0.0.0.0", port: int = 0, ssl_context: Optional[ssl.SSLContext] = None
    ) -> None:
        super().__init__(host, port)
        self.ssl_context = ssl_context
        self.scheme = "tls" if ssl_context is not None else "tcp"
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self, on_connection: ConnectionHandler) -> None:
        async def _cb(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            transport = StreamTransport(reader, writer)
            try:
                await on_connection(transport)
            finally:
                await transport.close()

        self._server = await asyncio.start_server(
            _cb, self.host, self._requested_port, ssl=self.ssl_context
        )
        self.port = self._server.sockets[0].getsockname()[1]

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None


class KCPListener(Listener):
    """KCP (reliable UDP) listener backed by ``aiokcp``."""

    scheme = "kcp"

    def __init__(self, host: str = "0.0.0.0", port: int = 0) -> None:
        super().__init__(host, port)
        self._server: Any = None

    async def start(self, on_connection: ConnectionHandler) -> None:
        import aiokcp

        async def _cb(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            transport = StreamTransport(reader, writer)
            try:
                await on_connection(transport)
            finally:
                await transport.close()

        self._server = await aiokcp.start_server(_cb, self.host, self._requested_port)
        # Fast mode matching kcp-go / kcp-java defaults (no delay, 20ms flush).
        self._server.set_nodelay(True, 20, 2, True)
        # aiokcp's stream-level start_server does not start the KCP update loop,
        # which owns flushing and retransmission on the server side; without it
        # downstream frames only leave when an inbound packet nudges the state.
        await self._server.start_serving()
        sockname = self._server._transport.get_extra_info("sockname")
        self.port = sockname[1]

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None


class WSListener(Listener):
    """WebSocket listener; pass an ``ssl_context`` to get WSS."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 0,
        path: str = DEFAULT_WS_PATH,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        super().__init__(host, port)
        self.path = path or DEFAULT_WS_PATH
        self.ssl_context = ssl_context
        self.scheme = "wss" if ssl_context is not None else "ws"
        self._server: Any = None

    async def start(self, on_connection: ConnectionHandler) -> None:
        from websockets.asyncio.server import serve

        async def _cb(ws: Any) -> None:
            transport = WebSocketTransport(ws)
            transport.start()
            try:
                await on_connection(transport)
            finally:
                await transport.close()

        self._server = await serve(
            _cb,
            self.host,
            self._requested_port,
            subprotocols=[WS_SUBPROTOCOL],
            ssl=self.ssl_context,
            max_size=None,
        )
        self.port = self._server.sockets[0].getsockname()[1]

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None


DEFAULT_QUIC_ALPN = "rmtt"


class QUICListener(Listener):
    """QUIC listener: one bidirectional stream carries one RMTT connection."""

    scheme = "quic"

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 0,
        certfile: Optional[str] = None,
        keyfile: Optional[str] = None,
        alpn: Optional[list[str]] = None,
        idle_timeout: float = 15 * 60,
    ) -> None:
        super().__init__(host, port)
        self.certfile = certfile
        self.keyfile = keyfile
        # "rmtt" is the ALPN every implementation that negotiates one expects
        # (netty QUIC rejects the handshake without it); servers that ignore
        # ALPN (Go, Python's own client) are unaffected.
        self.alpn = alpn or [DEFAULT_QUIC_ALPN]
        self.idle_timeout = idle_timeout
        self._server: Any = None
        self._tasks: set[asyncio.Task] = set()

    async def start(self, on_connection: ConnectionHandler) -> None:
        from aioquic.asyncio.server import serve
        from aioquic.quic.configuration import QuicConfiguration

        if not self.certfile or not self.keyfile:
            raise ValueError("QUIC listener requires certfile and keyfile")

        configuration = QuicConfiguration(
            is_client=False, alpn_protocols=self.alpn, idle_timeout=self.idle_timeout
        )
        configuration.load_cert_chain(self.certfile, self.keyfile)

        def _factory(quic: Any, stream_handler: Optional[Callable] = None) -> RmttQuicProtocol:
            def _spawn(transport: QuicStreamTransport) -> None:
                task = asyncio.create_task(on_connection(transport))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

            return RmttQuicProtocol(quic, stream_handler=stream_handler, on_stream=_spawn)

        self._server = await serve(
            self.host,
            self._requested_port,
            configuration=configuration,
            create_protocol=_factory,
        )
        sockname = self._server._transport.get_extra_info("sockname")
        self.port = sockname[1]

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
        for task in list(self._tasks):
            task.cancel()
        self._tasks.clear()
