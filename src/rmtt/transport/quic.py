"""QUIC transport built on ``aioquic``.

One bidirectional stream per RMTT connection, mirroring the Go / Java / C++
transports.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.events import ConnectionTerminated, StreamDataReceived

from .base import BufferedTransport


class QuicStreamTransport(BufferedTransport):
    """A single bidirectional QUIC stream exposed as a byte stream."""

    def __init__(self, protocol: "RmttQuicProtocol", stream_id: int) -> None:
        super().__init__()
        self._protocol = protocol
        self._stream_id = stream_id
        self._closed = False
        self._close_hook: Optional[Callable] = None
        protocol._rmtt_streams[stream_id] = self

    @property
    def stream_id(self) -> int:
        return self._stream_id

    def set_close_hook(self, hook: Callable) -> None:
        """Register an async callback run when the transport is closed (used by
        the client to tear down the owning QUIC connection)."""
        self._close_hook = hook

    async def write(self, data: bytes) -> None:
        self._protocol.write_stream(self._stream_id, data)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._protocol.close_stream(self._stream_id)
        except Exception:
            pass
        self._protocol._rmtt_streams.pop(self._stream_id, None)
        if self._close_hook is not None:
            await self._close_hook()


class RmttQuicProtocol(QuicConnectionProtocol):
    """Routes inbound stream data to :class:`QuicStreamTransport` objects."""

    def __init__(
        self,
        quic: Any,
        stream_handler: Optional[Callable] = None,
        on_stream: Optional[Callable[[QuicStreamTransport], None]] = None,
    ) -> None:
        super().__init__(quic, stream_handler=stream_handler)
        self._rmtt_streams: dict[int, QuicStreamTransport] = {}
        self._on_stream = on_stream

    def open_transport(self) -> QuicStreamTransport:
        """Open the stream a client uses to speak RMTT."""
        stream_id = self._quic.get_next_available_stream_id()
        return QuicStreamTransport(self, stream_id)

    def quic_event_received(self, event: Any) -> None:
        if isinstance(event, StreamDataReceived):
            transport = self._rmtt_streams.get(event.stream_id)
            if transport is None:
                transport = QuicStreamTransport(self, event.stream_id)
                if self._on_stream is not None:
                    self._on_stream(transport)
            transport.feed(event.data)
            if event.end_stream:
                transport.feed_eof()
        elif isinstance(event, ConnectionTerminated):
            for transport in list(self._rmtt_streams.values()):
                transport.feed_eof()

    def write_stream(self, stream_id: int, data: bytes) -> None:
        self._quic.send_stream_data(stream_id, data)
        self.transmit()

    def close_stream(self, stream_id: int) -> None:
        self._quic.send_stream_data(stream_id, b"", end_stream=True)
        self.transmit()

    async def wait_closed(self) -> None:
        await self._closed.wait()
