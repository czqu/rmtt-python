"""Shared transport behaviour.

A transport is a bidirectional, ordered byte stream. Higher layers only use
``readexactly`` / ``write`` / ``close`` so TCP, TLS, KCP, WebSocket and QUIC
connections are interchangeable.
"""

from __future__ import annotations

import asyncio
from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class Transport(Protocol):
    """A bidirectional byte stream carrying RMTT frames."""

    async def readexactly(self, n: int) -> bytes: ...

    async def write(self, data: bytes) -> None: ...

    async def close(self) -> None: ...


class BufferedTransport:
    """Buffers message-oriented input (WebSocket frames, QUIC stream data)
    and exposes it as an ``asyncio``-style byte stream."""

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._waiter: Optional[asyncio.Future] = None
        self._eof = False
        self._error: Optional[BaseException] = None

    def feed(self, data: bytes) -> None:
        if not data:
            return
        self._buffer.extend(data)
        self._wake()

    def feed_eof(self) -> None:
        self._eof = True
        self._wake()

    def feed_error(self, exc: BaseException) -> None:
        if self._error is None:
            self._error = exc
        self._wake()

    def _wake(self) -> None:
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    async def readexactly(self, n: int) -> bytes:
        while len(self._buffer) < n:
            if self._error is not None:
                raise self._error
            if self._eof:
                raise asyncio.IncompleteReadError(bytes(self._buffer), n)
            self._waiter = asyncio.get_running_loop().create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None
        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        return data
