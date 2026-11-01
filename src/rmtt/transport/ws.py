"""WebSocket / WebSocket-over-TLS transport.

One RMTT packet travels in one WebSocket binary frame (matching the Go, Java
and C++ implementations), but the reader is tolerant of frames that carry a
partial or several frames, so it behaves as a byte stream.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from .base import BufferedTransport

DEFAULT_WS_PATH = "/rmtt"
WS_SUBPROTOCOL = "rmtt"


class WebSocketTransport(BufferedTransport):
    """Adapts a ``websockets`` connection to the transport interface."""

    def __init__(self, ws: Any) -> None:
        super().__init__()
        self._ws = ws
        self._pump_task: Optional[asyncio.Task] = None
        self._closed = False

    def start(self) -> None:
        self._pump_task = asyncio.create_task(self._pump())

    async def _pump(self) -> None:
        try:
            async for message in self._ws:
                if isinstance(message, str):
                    message = message.encode("utf-8")
                self.feed(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # connection closed / protocol error
            self.feed_error(exc)
        finally:
            self.feed_eof()

    async def write(self, data: bytes) -> None:
        await self._ws.send(data)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._pump_task is not None:
            self._pump_task.cancel()
            try:
                await self._pump_task
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await self._ws.close()
        except Exception:
            pass
