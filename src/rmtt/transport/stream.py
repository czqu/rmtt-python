"""TCP / TLS / KCP transports backed by ``asyncio`` streams."""

from __future__ import annotations

import asyncio
from typing import Optional


class StreamTransport:
    """Wraps an ``asyncio`` reader/writer pair."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._closed = False

    async def readexactly(self, n: int) -> bytes:
        return await self._reader.readexactly(n)

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except (ConnectionError, RuntimeError, OSError):
            pass

    @property
    def peername(self) -> Optional[str]:
        try:
            return str(self._writer.get_extra_info("peername"))
        except Exception:  # pragma: no cover - defensive
            return None
