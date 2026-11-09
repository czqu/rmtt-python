"""Tokens: awaitable / blocking completion handles for async operations."""

from __future__ import annotations

import asyncio
import concurrent.futures
from typing import Optional


class Token:
    """Tracks the completion of a connect / push / disconnect operation.

    The same object supports both programming styles::

        token = client.connect()
        if token.wait(5) and token.error is None: ...

        await token
    """

    def __init__(self, kind: str = "") -> None:
        self.kind = kind
        self._future: concurrent.futures.Future = concurrent.futures.Future()

    # -- producers ---------------------------------------------------------
    def _complete(self, error: Optional[BaseException] = None) -> None:
        if self._future.done():
            return
        if error is None:
            self._future.set_result(None)
        else:
            self._future.set_exception(error)

    # -- consumers ---------------------------------------------------------
    def wait(self, timeout: Optional[float] = None) -> bool:
        """Block until completion (or ``timeout`` seconds); returns True when done."""
        try:
            self._future.exception(timeout)
        except concurrent.futures.TimeoutError:
            return False
        except asyncio.CancelledError:
            # The token was completed through cancellation of an awaiter; the
            # operation itself is finished, so report completion.
            return True
        return True

    def wait_timeout(self, timeout: float) -> bool:
        return self.wait(timeout)

    def done(self) -> bool:
        return self._future.done()

    @property
    def error(self) -> Optional[BaseException]:
        if not self._future.done():
            return None
        if self._future.cancelled():
            return asyncio.CancelledError()
        return self._future.exception()

    def result(self) -> None:
        self._future.result()

    def __await__(self):
        # Shield the shared future: cancelling ``await token`` must not cancel
        # the underlying operation, whose outcome is still reported via
        # ``wait()`` / ``error``.
        return asyncio.shield(asyncio.wrap_future(self._future)).__await__()

    def __repr__(self) -> str:
        state = "done" if self._future.done() else "pending"
        return f"<Token {self.kind} {state}>"


class ConnectToken(Token):
    """Token for CONNECT; exposes the server's CONNACK return code."""

    def __init__(self) -> None:
        super().__init__("connect")
        self.return_code: int = 0
