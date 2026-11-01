"""Inbound PUSH message."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Message:
    """A PUSH payload received from the peer."""

    payload: bytes

    def payload_str(self, encoding: str = "utf-8", errors: str = "replace") -> str:
        return self.payload.decode(encoding, errors)

    def __str__(self) -> str:
        return self.payload_str()
