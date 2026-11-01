"""Client connection states."""

from __future__ import annotations

from enum import Enum


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    DISCONNECTING = "disconnecting"
    CONNECTING = "connecting"
    RECONNECTING = "reconnecting"
    CONNECTED = "connected"
