"""Client configuration."""

from __future__ import annotations

import ssl
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from ..transport.ws import DEFAULT_WS_PATH


@dataclass
class ClientOptions:
    """Configuration for :class:`rmtt.Client`.

    Defaults mirror ``rmtt-go``: 10 s heartbeat, 30 s connect timeout,
    automatic reconnect with a 1 s base and 25 % jitter.
    """

    servers: list[str] = field(default_factory=list)
    credential: str = ""
    heartbeat: float = 10.0
    protocol_version: int = 1
    connect_retry: bool = True
    connect_retry_interval: float = 30.0
    connect_timeout: float = 30.0
    write_timeout: float = 0.0
    auto_reconnect: bool = True
    max_reconnect_interval: float = 600.0
    reconnect_base: float = 1.0
    reconnect_jitter: float = 0.25

    # Adaptive heartbeat (ported from the Go/Java state machine).
    adaptive_heartbeat: bool = False
    adaptive_short: float = 1.0
    adaptive_max: float = 60.0
    probe_count: int = 3
    response_window: float = 2.0
    fine_step: float = 5.0

    # Transports.
    tls_context: Optional[ssl.SSLContext] = None
    ws_path: str = DEFAULT_WS_PATH
    quic_alpn: Optional[list[str]] = None
    quic_ca_file: Optional[str] = None
    quic_insecure: bool = True

    # Hooks.
    on_connection_lost: Optional[Callable[[BaseException], Awaitable[None] | None]] = None
    on_reconnecting: Optional[Callable[[], Awaitable[None] | None]] = None
    on_connect_attempt: Optional[Callable[[str], Awaitable[None] | None]] = None

    def add_server(self, server: str) -> "ClientOptions":
        """Append a server URL; a bare address is treated as TCP."""
        if server.startswith(":"):
            server = "127.0.0.1" + server
        if "://" not in server:
            server = "tcp://" + server
        self.servers.append(server)
        return self

    def set_credential(self, credential: str) -> "ClientOptions":
        self.credential = credential
        return self

    def set_heartbeat(self, seconds: float) -> "ClientOptions":
        self.heartbeat = float(seconds)
        self.adaptive_heartbeat = False
        return self

    def set_adaptive_heartbeat(self, short_seconds: float, max_seconds: float) -> "ClientOptions":
        if short_seconds < 1:
            raise ValueError("short_seconds must be >= 1")
        if max_seconds < short_seconds:
            raise ValueError("max_seconds must be >= short_seconds")
        self.adaptive_heartbeat = True
        self.adaptive_short = float(short_seconds)
        self.adaptive_max = float(max_seconds)
        return self

    def set_connect_timeout(self, seconds: float) -> "ClientOptions":
        self.connect_timeout = float(seconds)
        return self

    def set_write_timeout(self, seconds: float) -> "ClientOptions":
        self.write_timeout = float(seconds)
        return self

    def set_tls_context(self, context: ssl.SSLContext) -> "ClientOptions":
        self.tls_context = context
        return self

    def set_ws_path(self, path: str) -> "ClientOptions":
        self.ws_path = path or DEFAULT_WS_PATH
        return self

    def set_auto_reconnect(self, enabled: bool) -> "ClientOptions":
        self.auto_reconnect = bool(enabled)
        return self

    def set_connect_retry(self, enabled: bool) -> "ClientOptions":
        self.connect_retry = bool(enabled)
        return self
