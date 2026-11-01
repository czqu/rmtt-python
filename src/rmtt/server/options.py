"""Server configuration and callback contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol

from ..transport.listeners import Listener

DEFAULT_PORT = 18883


class KeepalivePolicy:
    """Server-side keepalive negotiation.

    The server owns the final heartbeat: the client's CONNECT proposal is
    clamped into ``[min_seconds, max_seconds]``, 0 maps to ``default_seconds``
    unless ``allow_disable`` is set (which disables enforcement).
    """

    def __init__(
        self,
        min_seconds: int = 30,
        max_seconds: int = 600,
        default_seconds: int = 60,
        allow_disable: bool = False,
    ) -> None:
        self.min_seconds = min_seconds
        self.max_seconds = max_seconds
        self.default_seconds = default_seconds
        self.allow_disable = allow_disable

    def decide(self, client_keepalive: int) -> int:
        if client_keepalive <= 0:
            return 0 if self.allow_disable else self.default_seconds
        return max(self.min_seconds, min(client_keepalive, self.max_seconds))

    def __repr__(self) -> str:
        return (
            f"KeepalivePolicy(min={self.min_seconds}, max={self.max_seconds}, "
            f"default={self.default_seconds}, allow_disable={self.allow_disable})"
        )


class AuthResult:
    """Outcome of an authenticator callback."""

    def __init__(self, allowed: bool, device_id: str = "", return_code: int = 0x03) -> None:
        self.allowed = allowed
        self.device_id = device_id
        self.return_code = return_code

    @classmethod
    def allow(cls, device_id: str) -> "AuthResult":
        return cls(True, device_id, 0x00)

    @classmethod
    def reject(cls) -> "AuthResult":
        return cls(False, "", 0x03)

    @classmethod
    def reject_with(cls, return_code: int) -> "AuthResult":
        return cls(False, "", return_code)


class Authenticator(Protocol):
    def __call__(self, credential: str) -> AuthResult: ...


class ConnectionListener:
    """Lifecycle callbacks invoked by the server (all optional)."""

    def on_connection_established(self, device_id: str) -> None:  # pragma: no cover
        pass

    def on_connection_closed(self, device_id: str, reason: str) -> None:  # pragma: no cover
        pass


@dataclass
class ServerOptions:
    """Configuration for :class:`rmtt.Server`."""

    port: int = DEFAULT_PORT
    host: str = "0.0.0.0"
    listeners: list[Listener] = field(default_factory=list)
    authenticator: Optional[Callable[[str], AuthResult]] = None
    message_handler: Optional[Callable[[str, bytes], Any]] = None
    connection_listener: Optional[ConnectionListener] = None
    keepalive_policy: KeepalivePolicy = field(default_factory=KeepalivePolicy)
    connect_timeout: float = 10.0

    def add_listener(self, listener: Listener) -> "ServerOptions":
        self.listeners.append(listener)
        return self

    def set_port(self, port: int) -> "ServerOptions":
        self.port = port
        return self

    def set_authenticator(self, authenticator: Callable[[str], AuthResult]) -> "ServerOptions":
        self.authenticator = authenticator
        return self

    def set_message_handler(self, handler: Callable[[str, bytes], Any]) -> "ServerOptions":
        self.message_handler = handler
        return self

    def set_connection_listener(self, listener: ConnectionListener) -> "ServerOptions":
        self.connection_listener = listener
        return self

    def set_keepalive_policy(self, policy: KeepalivePolicy) -> "ServerOptions":
        self.keepalive_policy = policy
        return self
