"""RMTT server: authenticate devices, route pushes, enforce keepalive."""

from __future__ import annotations

import asyncio
import inspect
import threading
from typing import Any, Optional

from ..codec import (
    ACCEPTED,
    ERR_REFUSED_BAD_PROTOCOL_VERSION,
    ERR_REFUSED_NOT_AUTHORISED,
    MAGIC_NUMBER,
    ConnackPacket,
    ConnectPacket,
    Disc,
    DisconnectPacket,
    PingreqPacket,
    PingrespPacket,
    ProtocolViolationError,
    PushPacket,
    UnknownPacketTypeError,
    encode_packet,
    read_packet,
)
from ..transport import Transport
from ..transport.listeners import Listener, StreamListener
from .options import AuthResult, ServerOptions


class DeviceOfflineError(LookupError):
    """The requested device has no active connection."""


class ServerConnection:
    """A live device session; all writes are serialized."""

    def __init__(self, device_id: str, transport: Transport) -> None:
        self.device_id = device_id
        self.transport = transport
        self.active = True
        self.last_received = asyncio.get_running_loop().time()
        self._write_lock = asyncio.Lock()

    async def send(self, packet) -> None:
        async with self._write_lock:
            await self.transport.write(encode_packet(packet))

    async def send_push(self, payload: bytes) -> None:
        await self.send(PushPacket(payload=payload))

    async def send_disconnect(self, reason: int) -> None:
        try:
            await self.send(DisconnectPacket(return_code=reason))
        except Exception:  # noqa: BLE001 - best effort
            pass

    async def close(self) -> None:
        self.active = False
        try:
            await self.transport.close()
        except Exception:  # noqa: BLE001
            pass


class AsyncServer:
    """Asyncio-native RMTT server."""

    def __init__(self, options: Optional[ServerOptions] = None) -> None:
        self.options = options or ServerOptions()
        self._listeners: list[Listener] = list(self.options.listeners) or [
            StreamListener(self.options.host, self.options.port)
        ]
        self._connections: dict[str, ServerConnection] = {}
        self._sessions: set[asyncio.Task] = set()
        self._started = False
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------ state
    @property
    def port(self) -> int:
        return self._listeners[0].port if self._listeners else 0

    @property
    def listeners(self) -> list[Listener]:
        return list(self._listeners)

    def is_online(self, device_id: str) -> bool:
        conn = self._connections.get(device_id)
        return conn is not None and conn.active

    @property
    def online_count(self) -> int:
        return sum(1 for conn in self._connections.values() if conn.active)

    def device_ids(self) -> list[str]:
        return [device_id for device_id, conn in self._connections.items() if conn.active]

    # -------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        if self._started:
            return
        self._stop_event.clear()
        for listener in self._listeners:
            await listener.start(self._handle_connection)
        self._started = True

    async def serve_forever(self) -> None:
        await self.start()
        await self._stop_event.wait()

    async def close(self) -> None:
        if not self._started:
            return
        self._started = False
        for conn in list(self._connections.values()):
            await conn.send_disconnect(Disc.SERVER_SHUTDOWN)
            await conn.close()
        self._connections.clear()
        for listener in self._listeners:
            await listener.close()
        for task in list(self._sessions):
            task.cancel()
        self._sessions.clear()
        self._stop_event.set()

    # --------------------------------------------------------------- routing
    async def push(self, device_id: str, payload: Any) -> None:
        """Send a downstream PUSH; raises :class:`DeviceOfflineError` when offline."""
        conn = self._connections.get(device_id)
        if conn is None or not conn.active:
            raise DeviceOfflineError(f"device {device_id} not connected")
        data = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        await conn.send_push(data)

    async def kick(self, device_id: str, reason: int = Disc.KICKED_BY_ADMIN) -> None:
        """Disconnect a device with the given reason code."""
        conn = self._connections.get(device_id)
        if conn is None or not conn.active:
            raise DeviceOfflineError(f"device {device_id} not connected")
        self._connections.pop(device_id, None)
        await conn.send_disconnect(reason)
        await conn.close()
        self._notify_closed(device_id, f"kicked with reason 0x{reason:02x}")

    # ------------------------------------------------------------- connection
    async def _handle_connection(self, transport: Transport) -> None:
        device_id: Optional[str] = None
        conn: Optional[ServerConnection] = None
        reason = "connection closed"
        try:
            try:
                packet = await asyncio.wait_for(
                    read_packet(transport), self.options.connect_timeout
                )
            except (asyncio.TimeoutError, ProtocolViolationError, UnknownPacketTypeError):
                return
            if not isinstance(packet, ConnectPacket):
                return
            if packet.magic_number != MAGIC_NUMBER:
                return
            if packet.protocol_version != 1:
                await transport.write(
                    encode_packet(ConnackPacket(return_code=ERR_REFUSED_BAD_PROTOCOL_VERSION))
                )
                return

            auth = self.options.authenticator
            if auth is None:
                result = AuthResult.allow(packet.credential)
            else:
                result = await _maybe_await(auth(packet.credential))
            if not result.allowed:
                await transport.write(
                    encode_packet(ConnackPacket(return_code=result.return_code))
                )
                return
            if not result.device_id:
                await transport.write(
                    encode_packet(ConnackPacket(return_code=ERR_REFUSED_NOT_AUTHORISED))
                )
                return

            device_id = result.device_id
            server_keepalive = self.options.keepalive_policy.decide(packet.keepalive)
            conn = ServerConnection(device_id, transport)
            await transport.write(
                encode_packet(
                    ConnackPacket(
                        return_code=ACCEPTED, server_keepalive=server_keepalive
                    )
                )
            )

            previous = self._connections.get(device_id)
            self._connections[device_id] = conn
            if previous is not None and previous is not conn:
                self._spawn(self._take_over(previous))

            self._notify_established(device_id)
            reason = await self._session_loop(conn, server_keepalive)
        except asyncio.CancelledError:
            reason = "server shutdown"
            raise
        except Exception:  # noqa: BLE001 - any transport error ends the session
            reason = "connection closed"
        finally:
            await transport.close()
            if device_id is not None and conn is not None:
                if self._connections.get(device_id) is conn:
                    self._connections.pop(device_id, None)
                self._notify_closed(device_id, reason)

    async def _take_over(self, previous: ServerConnection) -> None:
        await previous.send_disconnect(Disc.SESSION_TAKEN_OVER)
        await previous.close()

    async def _session_loop(self, conn: ServerConnection, server_keepalive: int) -> str:
        watchdog: Optional[asyncio.Task] = None
        if server_keepalive > 0:
            watchdog = asyncio.create_task(self._keepalive_watchdog(conn, server_keepalive))
        loop = asyncio.get_running_loop()
        try:
            while conn.active:
                packet = await read_packet(conn.transport)
                conn.last_received = loop.time()
                if isinstance(packet, PushPacket):
                    self._dispatch_message(conn.device_id, packet.payload)
                elif isinstance(packet, PingreqPacket):
                    await conn.send(PingrespPacket())
                elif isinstance(packet, DisconnectPacket):
                    return "connection closed"
                else:
                    await conn.send_disconnect(Disc.PROTOCOL_VIOLATION)
                    return "protocol violation"
        except (ProtocolViolationError, UnknownPacketTypeError):
            await conn.send_disconnect(Disc.PROTOCOL_VIOLATION)
            return "protocol violation"
        except asyncio.CancelledError:
            return "server shutdown"
        except Exception:  # noqa: BLE001
            return "connection closed"
        finally:
            if watchdog is not None and not watchdog.done():
                watchdog.cancel()

    async def _keepalive_watchdog(self, conn: ServerConnection, server_keepalive: int) -> None:
        """Reap a silent device after 1.5x the negotiated keepalive."""
        deadline = server_keepalive * 1.5
        loop = asyncio.get_running_loop()
        try:
            while conn.active:
                await asyncio.sleep(min(max(deadline / 4, 0.1), 1.0))
                if loop.time() - conn.last_received >= deadline:
                    await conn.send_disconnect(Disc.KEEPALIVE_TIMEOUT)
                    await conn.close()
                    return
        except asyncio.CancelledError:
            raise

    # ---------------------------------------------------------------- helpers
    def _dispatch_message(self, device_id: str, payload: bytes) -> None:
        """Dispatch an uplink PUSH without blocking the connection's read loop.

        Mirrors the Go server, which runs the handler in its own goroutine:
        coroutine handlers become tasks, plain callables run in the default
        executor so they may safely call the blocking ``Server.push`` API.
        """
        handler = self.options.message_handler
        if handler is None:
            return
        if inspect.iscoroutinefunction(handler):
            self._spawn(handler(device_id, payload))
            return
        asyncio.get_running_loop().run_in_executor(
            None, _call_sync_handler, handler, device_id, payload
        )

    def _notify_established(self, device_id: str) -> None:
        listener = self.options.connection_listener
        if listener is not None:
            listener.on_connection_established(device_id)

    def _notify_closed(self, device_id: str, reason: str) -> None:
        listener = self.options.connection_listener
        if listener is not None:
            listener.on_connection_closed(device_id, reason)

    def _spawn(self, awaitable) -> None:
        task = asyncio.ensure_future(awaitable)
        self._sessions.add(task)
        task.add_done_callback(self._sessions.discard)


class Server:
    """Blocking facade around :class:`AsyncServer`."""

    def __init__(self, options: Optional[ServerOptions] = None) -> None:
        self.options = options or ServerOptions()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, name="rmtt-server", daemon=True)
        self._thread.start()
        self._async = AsyncServer(self.options)
        self._started = False

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    @property
    def port(self) -> int:
        return self._async.port

    def start(self) -> None:
        """Bind every listener and return (non-blocking)."""
        asyncio.run_coroutine_threadsafe(self._async.start(), self._loop).result(timeout=30)
        self._started = True

    def listen_and_serve(self) -> None:
        """Bind every listener and block until :meth:`close` is called."""
        self.start()
        try:
            while self._started:
                self._thread.join(0.2)
        except KeyboardInterrupt:  # pragma: no cover - interactive use
            self.close()

    def is_online(self, device_id: str) -> bool:
        return self._async.is_online(device_id)

    def online_count(self) -> int:
        return self._async.online_count

    def device_ids(self) -> list[str]:
        return self._async.device_ids()

    def push(self, device_id: str, payload: Any) -> None:
        asyncio.run_coroutine_threadsafe(self._async.push(device_id, payload), self._loop).result(
            timeout=30
        )

    def kick(self, device_id: str, reason: int = Disc.KICKED_BY_ADMIN) -> None:
        asyncio.run_coroutine_threadsafe(self._async.kick(device_id, reason), self._loop).result(
            timeout=30
        )

    def close(self) -> None:
        if not self._started:
            return
        self._started = False
        try:
            asyncio.run_coroutine_threadsafe(self._async.close(), self._loop).result(timeout=10)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)
            if not self._loop.is_closed():
                self._loop.close()


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _call_sync_handler(handler, device_id: str, payload: bytes) -> None:
    try:
        handler(device_id, payload)
    except Exception:  # noqa: BLE001 - a handler bug must not kill the link
        pass
