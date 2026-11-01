"""RMTT client: connection lifecycle, heartbeats, pushes and reconnects."""

from __future__ import annotations

import asyncio
import inspect
import threading
import time
from typing import Any, Callable, Optional

from ..codec import (
    ACCEPTED,
    ConnRefusedError,
    ConnackPacket,
    ConnectPacket,
    Disc,
    DisconnectPacket,
    PingreqPacket,
    PingrespPacket,
    ProtocolViolationError,
    PushPacket,
    encode_packet,
    read_packet,
)
from ..message import Message
from ..token import ConnectToken, Token
from ..transport import Transport
from ..transport.dial import dial
from .backoff import Backoff
from .options import ClientOptions
from .status import ConnectionState

#: DISCONNECT reasons after which the client must not reconnect.
_NO_RECONNECT_REASONS = {Disc.NORMAL, Disc.SESSION_TAKEN_OVER, Disc.KICKED_BY_ADMIN}


class NotConnectedError(ConnectionError):
    """Raised / reported by push when the link is down."""


class ClientStateError(RuntimeError):
    """Raised when an operation is invalid for the current connection state."""


class AsyncClient:
    """Asyncio-native RMTT client."""

    def __init__(self, options: Optional[ClientOptions] = None) -> None:
        self.options = options or ClientOptions()
        self._handlers: list[Callable[[Message], Any]] = []
        self._transport: Optional[Transport] = None
        self._state = ConnectionState.DISCONNECTED
        self._server_keepalive = 0
        self._last_sent = 0.0
        self._last_received = 0.0
        self._write_lock = asyncio.Lock()
        self._reader_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._stopping = False
        self._backoff = Backoff(self.options.reconnect_jitter)
        self._disconnect_reason: Optional[int] = None

    # ------------------------------------------------------------------ state
    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def is_connected(self) -> bool:
        return self._state in (ConnectionState.CONNECTED, ConnectionState.RECONNECTING)

    @property
    def server_keepalive(self) -> int:
        """Keepalive negotiated by the server (seconds, 0 = disabled)."""
        return self._server_keepalive

    @property
    def disconnect_reason(self) -> Optional[int]:
        """Reason code of the last server DISCONNECT, when one was received."""
        return self._disconnect_reason

    # ---------------------------------------------------------------- connect
    async def connect(self) -> ConnectToken:
        """Establish the connection and complete the CONNECT/CONNACK handshake.

        Failures are reported through ``token.error`` rather than raised, which
        matches the Go ``Connect() Token`` contract.
        """
        token = ConnectToken()
        if self._state in (
            ConnectionState.CONNECTED,
            ConnectionState.CONNECTING,
            ConnectionState.RECONNECTING,
        ):
            token._complete(ClientStateError("client is already connected or connecting"))
            return token
        self._stopping = False
        self._disconnect_reason = None
        self._state = ConnectionState.CONNECTING
        try:
            transport, connack = await self._connect_with_retry()
            token.return_code = connack.return_code
            self._start_session(transport, connack)
            token._complete()
        except BaseException as exc:  # noqa: BLE001 - surfaced through the token
            self._state = ConnectionState.DISCONNECTED
            token._complete(exc)
        return token

    async def _connect_with_retry(self) -> tuple[Transport, ConnackPacket]:
        while True:
            try:
                return await self._connect_once()
            except ConnRefusedError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception:
                if not self.options.connect_retry or self._stopping:
                    raise
                await asyncio.sleep(self.options.connect_retry_interval)

    async def _connect_once(self) -> tuple[Transport, ConnackPacket]:
        servers = self.options.servers
        if not servers:
            raise ValueError("no server to connect to")
        last_error: Optional[BaseException] = None
        for server in servers:
            if self.options.on_connect_attempt is not None:
                await _maybe_await(self.options.on_connect_attempt(server))
            transport: Optional[Transport] = None
            try:
                transport = await dial(
                    server,
                    ssl_context=self.options.tls_context,
                    ws_path=self.options.ws_path,
                    quic_alpn=self.options.quic_alpn,
                    quic_ca_file=self.options.quic_ca_file,
                    quic_insecure=self.options.quic_insecure,
                    connect_timeout=self.options.connect_timeout,
                )
                connack = await self._handshake(transport)
                if connack.return_code != ACCEPTED:
                    raise ConnRefusedError(connack.return_code)
                return transport, connack
            except ConnRefusedError:
                await _close_quietly(transport)
                raise
            except Exception as exc:  # try the next server
                await _close_quietly(transport)
                last_error = exc
        raise last_error or ConnectionError("failed to connect to any server")

    async def _handshake(self, transport: Transport) -> ConnackPacket:
        keepalive = (
            int(self.options.adaptive_max)
            if self.options.adaptive_heartbeat
            else int(self.options.heartbeat)
        )
        connect = ConnectPacket(
            protocol_version=self.options.protocol_version,
            keepalive=max(0, min(keepalive, 0xFFFF)),
            credential=self.options.credential,
        )
        await transport.write(encode_packet(connect))
        packet = await asyncio.wait_for(read_packet(transport), self.options.connect_timeout)
        if not isinstance(packet, ConnackPacket):
            raise ProtocolViolationError("first packet from server was not CONNACK")
        return packet

    def _start_session(self, transport: Transport, connack: ConnackPacket) -> None:
        self._transport = transport
        self._server_keepalive = connack.server_keepalive
        self._state = ConnectionState.CONNECTED
        now = time.monotonic()
        self._last_sent = now
        self._last_received = now
        self._reader_task = asyncio.create_task(self._reader_loop())
        if self.options.heartbeat > 0 or self.options.adaptive_heartbeat:
            self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    # ------------------------------------------------------------------ send
    async def _send(self, packet) -> None:
        transport = self._transport
        if transport is None:
            raise NotConnectedError("not connected")
        async with self._write_lock:
            await transport.write(encode_packet(packet))
            self._last_sent = time.monotonic()

    async def push(self, payload: Any) -> Token:
        """Send an application payload; the token reports completion."""
        token = Token("push")
        if not self.is_connected or self._state is ConnectionState.RECONNECTING:
            token._complete(NotConnectedError("not connected"))
            return token
        data = _coerce_payload(payload)
        try:
            timeout = self.options.write_timeout or 30.0
            await asyncio.wait_for(self._send(PushPacket(payload=data)), timeout)
            token._complete()
        except BaseException as exc:  # noqa: BLE001
            token._complete(exc)
        return token

    # ------------------------------------------------------------- receive
    def add_payload_handler_last(self, handler: Callable[[Message], Any]) -> None:
        self._handlers.append(handler)

    async def _reader_loop(self) -> None:
        error: Optional[BaseException] = None
        try:
            while True:
                transport = self._transport
                if transport is None:
                    return
                packet = await read_packet(transport)
                self._last_received = time.monotonic()
                if isinstance(packet, PushPacket):
                    await self._dispatch(Message(packet.payload))
                elif isinstance(packet, PingreqPacket):
                    await self._send(PingrespPacket())
                elif isinstance(packet, PingrespPacket):
                    continue
                elif isinstance(packet, DisconnectPacket):
                    self._disconnect_reason = packet.return_code
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - link failure
            error = exc
        finally:
            self._on_link_down(error)

    async def _dispatch(self, message: Message) -> None:
        # Like the Go client, every handler is dispatched independently so a
        # slow callback cannot stall reads or heartbeats.
        for handler in list(self._handlers):
            if inspect.iscoroutinefunction(handler):
                asyncio.create_task(handler(message))
            else:
                asyncio.get_running_loop().run_in_executor(None, _call_sync_handler, handler, message)

    def _on_link_down(self, error: Optional[BaseException]) -> None:
        if self._state is not ConnectionState.CONNECTED:
            return
        self._state = ConnectionState.RECONNECTING if self.options.auto_reconnect else ConnectionState.DISCONNECTED
        self._cancel_task(self._keepalive_task)
        self._keepalive_task = None
        self._close_transport()
        if self.options.on_connection_lost is not None:
            asyncio.create_task(_maybe_await(self.options.on_connection_lost(error or ConnectionError("connection lost"))))
        if self._state is ConnectionState.RECONNECTING and not self._stopping:
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())
        else:
            self._state = ConnectionState.DISCONNECTED

    async def _reconnect_loop(self) -> None:
        while not self._stopping:
            reason = self._disconnect_reason
            if reason is not None and reason in _NO_RECONNECT_REASONS:
                self._state = ConnectionState.DISCONNECTED
                return
            if reason == Disc.KEEPALIVE_TIMEOUT:
                self._backoff.reset("reconnect")
            sleep = self._backoff.next_sleep(
                "reconnect",
                self.options.reconnect_base,
                self.options.max_reconnect_interval,
                self.options.connect_timeout,
                skip_first=True,
                now=time.monotonic(),
            )
            await asyncio.sleep(self._backoff.jitter(sleep))
            if self._stopping:
                break
            if self.options.on_reconnecting is not None:
                await _maybe_await(self.options.on_reconnecting())
            try:
                transport, connack = await self._connect_once()
            except Exception:  # noqa: BLE001 - keep retrying
                continue
            if self._stopping:
                await _close_quietly(transport)
                break
            self._disconnect_reason = None
            self._start_session(transport, connack)
            return
        self._state = ConnectionState.DISCONNECTED

    # -------------------------------------------------------------- keepalive
    async def _keepalive_loop(self) -> None:
        if self.options.adaptive_heartbeat:
            await self._adaptive_keepalive()
            return
        try:
            while True:
                heartbeat = self._server_keepalive or self.options.heartbeat
                if heartbeat <= 0:
                    return
                await asyncio.sleep(max(heartbeat / 4.0, 1.0))
                heartbeat = self._server_keepalive or self.options.heartbeat
                if heartbeat <= 0:
                    continue
                now = time.monotonic()
                if now - self._last_sent >= heartbeat:
                    await self._send(PingreqPacket())
                if now - self._last_received >= heartbeat * 1.5:
                    self._on_link_down(TimeoutError("keepalive receive timeout"))
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._on_link_down(exc)

    async def _adaptive_keepalive(self) -> None:
        """Port of the Go/Java adaptive heartbeat probing state machine."""
        short = max(float(self.options.adaptive_short), 1.0)
        ceiling = max(float(self.options.adaptive_max), short)
        kp = self._server_keepalive
        if kp <= 0:
            return
        ceiling = min(ceiling, float(kp))
        if ceiling <= short:
            self.options.adaptive_heartbeat = False
            await self._keepalive_loop()
            return

        probe_count = max(self.options.probe_count, 1)
        window = self.options.response_window or 2.0
        step = max(float(self.options.fine_step), 1.0)
        PHASE_SHORT, PHASE_DOUBLE, PHASE_FINE, PHASE_STABLE = range(4)
        phase = PHASE_SHORT
        interval = short
        last_success = 0.0
        stable_interval = short
        short_ok = 0
        sent_at: Optional[float] = None
        awaiting = False

        def got_response() -> bool:
            return sent_at is not None and self._last_received >= sent_at

        async def send_probe() -> None:
            nonlocal sent_at, awaiting
            await self._send(PingreqPacket())
            sent_at = time.monotonic()
            awaiting = True

        try:
            while True:
                await asyncio.sleep(0.25)
                now = time.monotonic()
                if phase == PHASE_SHORT:
                    if awaiting:
                        if got_response():
                            awaiting = False
                            short_ok += 1
                            if short_ok >= probe_count:
                                phase = PHASE_DOUBLE
                                interval = short
                                last_success = short
                                awaiting = False
                        elif now - (sent_at or now) >= window:
                            self._on_link_down(TimeoutError("adaptive heartbeat short probe failed"))
                            return
                    elif now - self._last_sent >= interval:
                        await send_probe()
                elif phase in (PHASE_DOUBLE, PHASE_FINE):
                    if awaiting:
                        if got_response():
                            awaiting = False
                            last_success = interval
                            if phase == PHASE_DOUBLE:
                                nxt = min(interval * 2, ceiling)
                            else:
                                nxt = interval + step
                            if nxt <= interval:
                                stable_interval = max(last_success * 0.9, short)
                                phase = PHASE_STABLE
                                awaiting = False
                            else:
                                interval = nxt
                        elif now - (sent_at or now) >= window:
                            stable_interval = max(last_success * 0.9, short)
                            phase = PHASE_STABLE
                            awaiting = False
                    elif now - self._last_sent >= interval:
                        await send_probe()
                else:  # PHASE_STABLE
                    if now - self._last_received >= stable_interval * 1.5:
                        phase = PHASE_SHORT
                        short_ok = 0
                        interval = short
                        last_success = 0.0
                        stable_interval = short
                        awaiting = False
                    elif now - self._last_sent >= stable_interval:
                        await send_probe()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._on_link_down(exc)

    # ------------------------------------------------------------- disconnect
    async def disconnect(self, quiesce: float = 1.0) -> None:
        """Send DISCONNECT and tear the connection down.

        ``quiesce`` is the number of seconds to wait for the DISCONNECT frame to
        be flushed (the Go API expresses this in milliseconds).
        """
        if self._state is ConnectionState.DISCONNECTED:
            return
        self._stopping = True
        self._state = ConnectionState.DISCONNECTING
        self._cancel_task(self._reconnect_task)
        self._reconnect_task = None
        try:
            await asyncio.wait_for(self._send(DisconnectPacket(return_code=Disc.NORMAL)), quiesce)
        except Exception:  # noqa: BLE001 - best effort
            pass
        self._cancel_task(self._reader_task)
        self._reader_task = None
        self._cancel_task(self._keepalive_task)
        self._keepalive_task = None
        await self._close_transport_async()
        self._state = ConnectionState.DISCONNECTED

    async def shutdown(self) -> None:
        """Alias for :meth:`disconnect` without sending a frame (used on exit)."""
        self._stopping = True
        self._cancel_task(self._reconnect_task)
        self._cancel_task(self._reader_task)
        self._cancel_task(self._keepalive_task)
        self._reconnect_task = self._reader_task = self._keepalive_task = None
        await self._close_transport_async()
        self._state = ConnectionState.DISCONNECTED

    # ----------------------------------------------------------------- helpers
    @staticmethod
    def _cancel_task(task: Optional[asyncio.Task]) -> None:
        if task is not None and not task.done():
            task.cancel()

    def _close_transport(self) -> None:
        transport = self._transport
        self._transport = None
        if transport is not None:
            asyncio.create_task(_close_quietly(transport))

    async def _close_transport_async(self) -> None:
        transport = self._transport
        self._transport = None
        if transport is not None:
            await _close_quietly(transport)

    async def wait_until_disconnected(self) -> None:
        """Block until a reconnect attempt ends in the disconnected state."""
        while self._state is not ConnectionState.DISCONNECTED:
            await asyncio.sleep(0.05)


class Client:
    """Blocking facade around :class:`AsyncClient` (paho-style API).

    The client owns a private event loop thread, so ``connect`` / ``push``
    return tokens that can be waited for without an ``await``.
    """

    def __init__(self, options: Optional[ClientOptions] = None) -> None:
        self.options = options or ClientOptions()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, name="rmtt-client", daemon=True)
        self._thread.start()
        self._async = AsyncClient(self.options)

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _submit(self, coro) -> None:
        asyncio.run_coroutine_threadsafe(coro, self._loop)

    @property
    def state(self) -> ConnectionState:
        return self._async.state

    @property
    def server_keepalive(self) -> int:
        return self._async.server_keepalive

    def is_connected(self) -> bool:
        return self._async.is_connected

    def add_payload_handler_last(self, handler: Callable[[Message], Any]) -> None:
        self._async.add_payload_handler_last(handler)

    def connect(self) -> ConnectToken:
        token = ConnectToken()

        async def _do() -> None:
            inner = await self._async.connect()
            token.return_code = inner.return_code
            token._complete(inner.error)

        self._submit(_do())
        return token

    def push(self, payload: Any) -> Token:
        token = Token("push")

        async def _do() -> None:
            inner = await self._async.push(payload)
            token._complete(inner.error)

        self._submit(_do())
        return token

    def disconnect(self, quiesce: float = 1.0) -> None:
        future = asyncio.run_coroutine_threadsafe(self._async.disconnect(quiesce), self._loop)
        try:
            future.result(timeout=quiesce + 5)
        except Exception:  # noqa: BLE001 - best effort shutdown
            pass

    def shutdown(self) -> None:
        """Stop the background loop; the client cannot be reused afterwards."""
        try:
            asyncio.run_coroutine_threadsafe(self._async.shutdown(), self._loop).result(timeout=5)
        except Exception:  # noqa: BLE001
            pass
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        if not self._loop.is_closed():
            self._loop.close()


def _coerce_payload(payload: Any) -> bytes:
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, (bytearray, memoryview)):
        return bytes(payload)
    if isinstance(payload, str):
        return payload.encode("utf-8")
    raise TypeError(f"unsupported payload type: {type(payload)!r}")


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _close_quietly(transport: Optional[Transport]) -> None:
    if transport is None:
        return
    try:
        await transport.close()
    except Exception:  # noqa: BLE001
        pass


def _call_sync_handler(handler, message: Message) -> None:
    try:
        handler(message)
    except Exception:  # noqa: BLE001 - a handler bug must not kill the link
        pass
