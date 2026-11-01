# rmtt-python

Python client and server for the **RMTT** (Remote Message Telemetry Transport)
protocol, wire-compatible with [rmtt-go](https://github.com/czqu/rmtt-go),
[rmtt-java](https://github.com/czqu/rmtt-java),
[rmtt-rust](https://github.com/czqu/rmtt-rust) and
[rmtt-cpp](https://github.com/czqu/rmtt-cpp).

- Six transports: `tcp://` `tls://` `ws://` `wss://` `kcp://` `quic://`
- Fixed, adaptive and disabled heartbeats, negotiated by the server
- Automatic reconnect with exponential backoff and jitter
- Session takeover, kick with reason codes, keepalive reaping, protocol-violation handling
- Asyncio core plus a blocking facade (`token.wait()`) modelled on `paho.mqtt`

## Install

```bash
pip install -e .
```

Optional transports pull `websockets`, `aioquic` and `aiokcp` (declared as
dependencies). The codec itself has no third-party dependencies.

## Client

```python
from rmtt import Client, ClientOptions

options = ClientOptions()
options.add_server("tcp://127.0.0.1:18883")
options.set_credential("dev-001")

client = Client(options)
client.add_payload_handler_last(lambda msg: print("<-", msg.payload))

token = client.connect()
token.wait(5)
assert token.error is None

client.push("hello").wait(5)
client.disconnect()
```

The asyncio API mirrors it:

```python
from rmtt import AsyncClient

client = AsyncClient(options)
token = await client.connect()
await client.push("hello")
await client.disconnect()
```

## Server

```python
from rmtt import Server, ServerOptions
from rmtt.server import AuthResult

options = ServerOptions(port=18883)
options.set_authenticator(lambda credential: AuthResult.allow(credential))
options.set_message_handler(lambda device_id, payload: print(device_id, payload))

server = Server(options)
server.start()
server.push("dev-001", b"from-server")
```

Use `rmtt.transport.listeners` to expose TLS, WebSocket, KCP or QUIC:

```python
from rmtt.transport import listeners

options.listeners = [listeners.WSListener("0.0.0.0", 18886)]
options.listeners.append(
    listeners.QUICListener("0.0.0.0", 18885, certfile="cert.pem", keyfile="key.pem")
)
```

## Tests

```bash
pytest                       # codec vectors + client/server integration on every transport
python e2e/matrix.py --expected "go,java,quic"  # cross-language matrix (needs ./e2e/setup_refs.sh first)
python e2e/protocol_suite.py # raw-wire conformance against every server
```

`e2e/setup_refs.sh` clones and builds the Go / Java / Rust / C++ reference
implementations, then runs the full server x client x transport matrix.
