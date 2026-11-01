"""E2E echo server used by the cross-language interop matrix.

Usage:
    python -m e2e.harness.server <scheme> <port> [--cert FILE --key FILE] [--path /rmtt]
                                  [--keepalive-min N] [--keepalive-max N]

Prints ``SERVER_READY <scheme> <port>`` once bound, ``PUSH device=<id> len=<n>``
for every upstream frame and echoes ``echo:<payload>`` back to the sender.
"""

from __future__ import annotations

import argparse
import os
import signal
import ssl
import sys
import time

from rmtt import KeepalivePolicy, ServerOptions
from rmtt.server import AuthResult
from rmtt.server.server import Server
from rmtt.transport import listeners as listeners_mod


def _listener(args):
    if args.scheme == "tcp":
        return listeners_mod.StreamListener("0.0.0.0", args.port)
    if args.scheme == "tls":
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(args.cert, args.key)
        return listeners_mod.StreamListener("0.0.0.0", args.port, ssl_context=ctx)
    if args.scheme == "ws":
        return listeners_mod.WSListener("0.0.0.0", args.port, path=args.path)
    if args.scheme == "wss":
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(args.cert, args.key)
        return listeners_mod.WSListener("0.0.0.0", args.port, path=args.path, ssl_context=ctx)
    if args.scheme == "kcp":
        return listeners_mod.KCPListener("0.0.0.0", args.port)
    if args.scheme == "quic":
        return listeners_mod.QUICListener(
            "0.0.0.0", args.port, certfile=args.cert, keyfile=args.key
        )
    raise SystemExit(f"unsupported scheme: {args.scheme}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("scheme")
    parser.add_argument("port", type=int)
    parser.add_argument("--cert", default=os.environ.get("RMTT_CERT", "/tmp/rmtt-test-cert.pem"))
    parser.add_argument("--key", default=os.environ.get("RMTT_KEY", "/tmp/rmtt-test-key.pem"))
    parser.add_argument("--path", default="/rmtt")
    parser.add_argument("--keepalive-min", type=int, default=30)
    parser.add_argument("--keepalive-max", type=int, default=600)
    parser.add_argument("--reject", default="")
    args = parser.parse_args()

    policy = KeepalivePolicy(
        min_seconds=args.keepalive_min, max_seconds=args.keepalive_max, default_seconds=60
    )

    def authenticate(credential: str) -> AuthResult:
        if args.reject and credential == args.reject:
            return AuthResult.reject()
        return AuthResult.allow(credential)

    options = ServerOptions(port=args.port, keepalive_policy=policy)
    options.listeners = [_listener(args)]
    options.set_authenticator(authenticate)
    options.set_message_handler(
        lambda device_id, payload: _on_push(device_id, payload, server)
    )
    server = Server(options)
    server.start()

    print(f"SERVER_READY {args.scheme} {server.port}", flush=True)

    def _stop(*_args):
        server.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    while True:
        time.sleep(0.2)


def _on_push(device_id: str, payload: bytes, server: Server) -> None:
    print(f"PUSH device={device_id} len={len(payload)}", flush=True)
    try:
        server.push(device_id, b"echo:" + payload)
    except Exception as exc:  # noqa: BLE001
        print(f"ECHO_ERROR {exc}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
