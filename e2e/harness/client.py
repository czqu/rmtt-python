"""E2E client probe used by the cross-language interop matrix.

Usage:
    python -m e2e.harness.client <url> [--credential ID] [--count N] [--stay S]
                                 [--insecure] [--expect-refused]

Prints machine-checkable markers:

    CLIENT_CONNECTED server_kp=<n>
    CLIENT_SEQ_OK n=<n>
    CLIENT_CONCURRENT_OK n=<n>
    CLIENT_BINARY_OK bytes=<n>
    CLIENT_EMPTY_OK
    CLIENT_STAY_OK seconds=<n>
    CLIENT_DISCONNECTED reason=<n>
    CLIENT_REFUSED code=<n>
    CLIENT_E2E_PASS
"""

from __future__ import annotations

import argparse
import ssl
import sys
import threading
import time

from rmtt import Client, ClientOptions


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("--credential", default="py-e2e")
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--binary-size", type=int, default=262144)
    parser.add_argument("--stay", type=float, default=0)
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--expect-refused", action="store_true")
    parser.add_argument("--quic-alpn", default="")
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()

    options = ClientOptions(connect_retry=False, auto_reconnect=False, heartbeat=10)
    options.add_server(args.url)
    options.set_credential(args.credential)
    if args.url.startswith("tls://") or args.url.startswith("wss://"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        options.set_tls_context(ctx)
    if args.url.startswith("quic://"):
        options.quic_insecure = True
        if args.quic_alpn:
            options.quic_alpn = [args.quic_alpn]

    received: list[bytes] = []
    lock = threading.Lock()

    def on_message(message) -> None:
        with lock:
            received.append(message.payload)

    client = Client(options)
    client.add_payload_handler_last(on_message)
    token = client.connect()
    if not token.wait(args.timeout):
        print("CLIENT_FAIL connect timeout", file=sys.stderr)
        return 1
    if token.error is not None:
        if args.expect_refused:
            code = getattr(token.error, "return_code", -1)
            print(f"CLIENT_REFUSED code={code}")
            client.shutdown()
            print("CLIENT_E2E_PASS")
            return 0
        print(f"CLIENT_FAIL connect error: {token.error}", file=sys.stderr)
        return 1
    if args.expect_refused:
        print("CLIENT_FAIL expected refusal but connect succeeded", file=sys.stderr)
        client.disconnect()
        client.shutdown()
        return 1

    print(f"CLIENT_CONNECTED server_kp={client.server_keepalive}", flush=True)

    def wait_for(predicate, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.02)
        return False

    # Sequential round trips.
    for i in range(args.count):
        message = f"ping-{i}"
        push = client.push(message)
        if not push.wait(args.timeout) or push.error is not None:
            print(f"CLIENT_FAIL push {i}: {push.error}", file=sys.stderr)
            return 1
        if not wait_for(lambda: (f"echo:{message}".encode() in received), args.timeout):
            print(f"CLIENT_FAIL missing echo for {message}", file=sys.stderr)
            return 1
    print(f"CLIENT_SEQ_OK n={args.count}", flush=True)

    # Concurrent round trips.
    received.clear()
    errors: list[BaseException] = []

    def _push(i: int) -> None:
        try:
            token = client.push(f"conc-{i}")
            token.wait(args.timeout)
            if token.error is not None:
                errors.append(token.error)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=_push, args=(i,)) for i in range(args.count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        print(f"CLIENT_FAIL concurrent push: {errors[0]}", file=sys.stderr)
        return 1
    expected = {f"echo:conc-{i}".encode() for i in range(args.count)}
    if not wait_for(lambda: expected.issubset(set(received)), args.timeout):
        print("CLIENT_FAIL missing concurrent echoes", file=sys.stderr)
        return 1
    print(f"CLIENT_CONCURRENT_OK n={args.count}", flush=True)

    # Binary payload; 256 KiB exercises multi-byte remaining lengths, while
    # servers with a small frame cap (e.g. Java's 8092 bytes) need a smaller one.
    received.clear()
    size = args.binary_size
    blob = bytes(i % 256 for i in range(size))
    push = client.push(blob)
    if not push.wait(args.timeout) or push.error is not None:
        print(f"CLIENT_FAIL binary push: {push.error}", file=sys.stderr)
        return 1
    if not wait_for(lambda: (b"echo:" + blob in received), args.timeout * 2):
        print("CLIENT_FAIL binary echo mismatch", file=sys.stderr)
        return 1
    print(f"CLIENT_BINARY_OK bytes={len(blob)}", flush=True)

    # Empty payload.
    received.clear()
    push = client.push(b"")
    push.wait(args.timeout)
    if not wait_for(lambda: b"echo:" in received, args.timeout):
        print("CLIENT_FAIL empty echo", file=sys.stderr)
        return 1
    print("CLIENT_EMPTY_OK", flush=True)

    if args.stay > 0:
        time.sleep(args.stay)
        if not client.is_connected():
            print("CLIENT_FAIL lost connection while idle", file=sys.stderr)
            return 1
        print(f"CLIENT_STAY_OK seconds={args.stay}", flush=True)

    client.disconnect()
    client.shutdown()
    print("CLIENT_E2E_PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
