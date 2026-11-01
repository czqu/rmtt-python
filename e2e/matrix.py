"""Cross-language E2E matrix: every server x every client x every supported transport.

Each run starts a real server implementation (Go / Java / Rust / Python), waits
for its READY marker, runs a real client (Go / Java / Rust / C++ / Python) and
records whether the full round-trip probe passed.

Usage:
    python e2e/matrix.py [--only SERVER,CLIENT,TRANSPORT] [--jobs 1]
                         [--out e2e/results]
"""

from __future__ import annotations

import argparse
import json
import queue
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CERT = "/tmp/rmtt-test-cert.pem"
KEY = "/tmp/rmtt-test-key.pem"

PYTHON = sys.executable

GO_SERVER = "/tmp/bin/go-harness-server"
GO_CLIENT = "/tmp/bin/go-harness-client"
JAVA_CLASSES = "/tmp/ref/rmtt-java/rmtt-it/target/classes"
JAVA_CP_FILE = "/tmp/java-cp.txt"
RUST_SERVER = "/tmp/ref/rmtt-rust/e2e/rust-server/target/release/rmtt-rust-e2e-server"
RUST_CLIENT = "/tmp/ref/rmtt-rust/e2e/rust-client/target/release/rmtt-rust-e2e"
CPP_CLIENT = "/tmp/ref/rmtt-cpp/example/test_client"


def java_classpath() -> str:
    """Classpath for the Java harnesses, resolved lazily.

    Only jobs that actually exercise Java need ``./e2e/setup_refs.sh`` to have
    run; importing this module (e.g. from the python-only matrix or the raw
    protocol suite) must not depend on the reference build.
    """
    extra = ""
    cp_file = Path(JAVA_CP_FILE)
    if cp_file.exists():
        extra = cp_file.read_text().strip()
    return f"{JAVA_CLASSES}:{extra}" if extra else JAVA_CLASSES


def java_server_cmd() -> list[str]:
    return ["java", "-cp", java_classpath(), "net.czqu.rmtt.it.HarnessServerMain"]


def java_client_cmd() -> list[str]:
    return ["java", "-cp", java_classpath(), "net.czqu.rmtt.it.HarnessClientMain"]


@dataclass
class Result:
    server: str
    client: str
    transport: str
    status: str
    detail: str = ""
    duration: float = 0.0


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ServerProcess:
    def __init__(self, language: str, transport: str, port: int, extra: list[str] | None = None):
        self.language = language
        self.transport = transport
        self.port = port
        self.lines: list[str] = []
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._proc: subprocess.Popen | None = None
        self._extra = extra or []

    def _cmd(self) -> list[str]:
        port = str(self.port)
        if self.language == "go":
            return [GO_SERVER, self.transport, port, CERT, KEY, *self._extra]
        if self.language == "java":
            return [*java_server_cmd(), self.transport, port, CERT, KEY, *self._extra]
        if self.language == "python":
            return [PYTHON, "-m", "e2e.harness.server", self.transport, port, *self._extra]
        if self.language == "rust":
            return [RUST_SERVER, port, "30", "600"]
        raise ValueError(self.language)

    def start(self) -> None:
        self._proc = subprocess.Popen(
            self._cmd(),
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )

        def _reader() -> None:
            assert self._proc and self._proc.stdout
            for line in self._proc.stdout:
                self.lines.append(line.rstrip("\n"))
                self._queue.put(line)
            self._queue.put(None)

        threading.Thread(target=_reader, daemon=True).start()

    def wait_ready(self, timeout: float = 40.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._proc is not None and self._proc.poll() is not None:
                return False
            try:
                line = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if line and ("READY" in line):
                return True
        return False

    def stop(self) -> None:
        if self._proc is None:
            return
        try:
            self._proc.terminate()
            self._proc.wait(timeout=8)
        except Exception:  # noqa: BLE001
            try:
                self._proc.kill()
            except Exception:  # noqa: BLE001
                pass

    @property
    def log(self) -> str:
        return "\n".join(self.lines[-40:])


def _client_cmd(
    language: str, transport: str, port: int, credential: str, binary_size: int,
    server_language: str = "",
) -> tuple[list[str], str]:
    url = f"{transport}://127.0.0.1:{port}"
    if transport in ("ws", "wss"):
        url += "/rmtt"
    if language == "go":
        cmd = [GO_CLIENT, url, credential, str(binary_size)]
        if transport == "quic" and server_language in ("java", "python"):
            cmd += ["--alpn", "rmtt"]
        return cmd, "CLIENT_E2E_PASS"
    if language == "java":
        return [*java_client_cmd(), url, credential, str(binary_size)], "CLIENT_E2E_PASS"
    if language == "python":
        cmd = [
            PYTHON,
            "-m",
            "e2e.harness.client",
            url,
            "--credential",
            credential,
            "--binary-size",
            str(binary_size),
        ]
        if transport == "quic" and server_language in ("java", "python"):
            cmd += ["--quic-alpn", "rmtt"]
        return cmd, "CLIENT_E2E_PASS"
    if language == "rust":
        return [RUST_CLIENT, url, credential, str(binary_size)], "CLIENT_E2E_PASS"
    if language == "cpp":
        return [CPP_CLIENT, url, credential, "fixed", "0", "warn", "--insecure"], "disconnected, OK"
    raise ValueError(language)


# Which transports each implementation can serve / dial, verified against the
# reference sources (see the test report for the capability table).
SERVERS = {
    "go": ["tcp", "tls", "ws", "wss", "kcp", "quic"],
    "java": ["tcp", "ws", "wss", "kcp", "quic"],
    "python": ["tcp", "tls", "ws", "wss", "kcp", "quic"],
    "rust": ["tcp"],
}
CLIENTS = {
    "go": ["tcp", "tls", "ws", "wss", "kcp", "quic"],
    "java": ["tcp", "ws", "wss", "kcp", "quic"],
    "python": ["tcp", "tls", "ws", "wss", "kcp", "quic"],
    "rust": ["tcp", "tls"],
    "cpp": ["tcp", "tls", "ws", "wss", "kcp"],
}


def combos(only: list[str] | None) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []
    for server, transports in SERVERS.items():
        for client, client_transports in CLIENTS.items():
            for transport in sorted(set(transports) & set(client_transports)):
                if only and f"{server},{client},{transport}" not in only:
                    continue
                out.append((server, client, transport))
    return out


def run_one(server_lang: str, client_lang: str, transport: str, out_dir: Path) -> Result:
    port = _free_port()
    started = time.monotonic()
    server = ServerProcess(server_lang, transport, port)
    stem = f"{server_lang}-server__{client_lang}-client__{transport}"
    binary_size = 4096 if (server_lang == "java" or client_lang == "java") else 262144
    if transport == "quic" and client_lang == "java":
        # netty's QUIC client caps UDP payloads at 2048 bytes; larger single
        # frames are not delivered to an aioquic peer.
        binary_size = 1024
    elif transport == "kcp" and (server_lang == "java" or client_lang == "java"):
        binary_size = 4096
    elif transport == "kcp":
        # aiokcp's pure-Python KCP window handles moderate frames reliably;
        # very large single writes exceed the send window in the reference
        # kcp-java/kcp-go stacks as well.
        binary_size = 32768
    credential = f"{client_lang}-{transport}"
    try:
        server.start()
        if not server.wait_ready():
            return Result(
                server_lang, client_lang, transport, "FAIL",
                f"server did not become ready: {server.log[-600:]}", time.monotonic() - started,
            )
        cmd, marker = _client_cmd(
            client_lang, transport, port, credential, binary_size, server_lang
        )
        completed = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=180,
        )
        text = completed.stdout + completed.stderr
        (out_dir / "logs").mkdir(parents=True, exist_ok=True)
        (out_dir / "logs" / f"{stem}.client.log").write_text(text)
        (out_dir / "logs" / f"{stem}.server.log").write_text(server.log)
        ok = completed.returncode == 0 and marker in text
        detail = ""
        if not ok:
            tail = "\n".join(text.strip().splitlines()[-8:])
            detail = f"rc={completed.returncode} marker={marker!r} tail={tail}"
        return Result(
            server_lang, client_lang, transport, "PASS" if ok else "FAIL",
            detail, time.monotonic() - started,
        )
    except subprocess.TimeoutExpired:
        return Result(
            server_lang, client_lang, transport, "TIMEOUT", "client probe exceeded 180s",
            time.monotonic() - started,
        )
    except Exception as exc:  # noqa: BLE001
        return Result(
            server_lang, client_lang, transport, "ERROR", repr(exc), time.monotonic() - started
        )
    finally:
        server.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", default="")
    parser.add_argument("--expected", default="", help="semicolon separated known-failing combos (server,client,transport)")
    parser.add_argument("--out", default=str(ROOT / "e2e" / "results"))
    parser.add_argument("--markdown", default="")
    args = parser.parse_args()

    only = [item for item in args.only.split(";") if item]
    expected = {item for item in args.expected.split(";") if item}
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    results: list[Result] = []
    for server, client, transport in combos(only):
        result = run_one(server, client, transport, out_dir)
        key = f"{server},{client},{transport}"
        if result.status != "PASS" and key in expected:
            result.status = "XFAIL"
        elif result.status == "PASS" and key in expected:
            result.status = "XPASS"
        results.append(result)
        print(
            f"{result.status:7} {result.server:6} server <- {result.client:6} client  {result.transport:5} "
            f"{result.duration:5.1f}s {result.detail[:120]}",
            flush=True,
        )

    (out_dir / "matrix.json").write_text(
        json.dumps([result.__dict__ for result in results], indent=2)
    )
    passed = sum(1 for result in results if result.status in ("PASS", "XPASS"))
    print(f"\n{passed}/{len(results)} combinations passed")

    if args.markdown:
        Path(args.markdown).write_text(_to_markdown(results))
    unexpected = [
        result
        for result in results
        if result.status not in ("PASS", "XFAIL", "XPASS")
    ]
    return 0 if not unexpected else 1


def _to_markdown(results: list[Result]) -> str:
    lines = ["| server | client | transport | result | detail |", "|---|---|---|---|---|"]
    for result in results:
        detail = result.detail.replace("|", "\\|").replace("\n", " ")[:160]
        lines.append(
            f"| {result.server} | {result.client} | {result.transport} | "
            f"{result.status} | {detail} |"
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    sys.exit(main())
