"""Cross-language feature checks that go beyond the echo round trip.

For every server implementation this drives a real Python client through:

* short-keepalive stay-alive (client PINGREQ / server PINGRESP + enforcement)
* authentication rejection (CONNACK 0x03 surfaced by the client)

Usage:
    python e2e/features.py [--out e2e/results]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from matrix import PYTHON, ROOT, _free_port
from protocol_suite import _server


@dataclass
class FeatureResult:
    server: str
    feature: str
    status: str
    observed: str


def _run_client(port: int, credential: str, extra: list[str], timeout: float = 60) -> tuple[int, str]:
    cmd = [
        PYTHON,
        "-m",
        "e2e.harness.client",
        f"tcp://127.0.0.1:{port}",
        "--credential",
        credential,
        *extra,
    ]
    completed = subprocess.run(
        cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=timeout
    )
    return completed.returncode, completed.stdout + completed.stderr


def check_keepalive(language: str) -> FeatureResult:
    port = _free_port()
    server = _server(language, port, ["--kp-min", "1", "--kp-max", "2"])
    try:
        server.start()
        if not server.wait_ready():
            return FeatureResult(language, "short-keepalive stay-alive", "FAIL", "server not ready")
        rc, text = _run_client(port, f"ka-{language}", ["--stay", "6", "--binary-size", "1024"], timeout=90)
        ok = rc == 0 and "CLIENT_STAY_OK" in text and "CLIENT_E2E_PASS" in text
        kp = next((line for line in text.splitlines() if "server_kp=" in line), "")
        return FeatureResult(
            language, "short-keepalive stay-alive", "PASS" if ok else "FAIL",
            kp if ok else "\n".join(text.strip().splitlines()[-4:]),
        )
    finally:
        server.stop()


def check_auth_reject(language: str) -> FeatureResult:
    port = _free_port()
    server = _server(language, port, ["--reject", "reject-me"])
    try:
        server.start()
        if not server.wait_ready():
            return FeatureResult(language, "auth reject", "FAIL", "server not ready")
        rc, text = _run_client(port, "reject-me", ["--expect-refused"], timeout=60)
        ok = rc == 0 and "CLIENT_REFUSED code=3" in text
        observed = next((line for line in text.splitlines() if "CLIENT_REFUSED" in line), "")
        return FeatureResult(
            language, "auth reject", "PASS" if ok else "FAIL",
            observed or "\n".join(text.strip().splitlines()[-4:]),
        )
    finally:
        server.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(Path(__file__).resolve().parent / "results"))
    parser.add_argument("--servers", default="go,java,python,rust")
    args = parser.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    results: list[FeatureResult] = []
    for language in args.servers.split(","):
        for check in (check_keepalive, check_auth_reject):
            result = check(language)
            results.append(result)
            print(f"{result.status:4} {result.server:6} {result.feature:30} {result.observed[:100]}", flush=True)

    (out_dir / "features.json").write_text(json.dumps([r.__dict__ for r in results], indent=2))
    failures = [r for r in results if r.status != "PASS"]
    print(f"\n{len(results) - len(failures)}/{len(results)} feature checks passed")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
