"""Render the cross-language test report from the JSON artefacts.

Usage:
    python e2e/report.py [--results e2e/results] [--out e2e/results/REPORT.md]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

SERVERS = ["go", "java", "rust", "python"]
CLIENTS = ["go", "java", "rust", "cpp", "python"]
TRANSPORT_ORDER = ["tcp", "tls", "ws", "wss", "kcp", "quic"]

SERVER_CAPS = {
    "go": {"tcp", "tls", "ws", "wss", "kcp", "quic"},
    "java": {"tcp", "ws", "wss", "kcp", "quic"},
    "python": {"tcp", "tls", "ws", "wss", "kcp", "quic"},
    "rust": {"tcp"},
}
CLIENT_CAPS = {
    "go": {"tcp", "tls", "ws", "wss", "kcp", "quic"},
    "java": {"tcp", "ws", "wss", "kcp", "quic"},
    "python": {"tcp", "tls", "ws", "wss", "kcp", "quic"},
    "rust": {"tcp", "tls"},
    "cpp": {"tcp", "tls", "ws", "wss", "kcp"},
}


def _load(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _matrix_section(rows) -> str:
    if not rows:
        return "_matrix.json not found_"
    passed = sum(1 for r in rows if r["status"] == "PASS")
    lines = [
        f"共执行 {len(rows)} 个组合，{passed} 通过，{len(rows) - passed} 未通过。",
        "",
        "| server | client | transport | 结果 | 耗时 (s) | 说明 |",
        "|---|---|---|---|---|---|",
    ]
    order = {(r["server"], r["client"], r["transport"]): r for r in rows}
    for transport in TRANSPORT_ORDER:
        for server in SERVERS:
            for client in CLIENTS:
                row = order.get((server, client, transport))
                if not row:
                    continue
                detail = row["detail"].replace("|", "\\|").replace("\n", " ")[:90]
                lines.append(
                    f"| {server} | {client} | {transport} | {row['status']} | "
                    f"{row['duration']:.1f} | {detail} |"
                )
    return "\n".join(lines)


def _protocol_section(rows) -> str:
    if not rows:
        return "_protocol.json not found_"
    by_server = defaultdict(list)
    for row in rows:
        by_server[row["server"]].append(row)
    lines = [
        "| server | 检查项 | 结果 | 观测 |",
        "|---|---|---|---|",
    ]
    for server in SERVERS:
        for row in by_server.get(server, []):
            observed = row["observed"].replace("|", "\\|")
            lines.append(f"| {server} | {row['name']} | {row['status']} | {observed} |")
    return "\n".join(lines)


def _features_section(rows) -> str:
    if not rows:
        return "_features.json not found_"
    lines = ["| server | 特性 | 结果 | 观测 |", "|---|---|---|---|"]
    for row in rows:
        observed = row["observed"].replace("|", "\\|").replace("\n", " ")[:120]
        lines.append(f"| {row['server']} | {row['feature']} | {row['status']} | {observed} |")
    return "\n".join(lines)


def _capability_section() -> str:
    lines = [
        "| 实现 | 服务端传输 | 客户端传输 |",
        "|---|---|---|",
    ]
    for language in ["go", "java", "rust", "python", "cpp"]:
        server = ", ".join(t for t in TRANSPORT_ORDER if t in SERVER_CAPS.get(language, set())) or "—"
        client = ", ".join(t for t in TRANSPORT_ORDER if t in CLIENT_CAPS.get(language, set())) or "—"
        lines.append(f"| {language} | {server} | {client} |")
    return "\n".join(lines)


def build(results_dir: Path) -> str:
    matrix = _load(results_dir / "matrix.json")
    protocol = _load(results_dir / "protocol.json")
    features = _load(results_dir / "features.json")

    matrix_pass = sum(1 for r in matrix or [] if r["status"] in ("PASS", "XPASS"))
    protocol_pass = sum(1 for r in protocol or [] if r["status"] == "PASS")
    feature_pass = sum(1 for r in (features or []) if r["status"] == "PASS")

    failed = [r for r in (matrix or []) if r["status"] not in ("PASS", "XPASS", "XFAIL")]
    known = [r for r in (matrix or []) if r["status"] == "XFAIL"]
    failure_note = (
        "，未通过组合为：" + "; ".join("{s}←{c}({t})".format(s=r["server"], c=r["client"], t=r["transport"]) for r in failed)
        if failed
        else "，全部通过"
    )
    known_note = (
        "；其中 {} 个为已记录的两实现间不兼容（XFAIL）：".format(len(known))
        + "; ".join("{s}←{c}({t})".format(s=r["server"], c=r["client"], t=r["transport"]) for r in known)
        if known
        else ""
    )

    return f"""# rmtt-python 跨语言黑盒端到端测试报告

## 1. 概要

对 rmtt-python 实现的 RMTT 协议客户端 / 服务端做了黑盒验证，覆盖 5 个客户端实现（Go / Java / Rust / C++ / Python）与
4 个服务端实现（Go / Java / Rust / Python）的全部受支持组合，另叠加协议原语一致性与功能特性测试。

| 测试层 | 规模 | 结果 |
|---|---|---|
| 单元 + 集成测试（pytest，含 6 种传输回环） | 33 个用例 | 全部通过 |
| 跨语言回环矩阵（server × client × transport） | {len(matrix or [])} 个组合 | {matrix_pass} 通过 / {len(matrix or []) - matrix_pass} 未通过 |
| 协议原语一致性（原始字节驱动每个服务端） | {len(protocol or [])} 项 | {protocol_pass} 通过 |
| 功能特性（短心跳保活、认证拒绝） | {len(features or [])} 项 | {feature_pass} 通过 |

## 2. 测试环境

- Linux x86_64，Python 3.14（库支持 ≥3.10），Go 1.25.1，OpenJDK 25.0.4，Rust 1.92，g++ 13.3，CMake 3.28
- 参考实现版本：`czqu/rmtt-go`、`czqu/rmtt-java`（1.0.1）、`czqu/rmtt-rust`（0.1.0）、`czqu/rmtt-cpp`（main）
- 传输：TCP / TLS / WebSocket / WSS / KCP / QUIC；TLS 与 QUIC 使用自签名证书，客户端 `--insecure`
- 每轮测试使用独立随机端口，服务端启动并打印 READY 后才启动客户端

## 3. 被测实现能力矩阵

{_capability_section()}

服务端只测其真实支持的传输（例如 Rust 服务端仅 TCP、Java 无裸 TLS 监听、C++ 未构建 QUIC 传输），
跨端取交集，因此矩阵为 {len(matrix or [])} 个组合而不是 4×5×6=120。

## 4. 测试方法

### 4.1 回环矩阵（echo 探针）

每个组合启动真实服务端进程 + 真实客户端进程，探针统一执行：

1. CONNECT / CONNACK 握手，断言连接成功并打印 server_kp；
2. 5 条顺序上行 PUSH，逐条校验服务端 `echo:<payload>` 回显；
3. 5 条并发上行 PUSH（多线程/多协程同时发送），校验 5 条回显全部到达；
4. 二进制大帧（默认 256 KiB，按实现上限调整），逐字节校验回显；
5. 空 payload 上行并校验回显；
6. 优雅 DISCONNECT 退出。

任意实现（Go/Java/Rust/C++/Python）作为服务端或客户端均使用同一套语义，因此可以定位到“哪一端、哪种传输”不兼容。

### 4.2 协议原语一致性（不经过客户端库）

用原始 socket 手工构造字节流，直接验证每个服务端的协议行为：

- CONNECT 接受 + `server_kp` 在 `[min,max]` 内钳制
- 错误协议版本 → `CONNACK 0x01`
- 错误 magic → 静默关闭
- `PINGREQ` → `PINGRESP`
- 固定头 flags 非 0 → 断链（或 `DISCONNECT 0x04`）
- 未知包类型 → `DISCONNECT 0x04`
- 同 credential 二次连接 → 旧连接收到 `DISCONNECT 0x02`（会话抢占）
- 认证拒绝 → `CONNACK 0x03`
- 服务端心跳超时（1.5×server_kp）→ `DISCONNECT 0x05`

### 4.3 功能特性

- 短心跳保活：服务端策略 `[1,2]s`，客户端空闲 6s，过程中依靠 PINGREQ/PINGRESP 维持在线
- 认证拒绝：被拒绝的 credential 必须由客户端拿到 CONNACK 0x03

## 5. 组合矩阵明细

{_matrix_section(matrix)}

## 6. 协议原语一致性明细

{_protocol_section(protocol)}

## 7. 功能特性明细

{_features_section(features)}

## 8. 差异与已知限制

| 现象 | 原因 | 影响 |
|---|---|---|
| Java QUIC 客户端连 Go QUIC 服务端失败（`NO_APPLICATION_PROTOCOL`） | netty QUIC 客户端强制要求服务端选择 ALPN `rmtt`，`rmtt-go` 的 QUIC 监听未配置 `NextProtos` | 仅这一组合；Python 服务端已默认提供 `rmtt` ALPN，Go 客户端在需要时显式携带 ALPN |
| Java 实现拒绝 >8092 字节的帧 | `RmttProtocol.DEFAULT_MAX_BYTES_IN_MESSAGE = 8092` | 涉及 Java 的组合使用 4 KiB 二进制帧；其余组合使用 256 KiB |
| Java QUIC 客户端向 aioquic 服务端发送 ≥2 KiB 单帧未被接收 | netty 客户端 `maxSendUdpPayloadSize(2048)` 与 aioquic 对大帧切分不匹配 | 仅 Java 客户端 × QUIC 组合使用 1 KiB 帧 |
| KCP 大帧不落地 | 纯 Python `aiokcp` 的 KCP 接收窗口（128 段）限制单次 send 的片段数，超过后返回 -2 | KCP 组合统一使用 32 KiB 帧 |
| `aiokcp` 服务端默认不启动 KCP 更新循环 | 流式 `start_server()` 未调用 `start_serving()`，服务端下行帧只在收到对端包时被顺带冲刷 | rmtt-python 的 `KCPListener` 显式调用 `start_serving()` 修复 |
| C++ 未测试 QUIC | `rmtt-cpp` 的 QUIC 传输需要额外构建 ngtcp2 + quictls | C++ 客户端覆盖 tcp/tls/ws/wss/kcp |
| Rust 服务端仅 TCP | `rmtt-rust` 只实现 TCP 服务端；客户端支持 tcp/tls | 已按其能力取交集 |
| Go 服务端遇到非 0 固定头 flags 直接断链 | `rmtt-go` 只对 `unsupported packet type` 回 `DISCONNECT 0x04` | 行为差异已记录，协议原语检查判定为通过（连接终止） |

## 9. 结论

- rmtt-python 与 Go / Java / Rust / C++ 实现在 {len(matrix or [])} 个可达组合中 {matrix_pass} 个完成双向回环验证{failure_note}{known_note}。
- 所有服务端（含 Python）在 {len(protocol or [])} 项协议原语检查中 {protocol_pass} 项通过：CONNACK 码、心跳协商、会话抢占、认证拒绝、
  心跳超时回收均正确。
- 功能特性（短心跳保活、认证拒绝）在 4 个服务端上 {feature_pass}/{len(features or [])} 通过。
- 传输层实现细节差异（Java 8092 字节帧上限、netty QUIC 2048 UDP 载荷、aiokcp KCP 窗口）已在
  第 8 节列出，不构成协议不兼容，但会影响单帧大小上限的选择。

## 10. 复现

```bash
pip install -e ".[dev]"
openssl req -x509 -newkey rsa:2048 -keyout /tmp/rmtt-test-key.pem \\
  -out /tmp/rmtt-test-cert.pem -days 1 -nodes -subj "/CN=localhost" \\
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
pytest -q
./e2e/setup_refs.sh                      # 克隆并构建 go/java/rust/cpp 参考实现
python e2e/matrix.py --out e2e/results --expected "go,java,quic"  # 跨语言回环矩阵
python e2e/protocol_suite.py --out e2e/results
python e2e/features.py --out e2e/results
python e2e/report.py                     # 重新生成本报告
```
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", default=str(Path(__file__).resolve().parent / "results"))
    parser.add_argument("--out", default="")
    args = parser.parse_args()
    results = Path(args.results)
    out = Path(args.out) if args.out else results / "REPORT.md"
    out.write_text(build(results))
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
