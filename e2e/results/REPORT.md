# rmtt-python 跨语言黑盒端到端测试报告

## 1. 概要

对 rmtt-python 实现的 RMTT 协议客户端 / 服务端进行黑盒验证，覆盖 **5 个客户端实现（Go / Java / Rust / C++ / Python）** 与
**4 个服务端实现（Go / Java / Rust / Python）** 的全部受支持组合，并叠加协议原语一致性与功能特性测试。

| 测试层 | 规模 | 结果 |
|---|---|---|
| 单元 + 集成测试（pytest，含 6 种传输回环） | 33 个用例 | 全部通过 |
| 跨语言回环矩阵（server × client × transport） | 73 个组合 | 72 通过 / 1 未通过 |
| 协议原语一致性（原始字节驱动每个服务端） | 36 项 | 36 通过 |
| 功能特性（短心跳保活、认证拒绝） | 8 项 | 8 通过 |

## 2. 测试环境

- Linux x86_64，Python 3.14（库支持 ≥3.10），Go 1.25.1，OpenJDK 25.0.4，Rust 1.92，g++ 13.3，CMake 3.28
- 参考实现版本：`czqu/rmtt-go`、`czqu/rmtt-java`（1.0.1）、`czqu/rmtt-rust`（0.1.0）、`czqu/rmtt-cpp`（main）
- 传输：TCP / TLS / WebSocket / WSS / KCP / QUIC；TLS 与 QUIC 使用自签名证书，客户端 `--insecure`
- 每轮测试使用独立随机端口，服务端启动并打印 READY 后才启动客户端

## 3. 被测实现能力矩阵

| 实现 | 服务端传输 | 客户端传输 |
|---|---|---|
| go | tcp, tls, ws, wss, kcp, quic | tcp, tls, ws, wss, kcp, quic |
| java | tcp, ws, wss, kcp, quic | tcp, ws, wss, kcp, quic |
| rust | tcp | tcp, tls |
| python | tcp, tls, ws, wss, kcp, quic | tcp, tls, ws, wss, kcp, quic |
| cpp | — | tcp, tls, ws, wss, kcp |

服务端只测其真实支持的传输（例如 Rust 服务端仅 TCP、Java 无裸 TLS 监听、C++ 未构建 QUIC 传输），
跨端取交集，因此矩阵为 73 个组合而不是 4×5×6=120。

## 4. 测试方法

### 4.1 回环矩阵（echo 探针）

每个组合启动真实服务端进程 + 真实客户端进程，探针统一执行：

1. CONNECT / CONNACK 握手，断言连接成功并打印 server_kp；
2. **5 条顺序上行 PUSH**，逐条校验服务端 `echo:<payload>` 回显；
3. **5 条并发上行 PUSH**（多线程/多协程同时发送），校验 5 条回显全部到达；
4. **二进制大帧**（默认 256 KiB，按实现上限调整），逐字节校验回显；
5. **空 payload** 上行并校验回显；
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

- **短心跳保活**：服务端策略 `[1,2]s`，客户端空闲 6s，过程中依靠 PINGREQ/PINGRESP 维持在线
- **认证拒绝**：被拒绝的 credential 必须由客户端拿到 CONNACK 0x03

## 5. 组合矩阵明细

共执行 **73** 个组合，**72** 通过，1 未通过。

| server | client | transport | 结果 | 耗时 (s) | 说明 |
|---|---|---|---|---|---|
| go | go | tcp | PASS | 0.1 |  |
| go | java | tcp | PASS | 0.6 |  |
| go | rust | tcp | PASS | 1.6 |  |
| go | cpp | tcp | PASS | 7.6 |  |
| go | python | tcp | PASS | 0.5 |  |
| java | go | tcp | PASS | 0.5 |  |
| java | java | tcp | PASS | 1.0 |  |
| java | rust | tcp | PASS | 2.1 |  |
| java | cpp | tcp | PASS | 8.0 |  |
| java | python | tcp | PASS | 1.0 |  |
| rust | go | tcp | PASS | 0.9 |  |
| rust | java | tcp | PASS | 1.4 |  |
| rust | rust | tcp | PASS | 2.4 |  |
| rust | cpp | tcp | PASS | 7.6 |  |
| rust | python | tcp | PASS | 1.3 |  |
| python | go | tcp | PASS | 0.3 |  |
| python | java | tcp | PASS | 0.8 |  |
| python | rust | tcp | PASS | 1.8 |  |
| python | cpp | tcp | PASS | 7.8 |  |
| python | python | tcp | PASS | 0.8 |  |
| go | go | tls | PASS | 0.1 |  |
| go | rust | tls | PASS | 1.6 |  |
| go | cpp | tls | PASS | 7.6 |  |
| go | python | tls | PASS | 0.6 |  |
| python | go | tls | PASS | 0.3 |  |
| python | rust | tls | PASS | 1.9 |  |
| python | cpp | tls | PASS | 7.9 |  |
| python | python | tls | PASS | 0.8 |  |
| go | go | ws | PASS | 0.1 |  |
| go | java | ws | PASS | 0.6 |  |
| go | cpp | ws | PASS | 7.6 |  |
| go | python | ws | PASS | 0.6 |  |
| java | go | ws | PASS | 0.6 |  |
| java | java | ws | PASS | 1.1 |  |
| java | cpp | ws | PASS | 8.1 |  |
| java | python | ws | PASS | 1.1 |  |
| python | go | ws | PASS | 0.3 |  |
| python | java | ws | PASS | 1.0 |  |
| python | cpp | ws | PASS | 7.9 |  |
| python | python | ws | PASS | 0.9 |  |
| go | go | wss | PASS | 0.1 |  |
| go | java | wss | PASS | 1.1 |  |
| go | cpp | wss | PASS | 7.7 |  |
| go | python | wss | PASS | 0.6 |  |
| java | go | wss | PASS | 1.2 |  |
| java | java | wss | PASS | 2.1 |  |
| java | cpp | wss | PASS | 8.7 |  |
| java | python | wss | PASS | 1.8 |  |
| python | go | wss | PASS | 0.3 |  |
| python | java | wss | PASS | 1.1 |  |
| python | cpp | wss | PASS | 7.9 |  |
| python | python | wss | PASS | 0.9 |  |
| go | go | kcp | PASS | 2.1 |  |
| go | java | kcp | PASS | 1.4 |  |
| go | cpp | kcp | PASS | 8.0 |  |
| go | python | kcp | PASS | 3.1 |  |
| java | go | kcp | PASS | 0.7 |  |
| java | java | kcp | PASS | 1.6 |  |
| java | cpp | kcp | PASS | 8.2 |  |
| java | python | kcp | PASS | 2.3 |  |
| python | go | kcp | PASS | 1.1 |  |
| python | java | kcp | PASS | 1.5 |  |
| python | cpp | kcp | PASS | 7.8 |  |
| python | python | kcp | PASS | 2.3 |  |
| go | go | quic | PASS | 0.4 |  |
| go | java | quic | FAIL | 4.0 | rc=1 marker='CLIENT_E2E_PASS' tail=	at io.netty.channel.SingleThreadIoEventLoop.run(Single |
| go | python | quic | PASS | 1.0 |  |
| java | go | quic | PASS | 1.1 |  |
| java | java | quic | PASS | 1.9 |  |
| java | python | quic | PASS | 1.8 |  |
| python | go | quic | PASS | 0.4 |  |
| python | java | quic | PASS | 1.0 |  |
| python | python | quic | PASS | 1.0 |  |

## 6. 协议原语一致性明细

| server | 检查项 | 结果 | 观测 |
|---|---|---|---|
| go | connect+keepalive clamp | PASS | rc=0 server_kp=45 |
| go | bad protocol version | PASS | type=2 rc=1 |
| go | bad magic | PASS | closed without reply |
| go | PINGREQ->PINGRESP | PASS | type=6 |
| go | framing violation | PASS | connection closed without DISCONNECT |
| go | unknown packet type | PASS | DISCONNECT reason=0x04 |
| go | session takeover | PASS | first connection disconnect=0x02 |
| go | auth reject | PASS | type=2 rc=3 |
| go | keepalive timeout | PASS | server_kp=1 disconnect=0x05 |
| java | connect+keepalive clamp | PASS | rc=0 server_kp=45 |
| java | bad protocol version | PASS | type=2 rc=1 |
| java | bad magic | PASS | closed without reply |
| java | PINGREQ->PINGRESP | PASS | type=6 |
| java | framing violation | PASS | DISCONNECT reason=0x04 |
| java | unknown packet type | PASS | DISCONNECT reason=0x04 |
| java | session takeover | PASS | first connection disconnect=0x02 |
| java | auth reject | PASS | type=2 rc=3 |
| java | keepalive timeout | PASS | server_kp=1 disconnect=0x05 |
| rust | connect+keepalive clamp | PASS | rc=0 server_kp=45 |
| rust | bad protocol version | PASS | type=2 rc=1 |
| rust | bad magic | PASS | closed without reply |
| rust | PINGREQ->PINGRESP | PASS | type=6 |
| rust | framing violation | PASS | DISCONNECT reason=0x04 |
| rust | unknown packet type | PASS | DISCONNECT reason=0x04 |
| rust | session takeover | PASS | first connection disconnect=0x02 |
| rust | auth reject | PASS | type=2 rc=3 |
| rust | keepalive timeout | PASS | server_kp=1 disconnect=0x05 |
| python | connect+keepalive clamp | PASS | rc=0 server_kp=45 |
| python | bad protocol version | PASS | type=2 rc=1 |
| python | bad magic | PASS | closed without reply |
| python | PINGREQ->PINGRESP | PASS | type=6 |
| python | framing violation | PASS | DISCONNECT reason=0x04 |
| python | unknown packet type | PASS | DISCONNECT reason=0x04 |
| python | session takeover | PASS | first connection disconnect=0x02 |
| python | auth reject | PASS | type=2 rc=3 |
| python | keepalive timeout | PASS | server_kp=1 disconnect=0x05 |

## 7. 功能特性明细

| server | 特性 | 结果 | 观测 |
|---|---|---|---|
| go | short-keepalive stay-alive | PASS | CLIENT_CONNECTED server_kp=2 |
| go | auth reject | PASS | CLIENT_REFUSED code=3 |
| java | short-keepalive stay-alive | PASS | CLIENT_CONNECTED server_kp=2 |
| java | auth reject | PASS | CLIENT_REFUSED code=3 |
| python | short-keepalive stay-alive | PASS | CLIENT_CONNECTED server_kp=2 |
| python | auth reject | PASS | CLIENT_REFUSED code=3 |
| rust | short-keepalive stay-alive | PASS | CLIENT_CONNECTED server_kp=2 |
| rust | auth reject | PASS | CLIENT_REFUSED code=3 |

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

- rmtt-python 与 Go / Java / Rust / C++ 实现在 **73 个可达组合中 72 个** 完成双向回环验证，未通过组合为：go←java(quic)。
- 所有服务端（含 Python）在 36 项协议原语检查中 36 项通过：CONNACK 码、心跳协商、会话抢占、认证拒绝、
  心跳超时回收均正确。
- 功能特性（短心跳保活、认证拒绝）在 4 个服务端上 8/8 通过。
- 传输层实现细节差异（Java 8092 字节帧上限、netty QUIC 2048 UDP 载荷、aiokcp KCP 窗口）已在
  第 8 节列出，不构成协议不兼容，但会影响单帧大小上限的选择。

## 10. 复现

```bash
pip install -e ".[dev]"
openssl req -x509 -newkey rsa:2048 -keyout /tmp/rmtt-test-key.pem \
  -out /tmp/rmtt-test-cert.pem -days 1 -nodes -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
pytest -q
./e2e/setup_refs.sh                      # 克隆并构建 go/java/rust/cpp 参考实现
python e2e/matrix.py --out e2e/results --expected "go,java,quic"  # 跨语言回环矩阵
python e2e/protocol_suite.py --out e2e/results
python e2e/features.py --out e2e/results
python e2e/report.py                     # 重新生成本报告
```
