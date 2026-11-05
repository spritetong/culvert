# Micro-Tunnel: 轻量级 HTTP/3 & WSS TCP 隧道技术方案与需求规格说明书

> [!WARNING]
> **Deprecated Design Document**: This document represents an early architectural draft and contains legacy specifications that have been superseded. For current, authoritative design, architecture, and configuration specifications, refer to [README.md](../README.md), [DESIGN.md](DESIGN.md), and [PRD.md](PRD.md).

---

## 1. 项目背景与设计哲学

### 1.1 背景与痛点反思
在穿透与代理工具的选型中，诸如 `wstunnel`、`Xray`、`Hysteria` 等工具往往追求全功能闭环（自行管理 TLS 证书、实现私有混淆/握手协议、内置应用层多路复用等）。这带来了严重的工程副作用：
* **运维脆弱**：工具直接监听公网端口，自管 ACME/TLS 证书，Docker 容器升级或进程异常往往导致整个外部接入点瘫痪，且极易因版本迭代带来的协议格式断代而导致客户端与服务端失联。
* **反代兼容性差**：`wstunnel` 采用 WebTransport（Extended CONNECT）与自建 Yamux 会话，主流反向代理（如 Caddy、Nginx）无法对其实施 L7 卸载与转译；若退回 L4 盲转发，则服务端必须重新背负证书管理与伪装包袱。
* **协议冗余与状态机失控**：在标准 TCP 基础上引入应用层多路复用（Yamux/Smux），一旦发生丢包仍无法避免内核 TCP 队头阻塞；同时其裸 TCP 连接池缺乏协议感知，在 NAT 丢包或超时后极易借出僵死连接。

### 1.2 核心设计原则（Philosophy & Non-Goals）
本系统（Micro-Tunnel）专为**「作为 Caddy 边缘反向代理的内部下游服务」**设计，遵循极简与正交解耦哲学：

1. **绝对依托正规 Web 掩体（GFW 友好）**：
   公网仅暴露由标准 Caddy 托管的 TCP 443 与 UDP 443，对外展现合规的 Web PKI 证书与标准 HTTP/3、WSS 流量，天然免疫主动探测与启发式审查。
2. **边缘终结，内网纯明文**：
   Caddy 负责终结公网 QUIC 与 TLS 握手；Caddy 到后端 Server 走 **Unix Domain Socket (UDS)**。Server 端零证书、零加解密、零网络特权端口。
3. **彻底摒弃应用层多路复用（No Yamux / No Smux）**：
   QUIC 模式下直接使用 HTTP/3 原生双向流（Stream ID 隔离）；WSS 模式采用独立的 1:1 连接映射，彻底消除私有分帧状态机。
4. **坚决不做 UDP 代理（No UDP / No DNS Tunneling）**：
   移除不可靠报文、NAT 映射表与垃圾回收定时器。DNS 查询统一在服务端远端解析（Remote DNS），杜绝伪需求导致的系统脆弱性。
5. **协议级连接池（Protocol-Aware Pool）**：
   建立具备协议感知与会话排空（Draining）机制的连接池，使物理连接可跨 TCP 隧道反复复用，实现真正的 0-RTT 极速接入。
6. **演进兼容性（P2P-Ready）**：
   前期以 Caddy 反代模式运行，但在会话层与传输层彻底解耦，在架构上完整预留双路主备 P2P NAT 打洞通道的接入能力。

---

## 2. 总体架构拓扑与链路流向

```
+-----------------------------------------------------------------------------------+
| 客户端环境 (Client Node)                                                          |
|   [本地应用程序] ──(Raw TCP)──► [Local TCP Listener]                              |
|                                       │                                           |
|                                       ▼                                           |
|                           [Session & Routing Layer]                               |
|                                       │                                           |
|                   ┌───────────────────┼───────────────────┐                       |
|                   ▼                   ▼                   ▼                       |
|           [Driver 1: QUIC/H3]  [Driver 2: WSS]     [Driver 3: P2P]                |
|           (HTTP/3 POST Stream) (1:1 WS Stream)   (双信道热备打洞)                  |
+-------------------│───────────────────│───────────────────│────────────────-------+
                    │ (UDP 443)         │ (TCP 443)         │ (直接穿透 UDP/TCP)
                    ▼                   ▼                   │
+-------------------------------------------------------+   │
| 公网边缘接入层: Caddy (Edge Gateway)                  |   │
|   * 监听 0.0.0.0:443 (TCP & UDP)                      |   │
|   * 自动维护 Let's Encrypt / ZeroSSL 证书             |   │
|   * 阻断扫描探测，对未授权请求返回默认静态站          |   │
|   * 终结 TLS 1.3 / QUIC，flush_interval -1 实时转发   |   │
+---------------------------│---------------------------+   │
                            │                               │
                            │ Unix Domain Socket            │
                            │ (/run/tunnel.sock)            │
                            │ (明文 HTTP/1.1 / h2c 全双工流) │
                            ▼                               │
+-------------------------------------------------------+   │
| 内部后端服务: Micro-Tunnel Server                     |   │
|   * 仅监听 UDS，无证书、无 TLS 开销                   |   │
|   * 校验 X-Tunnel-Auth 鉴权头                         |   │
|   * 解析目标地址，在内网发起 TCP 握手                 |   │
|   * 双向全双工字节流桥接                              |   │
+---------------------------│---------------------------+   │
                            │ (Raw TCP Stream)              │
                            ▼                               ▼
               [目标 TCP 节点 (SSH / Web / 内网服务)] ◄─────┘
```

---

## 3. 核心机制详解

### 3.1 HTTP/3 二进制全双工流机制
系统利用 HTTP/3 原生的 Client-Initiated Bidirectional Stream 传输裸字节流，行为与操作系统的 TCP Socket 完全一致：

```
Client                                                   Caddy / Server
  │                                                           │
  ├─── HEADERS (POST /tunnel) ───────────────────────────────►│ (握手建立)
  │◄── HEADERS (200 OK) ──────────────────────────────────────┤
  │                                                           │
  │   【全双工阶段：双方独立、并发收发二进制 DATA 帧】         │
  ├─── DATA (Raw Binary Chunks) ─────────────────────────────►│ (上行: 客户端写入 TCP 数据)
  │◄── DATA (Raw Binary Chunks) ──────────────────────────────┤ (下行: 服务端写回 TCP 回包)
  │                                                           │
  ├─── QUIC FIN (客户端 TCP 断开) ─────────────────────────────►│ (单向半关闭)
  │◄── QUIC FIN (服务端 TCP 断开) ─────────────────────────────┤ (完全终止)
```
* **性能优势**：相较于 WebSocket，HTTP/3 的 `DATA` 帧无需客户端到服务端的 4 字节掩码（Masking Key）计算，减少逐字节异或的 CPU 损耗，且头部损耗仅 $2 \sim 3$ 字节（VarInt 编码）。
* **多路复用与隔离**：单条 QUIC 物理连接内可同时派发数百条双向流，单流丢包仅重传该流对应的 Frame，其余流不受影响，彻底消除 TCP 队头阻塞。

### 3.2 UDS（Unix Domain Socket）IPC 优化
Caddy 到 Server 端采用 UDS 替代传统的 `127.0.0.1` TCP Loopback：
* **资源保护**：完全跳过 TCP/IP 协议栈封包、校验和计算与滑动窗口，避免高频短连接下的临时端口耗尽（Ephemeral Port Exhaustion）与系统 `TIME_WAIT` 堆积。
* **IPC 效率**：直接在内核缓冲区完成进程间内存拷贝，吞吐量提升约 20%，并借助文件系统权限（`0660`）天然防御本机未授权进程的探测。全平台原生支持 Linux、macOS 以及 Windows（WinSock `AF_UNIX`）。

### 3.3 协议级 0-RTT 连接池（Protocol-Aware Connection Pool）
针对 WSS 模式（单连接模型）及未来 P2P 通道，必须避免频繁建立 TLS 握手或打洞，同时防止借出“僵尸连接”。

```
   ┌──────────────────────────────────────────────────────────────┐
   │                       State: IDLE                            │
   │  * 仅预热 Client <-> Server 传输层；x-tunnel-pool: idle      │
   │  * 服务端挂起等待，严禁提前连接后端 Target 服务             │
   │  * 定期探活与 30s 维护巡检；死连接自动驱逐                   │
   └───────────────┬──────────────────────────────▲───────────────┘
                   │ Pool.acquire() (0-RTT 取出)  │
                   │ 发送 ACTIVATE (0xA5) 激活帧  │ 状态干净，确认放回
                   ▼                              │
   ┌──────────────────────────────┐               │
   │        State: ACTIVE         │               │
   │  * 服务端就近直连后端 Target │               │
   │  * 绑定本地用户 TCP 隧道     │               │
   │  * 双向裸流透传 (~5Gbps+)    │               │
   └───────────────┬──────────────┘               │
                   │ 本地 TCP 断开 / FIN          │
                   ▼                              │
   ┌──────────────────────────────────────────────┴───────────────┐
   │                      State: DRAINING                         │
   │  1. 发起端发送 `CMD_TUNNEL_RESET(session_id)`                │
   │  2. 等待并吸收通道内残留的 In-flight 脏数据                  │
   │  3. 收到对端 `CMD_RESET_ACK`，确认双向缓冲区完全排空         │
   └──────────────────────────────────────────────────────────────┘
```
* **通道预热与目标建连解耦**：预热阶段仅完成广域网（Client ↔ Server）的握手与鉴权，服务端挂起不连接后端目标服务。用户发起连接时 Client 发送单字节 `0xA5` 激活帧，服务端以 0.05ms 时延就近直连后端，彻底杜绝目标服务因空闲等待超时而重置连接的问题。
* **坏死连接自愈**：出池时进行非阻塞健康检测，若检测到远端 FIN/RST 或写入激活帧失败，连接池自动丢弃该死连接并无缝尝试下一条，全死时自动降级实时建连。
* **自适应指数退避**：补充连接在故障时采用 `100ms * 2^n`（上限 5s）指数退避，避免雪崩重试。
* **排空协议（Draining Protocol）**：当用户 TCP 隧道关闭时，禁止立即物理断开 WSS 连接。客户端发送 `CMD_TUNNEL_RESET`，服务端关闭其对应的 Target TCP，并向客户端回复 `CMD_RESET_ACK`。双方排空残存数据后，连接安全还入池中供后续请求复用。
* **主动探活**：池中闲置连接以严格小于 Caddy `read_timeout` 的周期（如 15 秒）发送协议心跳，杜绝因 NAT 超时导致的借出假死。

### 3.4 P2P 架构预留与双信道热备（P2P-Ready）
为了支持 Server 位于内网/NAT 后且无公网 IP 的场景，前期抽象在架构上完全对齐 P2P 要求：
1. **信令解耦**：客户端与服务端维持一条长驻逻辑控制通道（Control Channel），前期承载正反向建连信令，后期承载 STUN/ICE 候选地址（Candidates）交换。
2. **主备双信道热备维持（Dual-Channel Hot Standby）**：
   * **Primary Channel**：优先打通的高性能直连信道（如基于 UDP 的原生 QUIC 直连）。
   * **Standby Channel**：备选直连路径（如 TCP 打洞信道或次选网络接口）。
   * **通道池化**：打洞耗时集中在握手阶段（$100 \sim 500\text{ ms}$）。打洞成功后套接字直接纳入协议连接池进行常驻保活，业务发起连接时直接取用，免除实时打洞的漫长等待。

---

## 4. 接口与协议规范

### 4.1 端点定义与 HTTP 信令规范
系统向 Caddy 注册两个端点：
* `/tunnel` (HTTP/3 全双工流，方法: `POST`)
* `/ws` (WebSocket 降级流，方法: `GET`)

所有控制信令均在握手头中携带，通信建立后通道退化为纯裸流：

| Header 键名 | 必填 | 示例值 | 说明 |
| :--- | :--- | :--- | :--- |
| `Host` | 是 | `tunnel.example.com` | Caddy 虚拟主机路由匹配与 SNI 验证。 |
| `X-Tunnel-Auth` | 是 | `PSK_SECRET_TOKEN` | 预共享安全认证密钥。 |
| `X-Session-ID` | 是 | `UUIDv4 / u64` | 逻辑会话全局唯一标识。 |
| `X-Tunnel-Mode` | 是 | `forward` 或 `reverse` | 标明正向隧道（客户端连远端）或反向隧道（服务端推本地）。 |
| `X-Target-Host` | 否 | `127.0.0.1` | 动态转发目标地址（缺省使用服务端配置）。 |
| `X-Target-Port` | 否 | `22` | 动态转发目标端口（缺省使用服务端配置）。 |

### 4.2 统一数据流抽象 Trait (Rust)
业务层不绑定具体的传输实现，统一基于 `AnyStream` 运作：

```rust
use tokio::io::{AsyncRead, AsyncWrite};

pub trait AnyStream: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static> AnyStream for T {}

#[async_trait::async_trait]
pub trait TransportDriver: Send + Sync {
    /// 打开一条通往对端的数据通道
    async fn open_stream(
        &self,
        session_id: &str,
        target_host: &str,
        target_port: u16,
    ) -> Result<Box<dyn AnyStream>, Box<dyn std::error::Error + Send + Sync>>;
}
```

---

## 5. 配置规范

### 5.1 Caddy 生产配置 (`Caddyfile`)
```caddyfile
tunnel.example.com {
    # 禁用非必要访问日志，减小 IO
    log {
        output discard
    }

    # 1. HTTP/3 全双工二进制流通道
    reverse_proxy /tunnel unix//run/tunnel.sock {
        # 极其关键：实时刷新，彻底禁用双向缓冲队列
        flush_interval -1
        buffer_requests false
        buffer_responses false

        transport http {
            versions h2c 1.1
            read_timeout 0
            write_timeout 0
            response_header_timeout 0
        }
    }

    # 2. WSS 降级通道
    reverse_proxy /ws unix//run/tunnel.sock {
        flush_interval -1
    }

    # 3. 兜底网站（抗探测伪装，对非授权访问返回 200 或反代普通博客）
    handle {
        respond "Hello World" 200
    }
}
```

### 5.2 服务端配置 (`server.toml`)
```toml
[server]
socket_path = "/run/tunnel.sock"
auth_token = "SECURE_PRESHARED_TOKEN_HEX"
default_target = "127.0.0.1:22"

[p2p]
# 前期保留项，设为 false
enabled = false
stun_servers = ["stun.cloudflare.com:3478"]
```

### 5.3 客户端配置 (`client.toml`)
```toml
[client]
local_listen = "127.0.0.1:10022"
remote_host = "tunnel.example.com"
remote_port = 443
auth_token = "SECURE_PRESHARED_TOKEN_HEX"

# 传输策略: "quic_first" (自动回退) | "quic_only" | "wss_only" | "p2p_first"
transport_mode = "quic_first"

[pool]
# 针对 WSS 与 P2P 模式的协议连接池参数
min_idle = 2
max_idle = 8
idle_timeout_secs = 60
ping_interval_secs = 15

[target]
host = "127.0.0.1"
port = 22
```

---

## 6. 系统调优与性能指标

### 6.1 宿主机与内核调优清单
在高吞吐环境下，QUIC (UDP) 对操作系统的接收缓冲区极其敏感，需在运行 Caddy 与 Client 的宿主机调整内核参数：
```bash
# 增大 Linux UDP 接收与发送缓冲区上限，避免高速传输时丢包
sudo sysctl -w net.core.rmem_max=2500000
sudo sysctl -w net.core.wmem_max=2500000

# 优化 UDS 套接字 backlog
sudo sysctl -w net.core.somaxconn=4096
```

### 6.2 非功能性指标（NFR）
* **单流延迟开销**：在局域网内，通过 Caddy UDS 转发相较于直连 TCP 增加的延迟 $\le 0.5\text{ ms}$。
* **弱网抗丢包表现**：在模拟 $2\%$ 随机丢包的跨国线路上，由于消除了 TCP 队头阻塞，$p99$ 尾部延迟相比 WSS (TCP) 降低 $60\%$ 以上。
* **内存占用**：Server 静态编译单进程常驻内存 $\le 15\text{ MB}$。
* **容灾复位时间**：服务端热重启或 systemd reload 时，客户端底层连接熔断并完成 fail-fast 响应时间 $\le 10\text{ ms}$。

---

## 7. 实施里程碑与验收测试

### 7.1 里程碑规划
* **Phase 1: 最小可用核心（MVP）**
  * 实现 Server 端监听 UDS（Axum 0.7）。
  * 实现 Server 端 `/tunnel` 的 HTTP/3 全双工流读写与本地 TCP 桥接。
  * 编写 Caddy 配置并打通 Client (Quinn + H3) 到 Server 的正向 TCP 端口转发。
* **Phase 2: 保底与连接池（Reliability）**
  * 增加 `/ws` WebSocket 传输驱动。
  * 实现带有 `CMD_TUNNEL_RESET` 排空机制的协议级连接池（Protocol-Aware Pool）。
  * 客户端实现 QUIC 握手超时自动降级至 WSS 的探测状态机。
* **Phase 3: 双向通道完善（Full Tunneling）**
  * 实现基于控制通道（Control Channel）的 Remote-to-Local 反向穿透功能。
* **Phase 4: P2P 驱动扩展（Future Ready）**
  * 引入 STUN 穿透与 UDP/TCP 双通道打洞驱动，接入统一的 `TransportDriver`。

### 7.2 核心测试与验收准则
1. **全双工吞吐压测**：
   使用 `iperf3 -c 127.0.0.1 -p 10022 -d` 执行双向并发吞吐测试，验证隧道无死锁且跑满物理带宽。
2. **连接池防脏数据污染测试**：
   在客户端主动切断上层 TCP 传输瞬间，Target 端持续灌入大流量，验证 Draining 状态机是否正确排空所有在途数据，放回池中的连接在下一次借出时是否绝对干净。
3. **服务端热重启容灾测试**：
   在大文件持续传输过程中强行 `systemctl restart micro-tunnel-server`，验证 Caddy 是否正确返回错误流，Client 是否立即 Fail-fast 释放本地连接，无僵死进程。
4. **网络漫游迁移测试**：
   客户端在通过 QUIC 传输时，将物理网卡从 Wi-Fi 切换至手机热点，验证基于 QUIC Connection ID 的连接是否保持不中断。