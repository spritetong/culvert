# Micro-Tunnel 详细技术方案设计规范 (DESIGN.md)

---

## 1. 整体架构与分层设计

Micro-Tunnel 专为与现代边缘网关（如 **Caddy**）协同工作设计。架构遵循分层解耦、动态分发（Dynamic Dispatch）、零冗余与声明式纳管原则，整体架构分为 7 大核心模块：

```
+---------------------------------------------------------------------------------------+
|                       声明式多隧道配置与调度中心 (Config & Supervisor)                    |
|   * YAML 声明式配置 (AppConfig: Global Defaults + Tunnels)                              |
|   * TunnelSupervisor: 基于 JoinSet + CancellationToken 集中管理多隧道并发生命周期       |
+---------------------------------------------------------------------------------------+
                                           │
                                           ▼
+---------------------------------------------------------------------------------------+
|                    端点上下文绑定与拨号层 (Endpoint Context & Dialer)                   |
|   * EndpointContext: 每个 Endpoint 独立绑定其专属的 DNS、Dialer 与高级 TLS 配置          |
|   * DnsResolver: 支持 prefer_ipv4 / prefer_ipv6 / ipv4_only / ipv6_only 与独立缓存     |
|   * TlsConfig: 覆盖 SNI 伪装 (server_name), ALPN 协商, 自定义 CA, mTLS, TLS 版本约束   |
|   * Unified Dialer: 单端点定制 connect_timeout, keepalive, nodelay 与 bind_interface  |
+---------------------------------------------------------------------------------------+
                                           │
                                           ▼
+---------------------------------------------------------------------------------------+
|                              核心抽象与会话配对层 (Core & Session)                       |
|   * TunnelStream: Box<dyn TunnelStream> (原生 AsyncRead + AsyncWrite)                 |
|   * SessionManager: 逆向隧道内存路由表 (DashMap + oneshot 配对)                          |
|   * Relay Engine: 64KB 大缓冲区零拷贝双工中继                                          |
|   * Failover Wrapper: 动态底层物理 IO 无缝迁移（H3 <-> WSS）                            |
+---------------------------------------------------------------------------------------+
        │                                  │                                  │
        ▼                                  ▼                                  ▼
+-----------------------+      +-----------------------+      +-------------------------+
|  控制信道层 (Control)  |      |   连接池层 (Pool)      |      | 传输驱动层 (Transport)   |
| * ControlMessage      |      | * StreamPool          |      | * TransportRegistry     |
| * Box<dyn             |      | * 预热与保活心跳      |      | * H3 Driver (quinn+h3)  |
|     ControlStream>    |      | * Draining 协议排空   |      | * WSS (fastwebsockets)  |
| (原生 Stream + Sink)  |      | * Box<dyn StreamPool> |      | * P2P Driver (未来扩展) |
+-----------------------+      +-----------------------+      +-------------------------+
```

### 1.1 核心设计约束落实
1. **彻底摒弃 `async_trait`**：`async_trait` 宏会在每次异步方法调用时引入额外的堆分配与虚表装箱，损耗微秒级 I/O 吞吐。系统全面采用：
   * 原生 `tokio::io::AsyncRead` 与 `tokio::io::AsyncWrite`（基于 `poll_read`/`poll_write`，零开销）；
   * 控制流采用原生 `futures_core::Stream` 与 `futures_sink::Sink`；
   * 仅在连接初始化/池获取等低频边界采用手写 `Pin<Box<dyn Future<Output = ...> + Send + '_>>`（即 `BoxFuture`），阻断宏膨胀。
2. **坚持 `Box<dyn Trait>` 动态派发，消除泛型膨胀**：
   * 业务层、配对表、中继引擎完全不感知底层是 HTTP/3 流、WSS 帧流、TCP 套接字还是 UDS 套接字，统一以 `Box<dyn TunnelStream>` 交互。
   * 接口具备严密的对象安全性（Object Safety），保证编译极速、二进制体积小且扩展灵活。

---

## 2. 端点上下文绑定 (Endpoint Context Binding)、DNS 与高级 TLS

### 2.1 端点上下文与分层继承 (`dialer::endpoint`)
系统支持全局基线配置与端点细粒度定制。**每一个 Endpoint 可以独立绑定其专属的 DNS、Dialer 与 TLS 规则**：

```rust
use std::sync::Arc;
use crate::dialer::dns::DnsResolver;
use crate::dialer::tls::TlsConfig;
use crate::dialer::unified::Dialer;

/// 运行时解析就绪的端点上下文
#[derive(Clone)]
pub struct EndpointContext {
    pub scheme: String,       // "wss", "https", "h3", "tcp", "uds"
    pub host: String,         // 目标域名、IP 或套接字路径
    pub port: u16,            // 端口
    pub path: String,         // 路径 (如 "/ws", "/tunnel")
    pub query: Option<String>,
    pub dns_resolver: Arc<dyn DnsResolver>,
    pub tls_config: Option<TlsConfig>,
    pub dialer: Arc<dyn Dialer>,
}
```

### 2.2 灵活的 DNS Resolver 与策略路由 (`dialer::dns`)
借鉴 sing-box 的 DNS 策略控制，解决跨国网络中的 IPv4/IPv6 路由劣化与 DNS 污染问题：

```rust
use std::net::IpAddr;
use crate::core::stream::BoxFuture;
use crate::core::error::TunnelError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DnsStrategy {
    #[default]
    PreferIpv4,
    PreferIpv6,
    Ipv4Only,
    Ipv6Only,
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct DnsConfig {
    pub strategy: Option<DnsStrategy>,
    pub servers: Option<Vec<String>>,
    pub cache_capacity: Option<usize>,
}

pub trait DnsResolver: Send + Sync {
    /// 根据端点专属配置的 DnsStrategy 异步解析域名为有序的 IP 列表
    fn resolve<'a>(&'a self, host: &'a str) -> BoxFuture<'a, Result<Vec<IpAddr>, TunnelError>>;
}
```

* **策略说明**：
  * `prefer_ipv4`：同时查询 A 与 AAAA，将 IPv4 排在前端；
  * `prefer_ipv6`：同时查询，将 IPv6 排在前端；
  * `ipv4_only`：仅发送 A 记录查询，彻底过滤 IPv6，杜绝无路由黑洞；
  * `ipv6_only`：仅发送 AAAA 记录查询。

### 2.3 高级 TLS 配置规范 (`dialer::tls`)
基于 `rustls` 构建，支持为每一个 Endpoint 独立定制高级 TLS 特性：

```rust
use std::path::PathBuf;

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 覆盖 SNI 校验域名 (支持 Web 伪装、CDN 穿透或多虚拟主机分流)
    pub server_name: Option<String>,
    /// 调试或自签场景跳过证书链合法性检查
    #[serde(default)]
    pub insecure: bool,
    /// ALPN 协商标识列表 (例如: ["h3"], ["h2", "http/1.1"])
    pub alpn: Option<Vec<String>>,
    /// 自定义 CA 根证书路径
    pub ca_certificate: Option<PathBuf>,
    /// 客户端 mTLS 证书与私钥路径
    pub client_certificate: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
    /// TLS 版本约束 (例如: "1.2", "1.3")
    pub min_version: Option<String>,
    pub max_version: Option<String>,
}
```

### 2.4 端点专属 Dialer (`dialer::unified`)
```rust
use std::time::Duration;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct DialerOptions {
    pub connect_timeout: Option<Duration>,
    pub tcp_keepalive: Option<Duration>,
    pub tcp_nodelay: Option<bool>,
    pub bind_interface: Option<String>,
}

pub trait Dialer: Send + Sync {
    /// 执行 DNS 解析、Socket 连接与可选的 TLS 握手
    fn dial<'a>(&'a self, endpoint: &'a EndpointContext) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>>;
}
```

---

## 3. 全量声明式 YAML 配置规范 (Full Schematic YAML)

系统完全摒弃冗长脆弱的命令行 CLI 参数，采用规范化、分层清晰的 YAML 配置文件：

```yaml
# ==========================================
# Micro-Tunnel 声明式主配置文件 (config.yaml)
# ==========================================

log:
  level: "info"         # trace | debug | info | warn | error
  output: "stdout"      # stdout | file path

# 全局默认 DNS 基线配置 (可选)
dns:
  strategy: "prefer_ipv4"
  servers:
    - "system"
    - "udp://1.1.1.1:53"

# 全局默认套接字拨号参数
dialer:
  connect_timeout: "5s"
  tcp_keepalive: "15s"
  tcp_nodelay: true

# 全局默认 TLS 参数
tls:
  enabled: true
  insecure: false
  alpn: ["h3", "http/1.1"]

# 并发并行运行的隧道列表
tunnels:
  # 隧道 1：反向穿透，定制专属 DNS 与 SNI 伪装
  - tag: "vps-ssh-reverse"
    mode: "reverse"
    listen: "0.0.0.0:2222"
    target: "127.0.0.1:22"
    token: "SECURE_PSK_HEX"
    endpoint:
      url: "wss://198.51.100.1:443/ws"
      # 端点专属 DNS：仅查 IPv4
      dns:
        strategy: "ipv4_only"
        servers: ["udp://8.8.8.8:53"]
      # 端点专属 TLS：覆盖 SNI 进行 Web 伪装与自签证书跳过
      tls:
        enabled: true
        server_name: "custom-camouflage.example.com"
        insecure: true
      dialer:
        connect_timeout: "3s"

  # 隧道 2：正向穿透，走海外节点，定制专属 IPv6 优先与 mTLS 双向认证
  - tag: "cloud-db-forward"
    mode: "forward"
    listen: "127.0.0.1:13306"
    target: "10.0.0.5:3306"
    token: "SECURE_PSK_HEX"
    endpoint:
      url: "https://db-edge.example.com:443/tunnel"
      dns:
        strategy: "prefer_ipv6"
      tls:
        enabled: true
        server_name: "db-edge.example.com"
        alpn: ["h3"]
        ca_certificate: "/etc/ssl/certs/custom_ca.pem"
        client_certificate: "/etc/ssl/certs/client.crt"
        client_key: "/etc/ssl/certs/client.key"
        min_version: "1.3"
```

### 3.1 实体反序列化模型与继承策略 (`config::model`)

```rust
use serde::Deserialize;
use crate::dialer::dns::DnsConfig;
use crate::dialer::tls::TlsConfig;
use crate::dialer::unified::DialerOptions;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub log: LogConfig,
    pub dns: Option<DnsConfig>,
    pub dialer: Option<DialerOptions>,
    pub tls: Option<TlsConfig>,
    pub tunnels: Vec<TunnelConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TunnelConfig {
    pub tag: String,
    pub mode: TunnelMode,
    pub listen: String,
    pub target: String,
    pub token: String,
    pub endpoint: EndpointConfig,
    pub failover: Option<FailoverConfig>,
}

/// 端点配置：支持直接写纯字符串 URL，或展开为对象结构
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum EndpointConfig {
    Simple(String),
    Detailed {
        url: String,
        dns: Option<DnsConfig>,
        tls: Option<TlsConfig>,
        dialer: Option<DialerOptions>,
    },
}
```

---

## 4. 多隧道并行生命周期主管系统 (`supervisor`)

为了在单进程内高效、稳定地并行运行多个独立 Tunnel，系统引入主管服务模式（Supervisor Pattern）：

```
                     ┌────────────────────────────────────────┐
                     │         TunnelSupervisor (主管)         │
                     │  - 共享资源: DnsResolver, Dialer, Pool  │
                     │  - 纳管: JoinSet<Result<(), TunnelError>>│
                     │  - 信号: root CancellationToken        │
                     └───────────────────┬────────────────────┘
                                         │
        ┌────────────────────────────────┼────────────────────────────────┐
        ▼                                ▼                                ▼
┌──────────────────────┐      ┌──────────────────────┐      ┌──────────────────────┐
│  Tunnel: "ssh-rev"   │      │  Tunnel: "db-fwd"    │      │  Tunnel: "web-rev"   │
│  Mode: Reverse       │      │  Mode: Forward       │      │  Mode: Reverse       │
│  Child Token         │      │  Child Token         │      │  Child Token         │
└──────────────────────┘      └──────────────────────┘      └──────────────────────┘
```

### 4.1 主管类签名与行为设计
```rust
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use crate::config::model::AppConfig;
use crate::core::error::TunnelError;

pub struct TunnelSupervisor {
    config: AppConfig,
    cancel_token: CancellationToken,
}

impl TunnelSupervisor {
    pub fn new(config: AppConfig, cancel_token: CancellationToken) -> Self {
        Self { config, cancel_token }
    }

    /// 启动所有声明的 Tunnel，并在收到 cancel_token 时执行确定性优雅停机
    pub async fn run(&self) -> Result<(), TunnelError> {
        let mut join_set = JoinSet::new();

        for tunnel_cfg in &self.config.tunnels {
            let child_token = self.cancel_token.child_token();
            let cfg = tunnel_cfg.clone();
            let global_dns = self.config.dns.clone();
            let global_tls = self.config.tls.clone();
            let global_dialer = self.config.dialer.clone();

            // 严禁丢弃 handle，统一纳管至 JoinSet
            join_set.spawn(async move {
                Self::run_single_tunnel(cfg, global_dns, global_tls, global_dialer, child_token).await
            });
        }

        // 监听退出信号或子任务异常
        tokio::select! {
            _ = self.cancel_token.cancelled() => {
                tracing::info!("Shutdown signal received, waiting for all tunnels to drain gracefully...");
            }
            Some(res) = join_set.join_next() => {
                match res {
                    Ok(Err(e)) => tracing::error!("Tunnel failed with error: {:?}", e),
                    Err(join_err) => tracing::error!("Tunnel panic: {:?}", join_err),
                    _ => {}
                }
            }
        }

        // 等待所有任务有序完全退出（绝不调用 abort）
        while let Some(res) = join_set.join_next().await {
            let _ = res;
        }

        Ok(())
    }
}
```

---

## 5. 核心 Crate 选型与可扩展驱动架构

### 5.1 高效 IO Crate 选型准则
* **WebSocket 传输选型：`fastwebsockets`**
  * 相比传统 `tokio-tungstenite`，`fastwebsockets` 具备零拷贝切片解析机制，并利用 AVX2 / NEON 硬件向量指令（SIMD）加速掩码运算，CPU 占用率降低 60% 以上。
* **HTTP/3 传输选型：`quinn` + `h3`**
  * 纯 Rust 实现，Tokio 异步生态官方推荐。原生支持 QUIC 双向流（`SendStream` / `RecvStream`），直通 `AsyncRead` / `AsyncWrite`，零私有应用层分帧。

### 5.2 驱动工厂与注册表模式 (`TransportRegistry`)
保证未来引入 P2P（STUN/ICE/打洞）或原生直连时**核心业务逻辑 0 修改**：

```rust
use std::sync::Arc;
use std::collections::HashMap;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::core::error::TunnelError;
use crate::transport::{TargetAddr, StreamMetadata};
use crate::dialer::endpoint::EndpointContext;

pub trait TransportConnector: Send + Sync {
    fn connect<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>>;

    fn protocol_scheme(&self) -> &'static str;
}

pub trait TransportListener: Send + Sync {
    fn accept<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>>;
}

pub trait TransportDriver: Send + Sync + 'static {
    fn scheme(&self) -> &'static str;

    fn build_connector(
        &self,
        endpoint_ctx: &EndpointContext,
        auth_token: &str,
    ) -> Result<Box<dyn TransportConnector>, TunnelError>;

    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: &str,
    ) -> Result<Box<dyn TransportListener>, TunnelError>;
}

#[derive(Default, Clone)]
pub struct TransportRegistry {
    drivers: Arc<HashMap<&'static str, Arc<dyn TransportDriver>>>,
}
```

---

## 6. 动态物理 IO 无缝切换协议 (Seamless Migration Protocol)

解决运营商对 UDP/QUIC 实行间歇性 QoS 丢包、限速或阶段性 UDP 阻断的问题。支持底层从 HTTP/3 动态切换到 WSS，而**上层 TCP（SSH/MySQL）保持存活、零断开、零丢包**。

### 6.1 帧格式与时序机制
1. **全局单调流偏移量 (Stream Offset)**：所有经过该层的数据块被打上全局绝对字节偏移量标签。
2. **环形发送重传缓冲区 (Ring Buffer)**：在内存中维持小容量（如 512KB）滑动窗口，缓存未被对端 ACK 确认的字节。
3. **极简信封帧格式 (Envelope Frame)**：
   ```
   +---------------+--------------------+--------------------------+-----------------------+
   | Type (1 Byte) | Length (2 Bytes)   | Stream Offset (8 Bytes)  | Payload (Length Bytes)|
   +---------------+--------------------+--------------------------+-----------------------+
   ```
   * `Type 0x01 (DATA)`：承载业务数据。开销仅 11 字节/64KB 块（$<0.017\%$）。
   * `Type 0x02 (ACK)`：单向确认已按序接收的绝对 Offset。
   * `Type 0x03 (MIGRATE_ATTACH)`：新物理信道附着握手。
   * `Type 0x04 (MIGRATE_ACK)`：附着确认与接收点对齐。
4. **切换无感**：上层 TCP 套接字始终保持 Connected 状态，双向数据完全平滑对齐，零字节丢失，接管耗时 $<30\text{ ms}$。

---

## 7. 核心抽象与 Traits 规范

### 7.1 基础 Stream 抽象模块 (`core::stream`)
```rust
use std::pin::Pin;
use std::future::Future;
use tokio::io::{AsyncRead, AsyncWrite};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait TunnelStream: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}
impl<T> TunnelStream for T where T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}
```

### 7.2 控制信道模块 (`control`)
```rust
use std::net::SocketAddr;
use futures_core::Stream;
use futures_sink::Sink;
use crate::core::error::TunnelError;
use crate::transport::TargetAddr;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ControlMessage {
    RegisterReverse {
        bind_addr: SocketAddr,
        target_addr: TargetAddr,
    },
    RegisterAck {
        bind_addr: SocketAddr,
        success: bool,
        error_msg: Option<String>,
    },
    NewConnection {
        session_id: String,
        client_ip: SocketAddr,
    },
    Ping,
    Pong,
}

pub trait ControlStream:
    Stream<Item = Result<ControlMessage, TunnelError>>
    + Sink<ControlMessage, Error = TunnelError>
    + Send + Sync + Unpin + 'static
{}

impl<T> ControlStream for T where
    T: Stream<Item = Result<ControlMessage, TunnelError>>
        + Sink<ControlMessage, Error = TunnelError>
        + Send + Sync + Unpin + 'static
{}
```

### 7.3 连接池与排空模块 (`pool`)
```rust
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::core::error::TunnelError;

pub trait StreamPool: Send + Sync {
    fn acquire<'a>(&'a self) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>>;
    fn release<'a>(&'a self, stream: Box<dyn TunnelStream>) -> BoxFuture<'a, ()>;
    fn stats(&self) -> PoolStats;
}
```

### 7.4 会话配对路由模块 (`session`)
```rust
use dashmap::DashMap;
use tokio::sync::oneshot;
use crate::core::stream::TunnelStream;
use crate::core::error::TunnelError;

pub struct SessionManager {
    pending_sessions: DashMap<String, oneshot::Sender<Box<dyn TunnelStream>>>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            pending_sessions: DashMap::new(),
        }
    }

    pub fn register(&self, session_id: String) -> oneshot::Receiver<Box<dyn TunnelStream>> {
        let (tx, rx) = oneshot::channel();
        self.pending_sessions.insert(session_id, tx);
        rx
    }

    pub fn fulfill(&self, session_id: &str, stream: Box<dyn TunnelStream>) -> Result<(), TunnelError> {
        if let Some((_, tx)) = self.pending_sessions.remove(session_id) {
            tx.send(stream).map_err(|_| TunnelError::SessionClosed)
        } else {
            Err(TunnelError::SessionNotFound(session_id.to_string()))
        }
    }
}
```

### 7.5 中继转发引擎与 poll_fn 状态机 (`core::relay`)
在性能核心路径（高吞吐双向中继循环）中，不使用 `tokio::select!`（避免反复取消和重新实例化 Future，以及分支优先级偏置），而是采用 `std::future::poll_fn` 状态机直接驱动：
* 在单个事件循环内并发驱动 **RX（A -> B）** 与 **TX（B -> A）**；
* 直接在同一个轮询周期内检查 `cancel_token` 退出信号，优雅执行写刷新与单向 `shutdown`，杜绝丢弃在途缓冲；
* 维持 64KB 对称环形切片，实现真正的零中间协程、零额外 Task 分配与最高吞吐。

```rust
pub struct RelayEngine;

impl RelayEngine {
    pub async fn relay(
        stream_a: Box<dyn TunnelStream>,
        stream_b: Box<dyn TunnelStream>,
    ) -> Result<(u64, u64), TunnelError>;

    pub async fn relay_with_cancel(
        mut stream_a: Box<dyn TunnelStream>,
        mut stream_b: Box<dyn TunnelStream>,
        cancel_token: CancellationToken,
    ) -> Result<(u64, u64), TunnelError>;
}
```

---

## 8. 工程与编码规范 (Engineering & Coding Rules)

为了确保代码具备极高的工业级健壮性、零资源泄漏与优雅退出能力，本项目严格遵守以下 5 条工程规则：

### 8.1 异步任务生命周期与 JoinHandle 管理
* **规则**：所有通过 `tokio::spawn` 衍生的后台异步任务，必须保留其 `tokio::task::JoinHandle`。
* **执行**：
  * 使用任务集合（如 `tokio::task::JoinSet` 或结构体内维护的句柄管理器）统一纳管。
  * 在服务退出阶段，主监督协程必须显式等待所有任务有序归巢，严禁丢弃句柄导致出现脱轨的孤儿任务（Orphan Tasks）。

### 8.2 确定性优雅停机与 CancellationToken
* **规则**：所有异步长任务必须监听 `tokio_util::sync::CancellationToken` 实现退出信号响应。
* **绝对红线**：**严禁调用 `JoinHandle::abort()`**。
  * `abort()` 会在未知的 await 点强行撕裂任务，导致正在传输的缓冲区被抛弃、TCP FIN 无法正常握手、UDS 套接字文件残留以及连接池借还失衡。
  * 必须使用 `tokio::select!` 监听 token 的 `cancelled()` 信号，并走显式的资源清理流程（Clean Up -> Flush -> Close）。

### 8.3 并发同步原语与锁规范
* **规则**：使用 `parking_lot` 提供的同步原语（`parking_lot::Mutex`, `parking_lot::RwLock`, `parking_lot::Condvar`）完全替代 `std::sync` 对象。
* **严禁滥用 Atomic**：
  * 禁止使用复杂的裸原子变量组合构建脆弱的无序隐式状态机。
  * 状态流转优先使用显式领域枚举与单锁保护（或 `DashMap` 分段锁），确保状态转换具备清晰的临界区语义。

### 8.4 模块化与封装要求
* **通用全局函数中心化**：公用网络辅助函数、Socket 选项微调（TCP_NODELAY、SO_KEEPALIVE 等）、Buffer 分配器统一归纳至 `src/common/` 模块。
* **配置与常量中心化**：全局魔数、协议头键名、默认超时阈值（如心跳间隔 15s、配对超时 5s、缓冲区尺寸 64KB）统一收拢于 `src/constants.rs`。
* **面向对象领域封装**：对于功能相关的自由函数（Free Functions），严禁散落在文件内，必须按内聚性封装进对应的结构体及 `impl` 块。

### 8.5 全局统一英文注释
* **规则**：源代码中的所有注释、文档注释（Docstrings）、结构体与字段说明必须**一律使用英文（English Only）**，遵循现代开源工业级标准。

---

## 9. 项目源代码目录结构规划

```
micro-tunnel/
├── Cargo.toml
├── docs/
│   ├── micro_tunnel.md         # 原始需求与设计背景
│   ├── PRD.md                  # 产品需求说明书
│   └── DESIGN.md               # 详细技术方案与规范 (本文档)
└── src/
    ├── main.rs                 # 程序入口，解析 YAML 路径与信号捕获
    ├── constants.rs            # 全局常量、配置默认值与魔数中心
    ├── common/                 # 公共辅助模块
    │   ├── mod.rs
    │   ├── net.rs              # Socket 选项微调与配置
    │   └── task.rs             # TaskTracker / JoinSet 辅助管理
    ├── config/                 # 声明式配置模块
    │   ├── mod.rs
    │   ├── model.rs            # AppConfig, TunnelConfig, TlsConfig 实体定义
    │   └── loader.rs           # YAML 加载与模式校验
    ├── dialer/                 # 统一解析与拨号层
    │   ├── mod.rs
    │   ├── endpoint.rs         # 统一 URL 解析器与 EndpointContext 组装
    │   ├── dns.rs              # sing-box 风格 DnsResolver (Prefer/Only 策略与缓存)
    │   ├── tls.rs              # 基于 Rustls 的高级 TLS 配置与验证器 (SNI/mTLS/ALPN)
    │   └── unified.rs          # 统一异步 Dialer 实现
    ├── supervisor/             # 多隧道生命周期主管
    │   ├── mod.rs
    │   └── runner.rs           # 基于 JoinSet 的多隧道并发管理与优雅退出
    ├── core/                   # 核心抽象
    │   ├── mod.rs
    │   ├── error.rs            # 统一错误枚举
    │   ├── stream.rs           # Box<dyn TunnelStream> 与 BoxFuture
    │   ├── relay.rs            # 64KB 大缓冲双工零拷贝中继
    │   └── failover.rs         # 物理 IO 动态热切包装器 (FailoverTunnelStream)
    ├── transport/              # 传输驱动与插件注册表
    │   ├── mod.rs              # TransportDriver / Connector / Listener Traits
    │   ├── registry.rs         # TransportRegistry 插件中心
    │   ├── h3.rs               # HTTP/3 (quinn + h3) 驱动实现
    │   ├── ws.rs               # WebSocket (fastwebsockets) 驱动实现
    │   └── uds.rs              # Unix Domain Socket 服务端驱动
    ├── control/                # 反向穿透控制通道
    │   ├── mod.rs              # ControlMessage 定义与序列化
    │   └── stream.rs           # Box<dyn ControlStream> 轮询式实现
    ├── pool/                   # 连接池
    │   ├── mod.rs              # StreamPool Trait
    │   └── connection_pool.rs  # 具备 Draining 排空协议的 WSS 连接池
    ├── session/                # 会话配对
    │   ├── mod.rs
    │   └── manager.rs          # SessionManager (DashMap 内存路由表)
    └── tunnel/                 # 具体隧道业务工作流
        ├── mod.rs
        ├── forward.rs          # 正向隧道实现
        └── reverse.rs          # 反向穿透隧道实现
```
