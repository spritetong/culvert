# Micro-Tunnel 详细技术方案设计规范 (DESIGN.md)

---

## 1. 整体架构与分层设计

Micro-Tunnel 专为与现代边缘网关（如 **Caddy**）协同工作设计。架构遵循分层解耦、动态分发（Dynamic Dispatch）、零冗余与声明式纳管原则，整体架构分为 7 大核心模块：

```
+---------------------------------------------------------------------------------------+
|                       声明式多隧道配置与调度中心 (Config & Supervisor)                    |
|   * YAML 声明式配置 (AppConfig: Server, Global Defaults + Tunnels + Mappings)           |
|   * TunnelSupervisor: 集中纳管 Server Ingress 监听与多 Tunnel Cluster 并发生命周期       |
|   * Self-Healing Retry: 端点初始化失败/网络断开自动延时自愈重试 (retry: delay/max_retries)|
+---------------------------------------------------------------------------------------+
                                           │
                                           ▼
+---------------------------------------------------------------------------------------+
|                    端点上下文绑定与拨号层 (Endpoint Context & Dialer)                   |
|   * EndpointContext: 每个 Endpoint 独立绑定其专属的 DNS、Dialer 与高级 TLS 配置          |
|   * DnsResolver: wstunnel 兼容 scheme (system://, dns://, dns+https://, dns+tls://)    |
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
|   * Multi-Mapping Cluster: 一个 Endpoint 承载多个双向 mappings 共享单个 Pool            |
+---------------------------------------------------------------------------------------+
        │                                  │                                  │
        ▼                                  ▼                                  ▼
+-----------------------+      +-----------------------+      +-------------------------+
|  控制信道层 (Control)  |      |   连接池层 (Pool)      |      | 传输驱动层 (Transport)   |
| * ControlMessage      |      | * ReplenishingPool    |      | * TransportRegistry     |
| * Box<dyn             |      | * 预热解耦保活        |      | * H3 Driver (quinn+h3)  |
|     ControlStream>    |      | * [0xA5, len, target] |      | * WSS (fastwebsockets)  |
| (原生 Stream + Sink)  |      |   动态按需激活        |      | * UDS Driver (IPC)      |
|                       |      | * 死连接无感剔除还池  |      | * Path 匹配与 404 伪装   |
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

### 2.2 灵活的 DNS Resolver 与协议支持 (`dialer::dns`)
全面兼容 `wstunnel` 的 DNS Resolver 规范，支持多种协议前缀：
* `dns://<IP>`：基于标准 UDP 端口 53 进行查询；
* `dns+https://<IP>?sni=<domain>`：基于 DNS over HTTPS (DoH) 查询，支持指定 SNI 防伪造；
* `dns+tls://<IP>?sni=<domain>`：基于 DNS over TLS (DoT) 查询；
* `system://0.0.0.0`：走本地系统 libc `getaddrinfo` 解析。

结合 sing-box 的 DNS 策略控制，解决跨国网络中的 IPv4/IPv6 路由劣化与 DNS 污染问题：

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
    pub resolvers: Option<Vec<String>>,
    pub cache_capacity: Option<usize>,
}

pub trait DnsResolver: Send + Sync {
    /// 根据端点专属配置的 DnsStrategy 与 upstream resolvers 异步解析域名为有序的 IP 列表
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
# ==================================================
# Micro-Tunnel 声明式主配置文件 (config-example.yaml)
# ==================================================

log:
  level: "info"         # trace | debug | info | warn | error
  output: "stdout"      # stdout | file path
  no_color: false       # 禁用彩色输出 (适合容器日志采集)

# Tokio 运行时 worker 线程数 (可选，默认核心数；1 为单线程运行时)
worker_threads: 4

# 全局默认 DNS 解析设置 (兼容 wstunnel 规范)
dns:
  strategy: "prefer_ipv4"
  resolvers:
    - "dns://1.1.1.1"
    - "dns+https://1.1.1.1?sni=cloudflare-dns.com"
    - "dns+tls://8.8.8.8?sni=dns.google"
    - "system://0.0.0.0"

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

# 服务端 Ingress 配置 (支持与客户端 tunnels 共存)
server:
  # 必须使用显式协议 URL，支持 URI Path 路由与多个 Listeners
  listen:
    - "wss://0.0.0.0:8443/tunnel"
    - "h3://0.0.0.0:8443/tunnel"
    - "ws://127.0.0.1:8080/tunnel"
  token: "SECURE_PRESHARED_TOKEN"   # 可选，省略或置空为免密
  default_target: "127.0.0.1:5201"  # 可选
  # TLS 证书文件 (可选，未指定时 wss/h3/quic 自动生成内存自签名证书以实现开箱即用)
  # certificate: "/path/to/server.crt"
  # key: "/path/to/server.key"

# 隧道集群列表
tunnels:
  # 隧道 1：多端口映射集群，共享同一个 0-RTT 连接池与自愈重试
  - tag: "corp-cluster"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    token: "SECURE_PRESHARED_TOKEN"
    pool_size: 8
    retry:
      delay: "3s"
      max_retries: 10
    mappings:
      # 正向映射：本地 15201 -> 远端 5201
      - mode: "forward"
        listen: "127.0.0.1:15201"
        target: "127.0.0.1:5201"
      # 正向映射：本地 13306 -> 远端 3306
      - mode: "forward"
        listen: "127.0.0.1:13306"
        target: "10.0.0.5:3306"
      # 逆向映射：远端 2222 -> 本地 SSH 22
      - mode: "reverse"
        listen: "0.0.0.0:2222"
        target: "127.0.0.1:22"

  # 隧道 2：经典单映射正向穿透，定制专属 IPv6 优先与 mTLS 双向认证
  - tag: "cloud-db-forward"
    mode: "forward"
    listen: "127.0.0.1:18080"
    target: "10.0.0.8:8080"
    token: "SECURE_PRESHARED_TOKEN"
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
use std::time::Duration;
use crate::dialer::dns::DnsConfig;
use crate::dialer::tls::TlsConfig;
use crate::dialer::unified::DialerOptions;

/// 解析就绪的 Server Listen 规格，显式拆解协议与 Path
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerListenSpec {
    pub scheme: String,
    pub bind_addr: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServerConfig {
    #[serde(default, alias = "listens", deserialize_with = "deserialize_one_or_many")]
    pub listen: Vec<String>,
    pub token: Option<String>,
    pub default_target: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PortMappingConfig {
    #[serde(default = "default_mapping_mode")]
    pub mode: TunnelMode,
    pub listen: String,
    pub target: String,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
pub struct TunnelRetryConfig {
    pub delay: Option<Duration>,
    pub max_retries: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TunnelConfig {
    pub tag: String,
    pub endpoint: EndpointConfig,
    pub token: Option<String>,
    pub pool_size: Option<usize>,
    pub failover: Option<FailoverConfig>,
    pub multiplex: Option<MultiplexConfig>,
    pub retry: Option<TunnelRetryConfig>,
    pub mappings: Vec<PortMappingConfig>,

    // 向前兼容单映射字段
    pub mode: Option<TunnelMode>,
    pub listen: Option<String>,
    pub target: Option<String>,
}
```
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
    fn schemes(&self) -> &[&'static str];

    fn build_connector(
        &self,
        endpoint_ctx: &EndpointContext,
        auth_token: &str,
        mode: TunnelMode,
    ) -> Result<Box<dyn TransportConnector>, TunnelError>;

    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
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

### 7.3 基于 Actor 事件循环与主动心跳的 0-RTT 连接池 (`pool`)

#### 7.3.1 连接生命周期与架构设计原则
系统针对连接生命周期确立了以下核心工程原则：
1. **单次使用，用完即销毁（Stream-Level One-Shot Lifecycle）**：
   * 业务 TCP 接入后，从连接池借出一条预热流用于代理。由于该业务连接关闭时（发送 FIN/RST），底层通道已被半关闭或写入了业务终止状态，将其重置并再次塞回连接池极易产生状态残留与数据污染。
   * 因此，**流一旦被 `acquire` 出池即脱离 Pool 管辖，生命周期结束后直接 Drop 销毁**。连接池仅作为“流的预热与分发工厂”，负责感知流被借出或销毁，并在后台主动保持健康水位。
2. **Actor 消息模型与直接数据结构访问解耦（Request-Response Pattern）**：
   * 彻底摒弃共享内存锁（如 `Arc<Mutex<VecDeque>>`）配合调用方直接写入激活魔数进行“试探性建连”的脆弱模式；
   * 连接池作为一个常驻运行的独立异步协程（**Pool Actor Loop**），外部调用方通过 `mpsc::Sender<AcquireRequest>` 发送请求，并通过随包附带的 `oneshot::Sender<Result<Box<dyn TunnelStream>, TunnelError>>` 异步等待交付；
   * 获取流的流程透明统一：池中有健康空闲流则瞬间返回（0-RTT），若池空则由 Actor 在后台发起瞬时直连并交付，业务层完全不需要感知后端底层细节。

```
                    ┌────────────────────────────────────────────────────────┐
Acquire Request ───►│                                                        │
(via oneshot reply) │                 Pool Actor Loop                        │
                    │                                                        │
Heartbeat Timer ───►│  1. 维护空闲队列 (idle_pool: VecDeque<ManagedStream>)   │
                    │  2. 定期心跳遍历所有空闲连接，淘汰 Dead 连接并立即重连     │
Replenish Event ───►│  3. 收到 AcquireRequest：                              │
                    │     - 若池中有 Healthy stream，直接 oneshot.send(stream) │
                    │     - 若池空，启动快速异步直连并返回，或立即触发补充     │
                    └────────────────────────────────────────────────────────┘
```

#### 7.3.2 主动心跳巡检与服务端重启即时自愈机制
* **痛点根因**：传统静态池在服务端重启时，操作系统虽然收到了 TCP FIN/RST，但由于没有异步任务在对空闲流调用 `read()`，事件循环无法感知对端已死。只有在客户端用户发起 TCP 请求时，池才一个个弹出死连接并遭遇超时，导致“服务端重启后客户端需要卡顿很久才能恢复”。
* **主动心跳与被动感知双轨制**：
  * **协议解耦的心跳抽象接口**：在 `TunnelStream` 增加协议无关的心跳与存活检测契约：
    ```rust
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum HeartbeatStatus {
        Alive,
        Dead,
    }

    pub trait TunnelStream: AsyncRead + AsyncWrite + Send + Sync + Unpin {
        /// 轮询向对端发送协议级 Ping / 心跳帧（WS 发送 Ping Frame，H3/QUIC 发送轻量探测）
        fn poll_send_ping(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        /// 轮询检查流活跃度：读到 EOF (0 字节)、ConnectionReset 或对端已关闭时返回 Dead
        fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus>;
    }
    ```
  * **常驻活跃度监听**：空闲流在池内停留期间，Pool Actor Loop 持续挂载其读就绪事件。服务端一旦发生重启或网络中断，底层流立即产生 EOF/Err，Actor 在微秒级即时捕获，**立即将所有坏死流从池中剔除并自动触发后台补充**。
  * **定时心跳巡检**：定时器（如每 5s~10s）定期驱动池内空闲连接调用 `poll_send_ping` 与 `poll_check_alive`，确保穿透中间 NAT 保持通道保活，任何无响应流即刻注销重建。
  * **结果**：服务端重启后，客户端在毫秒级内自动清理死流并完成重连注满，下一批客户端 TCP 接入时立即获得 100% 鲜活连接，无需经历任何试错延迟！

#### 7.3.3 0-RTT 预热与后端建连解耦设计 (Decoupled Pre-warming & Lazy Target Dialing)
1. **预热闲置通道（Idle Channel Pre-warming）**：
   * Client 连接池在后台向 Server 建立预热通道，握手头携带 `x-tunnel-pool: idle`。
   * Server 收到后进入挂起等待状态，**绝不触碰目标后端端口（Target Connection = 0）**。
2. **0-RTT 动态目标激活（Lazy Target Activation）**：
   * 业务 IO Task 从池中成功取到流后，写入激活帧 `[0xA5, target_len: u16, target_str]`。
   * Server 收到激活帧后就地拨号目标服务，随后立即接入中继。
   * 废除此前在池管理内部强行做 `write_all + flush` 判断连接存活的临时补丁，心跳与活性全权归还 Pool Actor，业务流与健康检查彻底解耦。


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

### 7.5 中继转发引擎与优雅双向半关闭排空状态机 (`core::relay`)
在性能核心路径（高吞吐双向中继循环）中，不使用 `tokio::select!`（避免反复取消和重新实例化 Future，以及分支优先级偏置），而是采用 `std::future::poll_fn` 状态机直接驱动：

#### 7.5.1 业务 IO Task 生命周期流转
对于每一个接入的本地 TCP 连接，其生命周期自始至终由专属的异步 IO Task 自治管理，外部 Supervisor 仅持有 `CancellationToken` 进行全局停机介入：
1. **TCP Inbound 接入**：Listener `accept()` 获得 `inbound_tcp`；
2. **Spawn 专属 IO Task**：为该连接派生异步任务，受 TaskTracker/conn_tracker 纳管；
3. **从 Pool 请求 Remote Transport**：
   - 通过 MPSC Request / Oneshot Reply 向 Pool Actor 发送流借出请求；
   - 若池中有健康连接，微秒级瞬间拿到；若池空，Actor 实时直连交付；若设置超时未交付，则报错并关闭本地连接；
4. **0-RTT 目标声明**：将动态目标地址格式化写入通道；
5. **进入 `RelayEngine::relay_with_cancel`**：驱动本地流与远程流的全双工双向排空中继；
6. **双向排空完成**：连接自然终结，Stream 直接释放 Drop，通知 Pool 水位。

#### 7.5.2 双向半关闭优雅排空状态机 (Bidirectional Half-Close Draining State Machine)
在生产环境中（例如 HTTP/1.1 长连接、SSH、大量短连接 iperf3），一方发送完数据常会先关闭写端（发送 TCP FIN 或 QUIC STREAM FIN），此时反向通道仍有大量待读写数据。如果中继盲目直接退出，会导致残余数据丢失或触发 RST。

`RelayEngine` 设计了确定性的单向传输状态机（`TransferState`）：

```
                    ┌─────────────────────────┐
                    │    RelayState::Active   │
                    │  (双向全双工高速转发)   │
                    └────────────┬────────────┘
                                 │
           ┌─────────────────────┴─────────────────────┐
           ▼                                           ▼
┌───────────────────────┐                   ┌───────────────────────┐
│ LocalTcpReadEof (FIN) │                   │ RemoteReadEof (FIN)   │
│  - 关闭 Remote 写端   │                   │  - 关闭 Local TCP 写端│
│  - 状态变为 HalfClosed │                   │  - 状态变为 HalfClosed│
└──────────┬────────────┘                   └──────────┬────────────┘
           │                                           │
           │ (继续读取对端剩余字节并排空写入本地)      │ (继续读取本地剩余字节并排空写入对端)
           └─────────────────────┬─────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  RelayState::Completed  │
                    │   (两端均完成排空与关闭)│
                    └────────────┬────────────┘
                                 │
                                 ▼
                    [低配版: 释放/Drop stream]
                    [通知 Pool 状态机水位]
```

* **状态机流转准则**：
  * **RX (A -> B)** 与 **TX (B -> A)** 各自独立跟踪 `read_pos`, `write_pos`, `read_done`, `write_shutdown`；
  * **Step 1 (Flush Write)**：将未写完的缓冲区内容优先全部推入 Writer；
  * **Step 2 (Read Chunk)**：Writer 缓冲区排空后，从 Reader 继续读取下一批数据；
  * **Step 3 (Half-Close Shutdown)**：当 Reader 返回 EOF (`Ok(0)`)，且缓冲区已全部 Flush 写入 Writer 后，对 Writer 调用 `poll_shutdown()` 发出半关闭信号；
  * **Step 4 (Completion)**：只有当 **A -> B 与 B -> A 两个方向均达成 `read_done && write_shutdown`** 时，中继才判定为成功终止（`Ok((bytes_a_to_b, bytes_b_to_a))`）；
  * **异常快速熔断**：任一方向遭遇 `ConnectionReset` 或致命 IO 错误时，立刻退出并返回 `Err`，不发生卡死挂起。

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
    ├── consts.rs               # 全局常量、配置默认值与魔数中心
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

---

## 10. 高性能现代架构优化演进

### 10.1 目标解耦的 0-RTT 预热连接池与动态路由 (`ReplenishingStreamPool`)
* **连接预热与目标解耦**：
  * 在旧架构中，连接池在建连时即绑定特定后端端口（如 5201）。当连接池预热时，服务端立即向目标发起连接，容易在大量连接空闲时导致后端连接溢出或触发远端空闲超时（如 WSL iperf3 断联）。
  * 新架构引入 `x-tunnel-pool: idle` 握手标识：客户端建立到服务端的预热空闲流，服务端仅保持传输层存活，**绝不提前触碰目标后端服务**。
* **按需即时激活与动态多路转发**：
  * 当客户端任意 mapping 端口接收到真实 TCP 流量时，以 0-RTT 从共享池取出空闲流，发送动态激活包：
    `[0xA5 (ACTIVATE_STREAM_MAGIC), target_len: u16, target_str...]`
  * 服务端接收到 0xA5 及目标地址后，即时拨号目标服务并进入双工中继。
  * **集群效益**：单一 Tunnel Endpoint 下的数十个正向端口映射完全共享同一个预热容量，避免建立多套连接池的开销。
* **陈旧死连接无感剔除**：
  * 在出池激活写入 magic 字节阶段，结合短时超时（500ms）校验；若底层物理连接已被 NAT 或服务端切断，写入立即失败并自动丢弃，无缝尝试下一条空闲连接，业务端完全无感。

### 10.2 Supervisor 错误重试与自愈控制 (`TunnelSupervisor`)
* **网络震荡容灾**：当远端 Ingress 端点启动较慢、暂时不可达或 DNS 抖动时，Supervisor 不会使进程 Panic 或退出，而是启动 `run_tunnel_with_retry` 监控协程。
* **重试策略可控**：支持通过 `retry: { delay: "3s", max_retries: 10 }` 控制间隔与上限；默认 `max_retries: None` 实现无限持久自愈。
* **全生命周期信号对齐**：监听 `CancellationToken`，在收到 SIGINT/SIGTERM 时能够干净利落地阻断重试循环并优雅退出。

### 10.3 控制信道二进制化：MessagePack (`to_vec_named`)
* 全面废除文本型 JSON，采用极度紧凑高效的二进制 MessagePack (`rmp-serde`)。
* 序列化统一使用 `rmp_serde::to_vec_named()`，在编码时输出带字段名的 Map 键值对，而非位置数组。
* **协议演进兼容性**：即使服务端与客户端版本不一致、新版本增加可选字段或重排字段，旧版本反序列化时自动忽略未知字段，新版本处理旧消息自动填充 `#[serde(default)]` 默认值，完美实现跨版本前后兼容。

### 10.4 WebSocket 传输流直驱：Zero-Task 纯状态机 (`WsTunnelStream`)
* 废弃常规基于 `tokio::io::duplex` 内存桥接及为每个连接额外 `spawn` 两个读写 pump task 的做法。
* 直接基于 `fastwebsockets::after_handshake_split` 取得底层的 `WebSocketRead` 与 `WebSocketWrite`。
* 运用**所有权转移状态机模式（Ownership Transfer Pattern）**，在 `poll_read` 和 `poll_write` 触发异步读写时将流的所有权安全借出/收回，直接实现 `tokio::io::AsyncRead` 与 `AsyncWrite`。
* **效果**：
  - 每条连接彻底**消灭 2 个后台 Task**（零额外 Task 消耗）；
  - 彻底消灭中间 duplex 环形缓冲区的双重内存拷贝；
  - 保持 100% 安全 Rust（Safe Rust），无自引用结构体困扰。

### 10.5 全链路 TCP Socket 底层性能调优 (`SocketTuner`)
* 针对正向监听、反向访客入站、服务端代理回拨、本地目标回拨的所有 TCP Stream，在连接建立或 `accept()` 的第一时刻立即通过 `SocketTuner` 强制应用：
  - `set_nodelay(true)`：彻底禁用 Nagle 延迟缓冲算法，杜绝 40ms~200ms ACK 延迟；
  - `socket2::TcpKeepalive`：自动配置 15s 空闲保活与探测间隔，及时清理网络死连接并防范中间 NAT 超时掉线。
