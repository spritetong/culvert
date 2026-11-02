# Micro-Tunnel 产品需求规格说明书 (PRD)

---

## 1. 产品定位与设计愿景

### 1.1 产品定位
`Micro-Tunnel` 是一个专为配合现代反向代理（以 **Caddy** 为代表）设计的轻量级、高性能、生产就绪型双向 TCP 隧道系统。旨在彻底替代 `wstunnel`，在保持 Web 伪装（WSS / HTTP/3）与防火墙穿透能力的同时，通过精简架构、去除冗余分帧与协议状态机，实现极致的吞吐性能、低延迟与企业级声明式配置体验。

### 1.2 核心理念与减法哲学 (Philosophy & Non-Goals)
相较于传统隧道工具（如 `wstunnel`、`frp`、`Xray`），Micro-Tunnel 遵循严格的减法哲学：
* **零内嵌证书申请管理 (No ACME)**：服务端不管理任何 ACME 证书申请逻辑，不监听公网 80/443。外部流量全部由 Caddy 终结并由 Caddy 自动签发和续签合规证书，后端服务仅监听 Unix Domain Socket (UDS) 或受限本地回环。服务端崩溃或版本升级绝不影响公网域名接入点。
* **彻底摒弃应用层多路复用 (No Yamux / No Smux)**：
  * HTTP/3 下直接依托 QUIC 原生流隔离（Stream ID 隔离），单流丢包重传不阻塞其他流；
  * WSS 下采用 1:1 双工流与预热连接池模型，消除私有应用层分帧带来的 CPU 二次打包开销与 TCP 队头阻塞。
* **坚决不做 UDP 隧道与 DNS 代理 (No UDP / DNS Tunneling)**：不维护庞大且脆弱的 UDP 映射表与垃圾回收定时器。DNS 查询统一在客户端/服务端远端解析（Remote DNS），杜绝边缘 NAT 带来的内存泄漏隐患。
* **剔除复合代理协议 (No SOCKS5 / HTTP Proxy)**：坚持 Unix 单一职责，只专注于稳定透明的底层双向 TCP 端口转发。如需 SOCKS5，通过标准端口映射外挂外部工具即可。
* **极简鉴权**：摒弃复杂的 HTTP Basic/Digest 体系，采用高熵预共享密钥（PSK）头部校验。

---

## 2. 用户场景与典型应用用例

### 2.1 场景一：将 VPS 监听端口映射到本地 (Remote Port Forwarding / 反向隧道)
* **用户痛点**：本地开发机、内网 NAS 或 GPU 服务器位于严格 NAT/防火墙后，无公网 IP，需将公网 VPS 上的某端口（如 `2222` 或 `8080`）直接映射回本地服务（如本地 `127.0.0.1:22` 或 `127.0.0.1:80`）。
* **业务链路**：
  1. 本地作为客户端，主动向 VPS 的 Caddy 边缘网关发起安全连接（HTTP/3 或 WSS）；
  2. 客户端向 Micro-Tunnel Server 注册反向监听请求；
  3. VPS 上的 Micro-Tunnel Server 在 VPS 本地或 `0.0.0.0` 绑定对应端口（如 `2222`）；
  4. 外部请求访问 `vps-ip:2222` 时，流量通过既有信道即时拉起数据通道回传本地，本地客户端透传至本地目标服务。

### 2.2 场景二：将 VPS 监听端口映射到云主机 (Local Port Forwarding / 正向隧道)
* **用户痛点**：VPS 需要访问云主机（具备域名与公网 Caddy 接入点）内网环境中的私有服务（如云主机内网数据库 `10.0.0.5:3306`），并在 VPS 本地开放一个访问入口（如 `127.0.0.1:13306`）。
* **业务链路**：
  1. VPS 作为客户端，在本地监听 `127.0.0.1:13306`；
  2. 本地应用连接 VPS 的 `13306` 端口；
  3. VPS 客户端通过 WSS/HTTP3 发起隧道连接至云主机 Caddy；
  4. 云主机 Micro-Tunnel Server 解析目标并将流量转发至云内网 `10.0.0.5:3306`。

### 2.3 场景三：异构多端点与多隧道并行 (Heterogeneous Multi-Endpoint & Multi-Tunnel)
* **用户痛点**：单台主机需要同时连接不同的远端基础设施（如既连国内 CDN 伪装的 VPS，又连海外直连 QUIC 节点），不同节点对 DNS 策略（国内要 `prefer_ipv4` 防污染，海外需要 `ipv6_only` 直连）、SNI 伪装域名、mTLS 双向认证证书以及超时控制完全不同。
* **业务链路**：
  * 支持在单一配置文件内定义多个 Tunnel，**每个 Endpoint 均可独立绑定或覆写专属的 `dns`、`dialer` 与 `tls` 参数**。
  * 未显式配置的选项自动继承全局默认值，兼顾极致灵活性与配置简洁性。

---

## 3. 功能需求规格 (Functional Requirements)

### 3.1 传输与协议能力 (Transport & Protocols)
* **FR-1.1 HTTP/3 原生双向流**：
  * 支持作为优先传输驱动（QUIC/UDP 443）。
  * 采用 `POST /tunnel` 发起长流，单条 QUIC 物理连接多路复用多个并发隧道流。
  * 避免 WebSocket Masking Key 异或计算开销。
* **FR-1.2 WSS 兼容回退传输**：
  * 基于高性能 `fastwebsockets` 实现标准 WebSocket（TCP 443）全双工二进制帧传输。
  * 支持 AVX2/NEON 硬件 SIMD 掩码加速，当 UDP 受阻时自动平滑降级至 WSS。
* **FR-1.3 IPC 极速互联 (UDS)**：
  * 服务端与 Caddy 之间严格走 Unix Domain Socket (`/run/tunnel.sock`)，免去内核网络协议栈封包、校验和计算与端口耗尽风险。

### 3.2 统一 URL 解析与 Endpoint 专属绑定 (URL Parser & Per-Endpoint Binding)
* **FR-2.1 标准化 URL 与复合 Endpoint 解析**：
  * 支持统一解析各种端点 URL：如 `wss://...`、`https://...`、`h3://...`、`uds://...`、`tcp://...`。
  * 支持将 `endpoint` 配置为简易 URL 字符串，或绑定独立上下文的高级对象结构。
* **FR-2.2 Endpoint 专属 DNS 解析策略 (借鉴 sing-box)**：
  * 支持为单个 Endpoint 独立配置 DNS 解析策略与上游服务器（系统、UDP DNS、DoH）。
  * 支持独立策略：
    * `prefer_ipv4`（默认优先解析与连接 IPv4）
    * `prefer_ipv6`（优先 IPv6）
    * `ipv4_only`（仅解析连接 A 记录，彻底忽略 AAAA）
    * `ipv6_only`（仅解析连接 AAAA 记录）
* **FR-2.3 Endpoint 专属细粒度 TLS 参数控制 (Rustls 原生高级覆盖)**：
  * `server_name`：独立覆盖 SNI 校验域名，支持 CDN 前置路由伪装与多租户分流。
  * `alpn`：独立指定应用层协议协商列表（如 `["h3"]` 或 `["http/1.1"]`）。
  * `insecure` / `skip_cert_verify`：针对特定端点开启自签证书跳过校验。
  * `ca_certificate`：独立加载端点专用的自定义 CA 根证书。
  * `client_certificate` & `client_key`：独立配置 mTLS 双向认证证书与私钥。
  * `min_version` / `max_version`：显式约束 TLS 最低与最高版本（如 `1.2`, `1.3`）。
* **FR-2.4 Endpoint 专属底层 Socket 拨号微调**：
  * `connect_timeout`：单端点定制建连超时。
  * `tcp_keepalive`：单端点 TCP Keepalive 周期探测。
  * `tcp_nodelay`：默认禁用 Nagle 算法。
  * `bind_interface`：单端点指定绑定的出站网卡（多网卡分流）。

### 3.3 声明式 YAML 配置与多隧道并行 (Full Schematic YAML & Multi-Tunnel)
* **FR-3.1 规范化 YAML 分层声明**：
  * 采用分层结构，全局定义 `log`、`dns`、`dialer`、`tls` 作为默认基线；
  * `tunnels` 列表内定义并发隧道，各隧道可对 `endpoint` 内部的 `dns`、`dialer`、`tls` 进行细粒度覆写。
* **FR-3.2 纳管多个并行运行的隧道 (Multi-Tunnel Execution)**：
  * 在单一进程内，通过 `tunnels` 列表声明多个并行运行的 Tunnel（支持正向与反向混跑）。
* **FR-3.3 集中式生命周期监控**：
  * 主管协程（Supervisor）统一纳管所有 Tunnel 任务的生命周期，支持在收到退出信号时一键安全平滑退出。

### 3.4 连接池与动态热切 (Pool & Seamless Failover)
* **FR-4.1 协议连接池 (Protocol Pool)**：
  * 针对 WSS/TCP 模式维护空闲物理连接池，实现 **0-RTT** 即时出池激活。
  * 上层 TCP 断开时执行 Draining 排空协议，清理残存脏数据后安全还池。
* **FR-4.2 底层物理 IO 动态无缝热切 (Seamless Failover)**：
  * 支持单隧道配置主端点（H3）与备用端点（WSS）。
  * 基于 64 位单调流偏移量（Stream Offset）与端侧滑动环形重传缓冲（Ring Buffer），在 H3 受到网络 QoS 严重丢包或超时阻断时，在 $<30\text{ ms}$ 内动态切换至 WSS 物理信道，**上层应用 TCP 连接完全不中断、不丢包**。

---

## 4. 完整配置规范示例 (`config.yaml`)

```yaml
log:
  level: "info"
  output: "stdout"

# 全局默认 DNS 基线配置 (可选，未指定端点将继承此配置)
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

# 并发运行的多隧道列表
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

---

## 5. 非功能性需求 (Non-Functional Requirements)

* **NFR-1 吞吐与零拷贝**：数据传输中继采用 64KB ~ 128KB 缓冲区，单连接吞吐跑满千兆至万兆物理带宽。
* **NFR-2 转发延迟开销**：局域网内通过 Caddy UDS 到 Micro-Tunnel 的中继，相较于纯直连 TCP 增加的延迟不得超过 $0.5\text{ ms}$。
* **NFR-3 资源隔离与共享**：不同端点的 TLS 会话与连接池彼此隔离，但共享底层全局异步运行时与任务监控，单进程多隧道空闲内存常驻 $\le 25\text{ MB}$。
* **NFR-4 容灾与故障恢复 (Fail-Fast)**：服务端热重启或物理链路中断时，客户端与上层 TCP 连接必须在 $10\text{ ms}$ 内感知并响应。
