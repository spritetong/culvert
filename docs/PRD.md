# Culvert 产品需求规格说明书 (PRD)

---

## 1. 产品定位与设计愿景

### 1.1 产品定位
`Culvert` 是一个专为配合现代反向代理（以 **Caddy** 为代表）设计的轻量级、高性能、生产就绪型双向 TCP 隧道系统。旨在彻底替代 `wstunnel`，在保持 Web 伪装（WSS / HTTP/3）与防火墙穿透能力的同时，通过精简架构、去除冗余分帧与协议状态机，实现极致的吞吐性能、低延迟与企业级声明式配置体验。

### 1.2 核心理念与减法哲学 (Philosophy & Non-Goals)
相较于传统隧道工具（如 `wstunnel`、`frp`、`Xray`），Culvert 遵循严格的减法哲学：
* **零内嵌证书申请管理 (No ACME)**：服务端不管理任何 ACME 证书申请逻辑，不监听公网 80/443。外部流量全部由 Caddy 终结并由 Caddy 自动签发和续签合规证书，后端服务仅监听 Unix Domain Socket (UDS) 或受限本地回环。服务端崩溃或版本升级绝不影响公网域名接入点。
* **单配置全栈纳管 (Unified Server & Tunnel Coexistence)**：支持在同一配置文件中同时声明 `server` 入口与多条 `tunnels` 实例，节点可兼具 Ingress 汇聚与中继客户端双重角色。
* **按需可选轻量鉴权 (Optional Token Authentication)**：支持高熵预共享密钥（PSK）校验，同时支持 token 省略或设为 null 免密模式（严格拒绝空字符串 ""），便于与 Caddy / Nginx 等前置反代鉴权或受信内网环境无缝对接。
* **彻底摒弃脆弱的多路复用分帧 (Native Streams & Decoupled 0-RTT Pool)**：
  * HTTP/3 下直接依托 QUIC 原生流隔离（Stream ID 隔离），单流丢包重传不阻塞其他流；
  * WSS 下采用 1:1 双工流与解耦预热连接池模型，消除私有应用层分帧带来的 CPU 二次打包开销与 TCP 队头阻塞。
  * **集群多端口共享单连接池 (Multi-Mapping Cluster Sharing Pool)**：单一 Tunnel Endpoint 可声明多个双向端口映射（Forward / Reverse 混合），共享同一个 0-RTT 连接池，流量触发时按需动态激活目标，绝不提前占用下游端口。
* **坚决不做 UDP 隧道与 DNS 代理 (No UDP / DNS Tunneling)**：不维护庞大且脆弱的 UDP 映射表与垃圾回收定时器。DNS 查询统一在客户端/服务端远端解析（Remote DNS），杜绝边缘 NAT 带来的内存泄漏隐患。
* **剔除复合代理协议 (No SOCKS5 / HTTP Proxy)**：坚持 Unix 单一职责，只专注于稳定透明的底层双向 TCP 端口转发。如需 SOCKS5，通过标准端口映射外挂外部工具即可。
* **全生命周期自愈重试 (Self-Healing Retry Supervision)**：Supervisor 对隧道启动与运行失败进行自愈接管，支持配置重试延时与最大重试次数，默认持续重连直至远端恢复。

---

## 2. 用户场景与典型应用用例

### 2.1 场景一：将 VPS 监听端口映射到本地 (Remote Port Forwarding / 反向隧道)
* **用户痛点**：本地开发机、内网 NAS 或 GPU 服务器位于严格 NAT/防火墙后，无公网 IP，需将公网 VPS 上的某端口（如 `2222` 或 `8080`）直接映射回本地服务（如本地 `127.0.0.1:22` 或 `127.0.0.1:80`）。
* **业务链路**：
  1. 本地作为客户端，主动向 VPS 的 Caddy 边缘网关发起安全连接（HTTP/3 或 WSS）；
  2. 客户端向 Culvert Server 注册反向监听请求；
  3. VPS 上的 Culvert Server 在 VPS 本地或 `0.0.0.0` 绑定对应端口（如 `2222`）；
  4. 外部请求访问 `vps-ip:2222` 时，流量通过既有信道即时拉起数据通道回传本地，本地客户端透传至本地目标服务。

### 2.2 场景二：将 VPS 监听端口映射到云主机 (Local Port Forwarding / 正向隧道)
* **用户痛点**：VPS 需要访问云主机（具备域名与公网 Caddy 接入点）内网环境中的私有服务（如云主机内网数据库 `10.0.0.5:3306`），并在 VPS 本地开放一个访问入口（如 `127.0.0.1:13306`）。
* **业务链路**：
  1. VPS 作为客户端，在本地监听 `127.0.0.1:13306`；
  2. 本地应用连接 VPS 的 `13306` 端口；
  3. VPS 客户端通过 WSS/HTTP3 发起隧道连接至云主机 Caddy；
  4. 云主机 Culvert Server 解析目标并将流量转发至云内网 `10.0.0.5:3306`。

### 2.3 场景三：一个 Endpoint 多个双向映射共享连接池 (Multi-Mapping Sharing One Connection Pool)
* **用户痛点**：客户端与服务端之间有多个端口需要转发（例如本地 15201 映射云端 iperf3 5201，本地 13306 映射云端 MySQL 3306，同时把云端 2222 逆向映射到本地 SSH 22）。传统做法需要建立多个独立的隧道，每个隧道独立握手与建立连接池，造成网络连接爆炸与连接维护开销剧增。
* **业务链路**：
  1. 客户端声明单一 Tunnel 配置，指向远端 Ingress URL（如 `wss://tunnel.example.com/tunnel`）；
  2. 声明 `mappings: [...]` 包含多组 `forward` / `reverse` 规则；
  3. 多个映射共享同一个预热连接池；连接处于 Idle 状态时不提前拨号目标后端；
  4. 真实业务流量进入某映射端口时，发送带目标地址的单字节魔数激活报文，服务端在收到激活信号后才即时拨号目标服务，兼顾 0-RTT 与低资源占用。

### 2.4 场景四：异构多端点与多隧道并行 (Heterogeneous Multi-Endpoint & Multi-Tunnel)
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
* **FR-1.3 跨平台 IPC 极速互联 (Unix Domain Socket - UDS)**：
  * 服务端与 Caddy/Nginx 之间走 Unix Domain Socket（如 Linux/macOS 的 `/run/tunnel.sock` 或 Windows 的 `C:/temp/tunnel.sock`），免去内核网络协议栈封包、校验和计算与端口耗尽风险。
  * **全平台原生支持**：支持 Unix（Linux/macOS）以及 Windows（Windows 10 17063+ / Windows 11 / Server 2019+，基于 WinSock `AF_UNIX`），统一封装于内部 `platform` 模块，保障 100% Tokio 取消安全且零线程泄露。

### 3.2 统一 URL 解析、Server 显式协议与 Endpoint 专属绑定
* **FR-2.1 Server 监听配置与 Path 路由校验**：
  * Server 的 `listen` 字段与 Client 的 `endpoint` 采用完全一致的 URL 格式规范与协议集。
  * 支持的 scheme 有且只有 7 种：`ws`, `wss`, `https`, `h3`, `quic`, `ws+unix`, `wss+unix`（去除了 `uds`, `unix`, `ws+uds` 等别名）。
  * 彻底解耦 L4 传输介质（TCP/UDS）与 L7 应用协议（WebSocket/H3）及 HTTP URI Path 路由，例如 `ws+unix:///run/tunnel.sock?path=/tunnel&chmod=0o660`。
  * 支持在 Unix 平台上为 UDS 套接字通过 URL 查询参数指定权限模式（`?chmod=0o660`）。
  * 支持单个字符串或字符串数组声明多个 Listen 端点，服务端自动启动对应的异构协议监听。
  * **Path 路由伪装**：客户端发起请求的 URI Path 必须与服务端精确匹配；不匹配时直接以标准 **HTTP 404 Not Found** 响应，防范未授权扫描。
* **FR-2.2 可选 Token 鉴权 (Optional Auth)**：
  * Server 与 Tunnel 的 `token` 字段均为可选。未配置或设为 null 时跳过鉴权（空字符串 "" 会在加载期被拒绝），方便内网通信或前置由 Caddy/Nginx 执行鉴权。
* **FR-2.3 标准化 URL 与复合 Endpoint 解析，映射三格式规范**：
  * 支持统一解析 7 种端点 URL：`ws://...`、`wss://...`、`https://...`、`h3://...`、`quic://...`、`ws+unix://...`、`wss+unix://...`（全平台统一支持，无 OS 差别）。
  * 端口映射（`mappings`）中的 `listen` 与 `target` 统一支持三种格式：`ip:port`、`hostname:port`、`unix://path`（端口号不能省略）。显式添加 `unix://` 协议头以彻底避免与主机名/相对域名冲突。
  * 支持将 `endpoint` 配置为简易 URL 字符串，或绑定独立上下文的高级对象结构。
* **FR-2.4 丰富的主机名与 DNS 解析支持 (wstunnel 兼容规范)**：
  * 支持多种主流 DNS 协议类型：
    * `system://0.0.0.0`：系统 libc 本地解析；
    * `dns://1.1.1.1`：标准 UDP DNS 解析；
    * `https://1.1.1.1?sni=cloudflare-dns.com`：DNS over HTTPS (DoH)；
    * `tls://8.8.8.8?sni=dns.google`：DNS over TLS (DoT)。
  * 支持独立策略：
    * `prefer_ipv4`（默认优先解析与连接 IPv4）
    * `prefer_ipv6`（优先 IPv6）
    * `ipv4_only`（仅解析连接 A 记录，彻底忽略 AAAA）
    * `ipv6_only`（仅解析连接 AAAA 记录）
* **FR-2.5 Endpoint 专属细粒度 TLS 参数控制 (Rustls 原生高级覆盖)**：
  * `server_name`：独立覆盖 SNI 校验域名，支持 CDN 前置路由伪装与多租户分流。
  * `alpn`：独立指定应用层协议协商列表（如 `["h3"]` 或 `["http/1.1"]`）。
  * `insecure`：针对特定端点开启自签证书跳过校验。
  * `ca_certificate`：独立加载端点专用的自定义 CA 根证书。
  * `client_certificate` & `client_key`：独立配置 mTLS 双向认证证书与私钥。
  * `min_version` / `max_version`：显式约束 TLS 最低与最高版本（如 `1.2`, `1.3`）。
* **FR-2.6 Endpoint 专属底层 Socket 拨号微调**：
  * `connect_timeout`：单端点定制建连超时。
  * `tcp_keepalive`：单端点 TCP Keepalive 周期探测。
  * `tcp_nodelay`：默认禁用 Nagle 算法。
  * `bind_interface`：单端点指定绑定的出站网卡（多网卡分流）。

### 3.3 声明式 YAML 配置、多端口映射与自愈重试 (Declarative YAML, Multi-Mapping & Retry)
* **FR-3.1 规范化 YAML 分层声明与预检工具**：
  * 采用分层结构，全局定义 `log`、`worker_threads`、`dns`、`dialer`、`tls` 作为默认基线；
  * 提供完善的命令行校验体系：`--check` 支持在不启动守护进程的情况下执行配置反序列化、YAML 结构完整性与 Schema 静态语义校验；
  * `log.no_color` 支持纯文本日志输出；`worker_threads` 精确指定 Tokio 运行时线程数（1 为单线程当前运行时）。
* **FR-3.2 纳管集群多端口映射 (Cluster Multi-Port Mappings)**：
  * 针对每个 Tunnel Endpoint，支持声明 `mappings: [...]` 数组，定义多个独立的端口转发（Forward / Reverse 混合）。
  * 强制通过 `mappings: [...]` 数组统一定义映射规则，不再支持隧道层级顶层的 `listen` / `target` / `mode`。
* **FR-3.3 集中式生命周期监控与失败重试 (Supervisor & Retry Loop)**：
  * 主管协程（Supervisor）统一纳管 Server 与所有 Tunnel 任务的生命周期，支持在收到退出信号时一键安全平滑退出。
  * **失败自愈与重试**：支持配置 `retry: { delay: "3s", max_retries: 10 }`。初始连接失败或网络临时中断不导致进程崩溃，而是按指数/固定延时自动恢复。默认无重试次数限制（持续重连）。

### 3.4 动态目标解耦连接池与主动自愈 (Decoupled Stream Pool & Active Self-Healing)
* **FR-4.1 解耦连接池 (Decoupled 0-RTT Pool)**：
  * 针对 WS/WSS/QUIC/H3 模式维护空闲物理连接池，打上 `x-tunnel-pool: idle` 标识。
  * 服务端仅维持空闲信道保活，绝不提前拨号后端服务。
  * 客户端通过动态写入 `[0xA5, target_len: u16, target_str]` 动态激活流，实现多端口映射共享一个连接池的零等待与零无效拨号。
  * **单次使用即销毁（Stream-Level One-Shot）**：出池流用于承载业务 TCP，结束后安全 Drop 释放并平滑补充新流，避免脏状态污染。
* **FR-4.2 Actor 消息模型与主动心跳自愈 (Active Heartbeat & Fast Recovery)**：
  * 连接池演进为独立的 Actor 事件循环任务，外部通过 Request/Response 通道异步索取流，移除直接读写内部数据的非安全模式。
  * **服务端重启秒级自愈**：Pool Actor 常驻监听所有空闲流的读事件，结合定时心跳探测（`poll_send_ping` / `poll_check_alive`）。服务端发生重启时，底层断开事件在微秒级被捕获，池自动清空死连接并后台瞬时补齐，客户端新连接无需经历任何超时重试即可无缝接通。
* **FR-4.3 全双工优雅半关闭排空 (Bidirectional Half-Close Draining)**：
  * 业务 IO Task 在中继时，严格支持单向 FIN 半关闭与残余缓冲完全写出，直到双向达成 EOF 且正常 Shutdown，杜绝数据截断与 RST 异常。
* **FR-4.4 底层物理 IO 动态无缝热切 (Seamless Failover)**：
  * 支持单隧道配置主端点（H3）与备用端点（WSS）。
  * 基于 64 位单调流偏移量（Stream Offset）与端侧滑动环形重传缓冲（Ring Buffer），在 H3 受到网络 QoS 严重丢包或超时阻断时，在 $<30\text{ ms}$ 内动态切换至 WSS 物理信道，**上层应用 TCP 连接完全不中断、不丢包**。

---

## 4. 完整配置规范示例 (`config-example.yaml`)

```yaml
log:
  level: "info"         # 日志级别 (支持 trace/debug/info/warn/error；注意：环境变量 RUST_LOG 优先级高于本字段)
  output: "stdout"
  no_color: false

# Tokio 运行时 worker 线程数 (可选，默认自动检测 CPU 核心数；设为 1 时为单线程运行时)
worker_threads: 4

# 全局默认 DNS 解析设置 (兼容 wstunnel 语法规范)
dns:
  strategy: "prefer_ipv4"
  resolvers:
    - "dns://1.1.1.1"
    - "https://1.1.1.1?sni=cloudflare-dns.com"
    - "tls://8.8.8.8?sni=dns.google"
    - "system://0.0.0.0"

# 全局默认套接字拨号参数
dialer:
  connect_timeout: "10s"
  tcp_keepalive: "15s"
  tcp_nodelay: true

# 全局默认 TLS 参数
tls:
  insecure: false
  alpn: ["h3", "http/1.1"]

# 服务端 Ingress 配置 (支持与客户端 tunnels 共存同一配置)
server:
  # 支持显式协议 URL (与客户端 endpoint 协议集完全一致: ws, wss, https, h3, quic, ws+unix, wss+unix)
  listen:
    - "ws+unix:///run/tunnel.sock?path=/tunnel&chmod=0o660"
    - "wss+unix:///run/tunnel2.sock?path=/tunnel"
    - "wss://0.0.0.0:8443/tunnel"
    - "h3://0.0.0.0:8443/tunnel"
    - "ws://127.0.0.1:8080/tunnel"
  token: "SECURE_PRESHARED_TOKEN"   # 可选，省略或设为 null 为免密模式（空字符串 "" 会被拒绝）
  default_target: "127.0.0.1:5201"  # 可选，缺省目标地址 (支持 ip:port, hostname:port, unix://path)
  # TLS 证书文件 (可选，未指定时 wss/h3/quic 自动生成内存自签名证书以实现开箱即用零配置启动)
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
    token: "SECURE_PRESHARED_TOKEN"
    endpoint:
      url: "https://db-edge.example.com:443/tunnel"
      dns:
        strategy: "prefer_ipv6"
      tls:
        server_name: "db-edge.example.com"
        alpn: ["h3"]
        ca_certificate: "/etc/ssl/certs/custom_ca.pem"
        client_certificate: "/etc/ssl/certs/client.crt"
        client_key: "/etc/ssl/certs/client.key"
        min_version: "1.3"
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:18080"
        target: "10.0.0.8:8080"
```

---

## 5. 非功能性需求 (Non-Functional Requirements)

* **NFR-1 吞吐与零拷贝**：数据传输中继采用 64KB ~ 128KB 缓冲区，单连接吞吐跑满千兆至万兆物理带宽。
* **NFR-2 转发延迟开销**：局域网内通过 Caddy UDS 到 Culvert 的中继，相较于纯直连 TCP 增加的延迟不得超过 $0.5\text{ ms}$。
* **NFR-3 资源隔离与共享**：不同端点的 TLS 会话与连接池彼此隔离，但共享底层全局异步运行时与任务监控，单进程多隧道空闲内存常驻 $\le 25\text{ MB}$。
* **NFR-4 容灾与故障恢复 (Fail-Fast)**：服务端热重启或物理链路中断时，客户端与上层 TCP 连接必须在 $10\text{ ms}$ 内感知并响应。
