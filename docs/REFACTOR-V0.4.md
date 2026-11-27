# Culvert 架构重构实验：State Machine + Coordinator

> 独立实验分支（`refactor/v0.4.0`）· 保留现有接口 · 不改变协议行为 · 保留旧实现作为对照组

---

## 1. 重构目标与基本理念

### 1.1 核心理念

**内部异步化，边界状态化。**

- 内部 IO 功能单元充分使用 Rust `async fn`、`Future` 和 `async/await`。
- 只在关键生命周期边界使用显式状态机。
- 使用 Coordinator Actor 作为 External State 的唯一修改者。
- 通过明确的资源所有权和状态转换管理并发。
- 尽可能消除分散的取消、超时、清理和状态同步逻辑。

不追求状态机数量，不追求抽象层次，也不追求代码量最少。真正的目标是减少系统必须同时理解的状态、分支和隐含约束。

### 1.2 需要避免的设计惯性

不要直接沿用线性异步拼装模式：

```text
accept
  -> spawn
  -> connect
  -> create channels
  -> forward
  -> handle errors
  -> cleanup
  -> wait for timeout
```

结合当前代码库（`tunnel/`、`pool/`、`io/pump.rs`、`server/runner.rs`、`common/task.rs`）中暴露出的长期运行稳定性问题，这种模式容易产生：

- 生命周期分散在多个 `async` 函数中，`TaskTracker` 需要依赖进程级 `SHUTDOWN_DEADLINE` 与 `abort_all()` 兜底。
- 每个模块都需要理解并传递 `CancellationToken`（从 `TunnelSupervisor` 到 `forward/reverse` 再穿透到 `core::serve` 与 `StreamStreamPump` 内部）。
- 各层分别添加 `timeout` 和 `select!`，退出时可能产生多层等待叠加或边界不一致。
- 资源释放逻辑散落在错误分支与 `scopeguard`（如 `on_stream_closed()`、`PendingSessionGuard`、`ReversePoolGuard`）中，一旦 future 被提前 abort，状态与真实资源容易出现窗口期错位。
- `ReplenishingStreamPool` 等模块通过未受 `TaskTracker` 直接托管的 `tokio::spawn`、`Arc<AtomicUsize>`、`Arc<Mutex<PoolStats>>` 和 `mpsc`/`oneshot` 与调用方形成隐式耦合。
- 理解一个连接的完整行为必须跨多个模块追踪。

新架构应该先建立资源和生命周期模型，再推导具体的异步实现。

---

## 2. 外部接口与重构边界

### 2.1 必须保留的现有抽象接口

对照现有代码结构，以下边界契约必须保持兼容，不作破坏性修改：

- **Frontend**（`crates/culvert/src/frontend/mod.rs`）
  - `Frontend`（`accept(&mut ReusableFutureRef<'static, FrontendOutput>)`、`shutdown`）、`FrontendAccept { channel: IoChannel, peer: PeerAddr }`、`FrontendFactory`。
- **Backend / Transport**（`crates/culvert/src/backend/mod.rs`、`crates/culvert/src/transport/mod.rs`）
  - `TransportConnector`、`TransportListener`（`accept(&mut ReusableFutureRef<'static, AcceptOutput>)`、`shutdown`）、`TransportDriver`、`StreamMetadata`。
- **Neutral I/O**（`crates/culvert/src/io/channel.rs`、`crates/culvert/src/io/stream.rs`）
  - `IoChannel`（`Stream` / `Datagram`）、`ChannelKind`、`TunnelStream`、`DatagramIo`。

> **关于 `core::serve` 与 `PumpCtx` 的边界说明**：
> 当前 `crates/culvert/src/core/serve.rs` 通过 `PumpCtx { cancel, heartbeat_interval, idle_timeout }` 将外部取消令牌、心跳和空闲超时直接注入 `StreamStreamPump::relay_with_heartbeat`（`io/pump.rs`）。在重构后的 `core + relay` 实验实现中，数据面转发内核应拆分为**不感知 `CancellationToken`** 的纯异步转发单元，将 Stop / Drain / Idle 生命周期控制上收到 IO Task 边界层；现有 `core::serve` 与 `io::pump` 保持原样作为对照组。

### 2.2 允许重新设计的内部实现与代码隔离

允许在独立模块（如 `crates/culvert/src/v2/` 或独立实验模块，不破坏原有 `core/`、`tunnel/`、`pool/` 调用路径）中重新设计：

- IO Task 的创建和调度
- Tunnel 内的 Task 集合与 Coordinator Actor
- Transport Pool 的资源托管与租约模型
- 生命周期状态机
- Shutdown / Cancellation 边界（Graceful vs Force）
- 内部资源所有权和回收

约束：

1. 不改变现有 `Frontend`、`TransportConnector`/`TransportListener`、`IoChannel`/`TunnelStream` 的抽象契约。
2. 不要求修改现有协议报文格式与 wire 行为（包括池化激活魔数 `0xA5`/`0xA7` 与 `0xAC` ACK）。
3. 不增加与本次目标无关的功能。
4. 不以兼容旧内部实现（`TaskTracker`、`ReplenishingStreamPool`、`Tunnel` 旧 runner）为目标，可以重新组织内部模块。
5. 旧实现完整保留，作为性能、复杂度和行为的对照组。

---

## 3. 双层状态模型

这是整个设计的核心。

### 3.1 Internal State

Internal State 属于 IO Task 自己，主要描述内部异步操作的执行状态。例如：

- Read / Write Future
- Pending Buffer 与 Flush 状态
- TCP EOF 与 Half-close 处理
- Transport IO 状态与活动时钟 / Keepalive 探测
- 内部错误和局部缓冲排空进度
- Future / Waker 相关执行状态

这些状态尽可能由 Rust `async/await` 和 `Future` 自己管理，不应无意义地再构造一套显式状态机。

### 3.2 External State

External State 由 Coordinator 统一管理，用于描述 IO Task 对调度系统和关闭流程可见的生命周期阶段与资源归属。

经过针对 TCP Forward 的收口分析，External State 不直接照搬候选全集（`EMPTY` / `IDLE` / `HALF_OPEN` / `ACTIVE` / `CLOSING` / `CLOSED`），而是遵循以下取舍：

- **`IDLE` 属于 Transport Pool 的空闲资源状态，不属于 IO Task 状态。**
- **IO Task 按连接动态创建，终结后直接从状态表移除**，不作为空槽位长期保留，因此不需要 `EMPTY` 和终态 `CLOSED` 长期驻留。
- **`HALF_CLOSE` 属于数据面转发的 Internal State**，不是 External State。

最终每个 IO Task 的 External State 收敛为清晰的三态（详见第 14 节）：

| 状态 | 含义 |
|---|---|
| `Connecting` | 已接收并独占 `Frontend` 流，正在取得（直连或从池中租用并激活）`Backend` 流 |
| `Active` | 前后端资源已全部就绪并交由 IO Task 独占执行双向数据转发 |
| `Closing` | 正在退出（已触发 Stop 或内部转发结束），等待最终清理与 Handle 回收 |

**External State 描述资源关系和调度/关闭资格，而不是复制内部 IO 进度。**

### 3.3 两层状态允许短暂不一致

例如：

```text
Internal State = EOF
External State = Active
```

IO Task 已经发现 EOF，但 Coordinator 尚未处理完成事件。这种短暂滞后可以接受，但必须保证：

- 已失效的资源不会被重复分配。
- 旧 Task 不会误操作新 Task 的资源。
- 资源不会提前释放或丢失。
- 状态更新顺序不会破坏所有权。

---

## 4. Coordinator Actor

### 4.1 职责

Coordinator 是 External State 的唯一写入者，负责：

- 管理运行中的 IO Task 集合及全部 `JoinHandle` / `JoinSet`（不允许 detached `tokio::spawn`）。
- 维护每个连接 Task 的 External State。
- 协调 `Frontend` 接收循环与 `TransportPool`（若启用）的启停。
- 处理 Task 生命周期事件（如进入 `Active`、正常完成、失败、取消）。
- 协调 Tunnel 级别的 Shutdown（Graceful Drain 与 Force Stop）并确保所有子任务有确定的退出结果。

Coordinator **不负责**：

- 实际 TCP / QUIC IO。
- 内部 Buffer 管理。
- 每次 Read / Write 的调度。
- 具体协议处理（如池化激活报文的读写细节本身仍在 IO Task 内部 `async fn` 执行）。
- 代替 IO Task 执行内部异步流程，或在事件循环内进行无界阻塞等待。

### 4.2 单写者原则

Coordinator 独占修改 External State，因此内部数据结构优先使用普通 `HashMap`、`Vec`、`VecDeque`，不为了并发访问而默认引入 `Mutex`、`RwLock` 或 `Atomic`。

其他 Task 对状态的交互方式：

- 数据面 Task 仅通过单向关闭信号（如 `watch` 广播当前 `StopMode`：`Running` / `GracefulDrain` / `ForceStop`）感知生命周期边界，无需主动查询 External State。
- 控制面或测试观测需要读取快照时，通过向 Coordinator 发消息或读取只读快照渠道获取。

### 4.3 Coordinator 不应该成为新的复杂中心

目标是**控制平面单写者，数据平面独立运行**。如果所有 IO Task 的获取资源、激活握手、心跳探测和转发分支都同步串行卡在 Coordinator 循环里，就只是把原来的复杂性搬到了一个新 Actor 中。

---

## 5. IO Task 与 Transport Pool

### 5.1 IO Task 是完整的 IO 功能单元

一个 IO Task 持有并独占一对关联的通道资源，在内部通过 `async/await` 完成初始化配对与数据转发，不需要让 Coordinator 参与每一次 IO 操作：

```text
Frontend IoChannel (Owned)
         |
         v
      IO Task  <--- Stop boundary (watch / select!)
         |
         v
Backend IoChannel (Owned)
```

### 5.2 Transport 生命周期与 IO Task 生命周期必须解耦

特别是 QUIC / HTTP/3：

- Connection 可以长期存在并复用。
- Stream 可以独立创建和关闭。
- 一个 Connection 可以承载多个 Stream。
- Stream 生命周期不等于 Connection 生命周期。

因此：

- **Transport Pool** 是独立的资源子系统，仅管理尚未分配的空闲 `TunnelStream`（及其所属的预建、保活、过期淘汰）。
- 当 IO Task 从 Pool 申请一条 Stream 时，执行所有权转移（`Box<dyn TunnelStream>` 从 Pool 移交给 IO Task）；同时 **Pool 的补充触发应由借出/归还的明确事件或租约释放信号驱动**，而不是靠调用方分散在各处的 `scopeguard::defer! { p.on_stream_closed(); }` 人工维护计数。
- 若启用了池化激活握手（`0xA5`/`0xA7` + `0xAC` ACK，含失败回退直连 1 次），该异步步骤在 IO Task 的 `Connecting` 阶段内部完成；失败的过期池流直接由 IO Task 丢弃（Drop），不会退回空闲池被二次分配。

---

## 6. Shutdown / Cancellation：核心验证目标

> **核心架构契约：每个 IO Task 功能单元必须是纯异步、非阻塞的；外部 Stop / Join 不应该等待内部 timeout 逐层耗尽。**

### 6.1 当前模式的问题

当前模式下，`CancellationToken` 穿透到 `forward.rs`、`connection_pool.rs` 和 `StreamStreamPump::relay_with_heartbeat`，各层各自嵌套 `select!` 与 `timeout`；退出时外层 `TaskTracker` 若超时则会触发 `abort_all()`，导致正在异步边界上的任务被硬 drop，进而把清理一致性全部压在 `scopeguard` 上。

### 6.2 新模式：边界统一裁决 + Graceful / Force 分层

内部 IO 功能单元（`Connecting` 阶段的连接/激活，`Active` 阶段的纯数据转发）不传递 `CancellationToken`，只专注把异步流程做对、做到非阻塞：

```rust
async fn run_io_task(...) -> TaskOutcome {
    // Stage 1: Connecting (acquire/dial + activation) raced against Stop
    // Stage 2: Active (pure duplex relay) raced against Stop (GracefulDrain vs ForceStop)
}
```

在实际实现中明确区分以下终止路径，而不是把所有退出都粗暴映射为 `Cancelled`：

1. **正常完成（Normal EOF）**：前后端双方向均正常完成数据传输并执行写半部关闭（`poll_shutdown`）。
2. **IO 错误（IoError）**：读/写错误或连接重置，记录原因并清理释放两侧流。
3. **空闲超时（IdleTimeout）**：数据面内部连续静默超过 `idle_timeout`（且保活探测未刷新活动时钟）触发的业务超时。
4. **Graceful Stop**：
   - Coordinator 进入 `Draining`：立即停止接收新连接，取消所有仍处于 `Connecting` 阶段的 Task。
   - 处于 `Active` 阶段的 Task 停止继续从读端拉取新数据，允许在**全局统一、短而有界的排空窗口**（如 `DEFAULT_RELAY_DRAIN_TIMEOUT`）内把已缓冲数据刷出并完成有序半关闭（避免 QUIC `SendStream` 未 `poll_shutdown` 直接 drop 触发对端 `RESET_STREAM(0)`）。
   - 若超时仍未排空，由统一边界升级为 Force Stop。
5. **Force Stop**：
   - 立即退出内层转发 `Future`，直接 Drop 通道资源并返回，不等待任何内部 timeout 或未完成的 flush。

### 6.3 Cancellation Safety 检查清单

使用 `tokio::select!` 裁决边界时，必须显式检查：

- `Frontend::accept` / `TransportListener::accept`：保留现有 `ReusableFutureRef` 槽位设计，确保在 Coordinator `select!` 中被其他分支抢占时不会丢弃进行中的 accept/TLS 握手。
- TCP Read / Write 的部分读写与 Buffer 所有权：纯数据面在被 `GracefulDrain` 暂停读或被 `ForceStop` 取消前，已读入局部缓冲的数据要么在排空窗内写完，要么随任务清晰丢弃，不跨任务泄漏。
- Transport Stream（特别是 `H3TunnelStream`、`QuinnTunnelStream`、`WsTunnelStream`）的取消安全性与后台 pump task 生命周期：Stream 被 Drop 时其内部后台 task 必须能立即终止，不得形成僵尸任务或阻塞外层 Join。

### 6.4 可测试的 Shutdown 契约

必须通过无真实网络的确定性测试验证：

1. 内部 Future 永远 `Pending` 时，Force Stop 仍能立即退出。
2. Stop 不需要等待内部长 timeout（如 30s `idle_timeout` / 10s `handshake_timeout`）逐层耗尽。
3. Coordinator 退出后没有任何遗留或 detached 的 IO Task / Pool Task。
4. 所有通道与流句柄都能确定性释放或归还，无句柄泄漏。
5. 部分读写或 Half-close 期间触发 Graceful Stop 时，行为符合边界定义且不损坏资源状态。
6. 重复触发 Stop 是幂等操作，不会引发 panic 或错误状态转换。
7. Shutdown 期间到达的新连接要么根本不被接收，要么在未进入 `Active` 前即被确定性拒绝/丢弃，不产生孤儿 Task。

---

## 7. 并发与资源所有权

把资源所有权作为设计的第一原则，而不是依靠锁来补救。

### 7.1 明确所有权

任何时刻，一个资源都只有一个明确的所有者：

- **Frontend Listener**：由 Coordinator（或其唯一托管的 Accept Loop Task）独占。
- **Frontend Stream**：从 `accept` 产出的瞬间移交给唯一对应的 IO Task 独占。
- **空闲 Backend Stream**：在池内时由 Transport Pool 独占；被借出后所有权转移给唯一对应的 IO Task。
- **可复用底层 Transport Connection**：由 `TransportConnector` / `TransportListener` 驱动内部会话缓存管理，Stream 与 Connection 生命周期解耦。
- **External State**：由 Coordinator 独占修改。
- **Internal State**：由对应 IO Task 独占管理。

### 7.2 状态转换与资源交接必须一致

禁止先把 External State 改成 `Active`，再异步去获取 `Backend` 资源。每次转换的提交点必须明确：

- `Connecting` → `Active` 只能在 `Backend` 流已成功获取且激活握手完成、前后端均已归位到 IO Task 手中之后提交。
- 在此之前若发生 `Stop` 或失败，IO Task 仅持有 `Frontend`（或刚打开即失败的 `Backend`），直接进入清理退出，不会污染任何“已连接”状态。

### 7.3 迟到事件与 Task 重用

由于本设计采用**每次连接分配单调递增的 `TaskId(u64)`**（而非复用固定下标的 Task 槽位），旧 IO Task 结束时上报的 `Completed(TaskId, Outcome)` 永远不可能匹配到新连接的 `TaskId`，从而通过所有权与唯一 ID 直接消除了 IO Task 层面的迟到事件污染。

仅对**确实存在跨生命周期复用的资源**（如 `TransportPool` 中的连接补建结果与池代际，以及后续扩展阶段的 Reverse 控制通道槽位）保留显式代际/租约校验。

---

## 8. 状态机设计与测试方法

### 8.1 状态转换表规范

每个显式状态机至少定义：

| 字段 | 含义 |
|---|---|
| `State` | 当前 External State |
| `Event` | 允许接收的事件 |
| `Guard` | 转换前必须满足的前置条件 |
| `Action` | 资源交接、所有权变化或指令派发 |
| `Next State` | 转换后的状态 |
| `Failure` | 条件不满足或非法转换的处理方式 |

非法转换必须有明确语义（记录异常或显式拒绝/忽略），不依赖隐含的执行顺序。

### 8.2 四类核心测试

1. **纯状态测试（不依赖 Tokio / 网络）**
   - 每个合法状态转换。
   - 非法状态转换（如 `Closing` 后收到 `BackendReady`）。
   - 重复事件（重复 `Stop`、重复 `Completed`）。
   - 事件乱序与迟到事件（旧 `TaskId` 通知到达）。
2. **资源所有权测试**
   - 分配成功、分配失败、重试回退直连。
   - `Connecting` 阶段取消时的资源释放（仅释放 `Frontend`）。
   - `TransportPool` 流租借转移与活跃计数一致性，无重复释放。
3. **异步行为测试（基于内存 Mock Channel / `duplex`）**
   - 永远 `Pending` 的读写 Future 下的 Force Stop 即时性。
   - EOF、Half-close、部分读写、Flush、IO 错误。
   - Graceful Stop 与正常完成同时发生时的竞争与幂等性。
4. **集成与故障测试**
   - Backend 连接失败、池内失效流静默失败触发 `0xAC` ACK 超时并回退直连。
   - Frontend 提前关闭、Tunnel Shutdown 期间新连接到达。
   - 高并发建连/断连下的资源零泄漏与 Coordinator 吞吐检查。

---

## 9. 实施路线

先用最小范围（单 Tunnel 下的 TCP Forward）验证架构价值，不一开始同时重构 TCP、UDP、QUIC 和 Reverse 全家桶。

1. **阶段一：建立基线**
   - 保留现有 `core/`、`tunnel/`、`pool/`、`io/pump.rs` 不动。
   - 记录现有 TCP Forward 代码量、主要单元测试/集成测试与吞吐基线。
   - 梳理现有 TCP Forward 两条路径（无池直连 vs 有池预热+`0xA5`/`0xAC` 激活回退）的精确行为。
2. **阶段二：定稿最小状态模型（见第 14 节）**
   - 定义 Coordinator、IO Task、Transport Pool 的职责与资源所有权。
   - 确认状态转换表、不变量与 Graceful/Force Stop 边界。
3. **阶段三：在隔离实验模块实现最小原型**
   - 复用现有 `Frontend`、`TransportConnector`、`IoChannel`、`TunnelStream` 接口。
   - 拆分纯异步双工转发内核（无 `CancellationToken`）与外层 IO Task 生命周期壳。
   - 实现 Coordinator Actor 与解耦版 Pool 交互。
4. **阶段四：验证正确性**
   - 完成纯状态转换测试、Mock 内存异步/Stop 契约测试、故障注入测试。
5. **阶段五：对比与评估**
   - 对比新旧实现的代码复杂度、模块可读性、Shutdown 响应速度、高并发吞吐与内存占用。
   - 评估 Coordinator 是否成为瓶颈、是否值得向全量迁移。
6. **阶段六：扩展验证**
   - 在架构模型无硬伤的前提下，再逐步验证 UDP（`DatagramIo`）、QUIC 多 Stream/Connection 协同以及 Reverse 双向控制通道与服务端 `ReverseInboundPool`。

---

## 10. 如何判断重构成功

| 指标 | 成功标准 |
|---|---|
| 接口兼容 | 现有 `Frontend`、`Transport*`、`IoChannel` 接口不需要破坏性修改 |
| 功能行为 | TCP Forward 行为（含池化激活、失败回退直连、半关闭）保持一致 |
| 生命周期 | 每个 Task 的状态和资源归属清晰，无 detached task |
| Shutdown | 不依赖内部 timeout 逐层退出，永远 Pending 的测试可在毫秒级 Stop |
| 并发安全 | 没有重复分配、资源泄漏、迟到事件污染和无效清理竞态 |
| 可测试性 | 核心状态机可完全脱离网络与 Tokio 独立单测 |
| 可维护性 | 理解一个连接的完整生命周期只需阅读 1~2 个内聚模块 |
| 性能 | 无不可接受的吞吐、延迟和内存退化 |

---

## 11. AI 参与开发的工作原则

1. 人先确定架构目标、边界和不变量。
2. AI 推导状态模型和转换表（已体现于第 14 节，待审定）。
3. 人审查设计模型，尤其是资源所有权和协议语义。
4. AI 根据已确认的模型在隔离实验模块实现。
5. AI 生成状态转换、Mock 异步行为和异常路径测试。
6. 使用独立审计检查设计与实现的不变量一致性。
7. 人通过实际运行和长稳压测发现遗漏。
8. 发现问题时**优先检查模型，而不是直接在实现上打补丁**。

重要约束：**不要在实现过程中自行扩大设计范围。** 如果实现发现原模型无法解释某种情况，应先停下来补充更新模型，再修改代码。

---

## 12. 明确的非目标

本次实验不做以下事情：

- 不把所有 `async` 函数改成手写 `Future` / 状态机。
- 不为了形式把内部异步 IO 强行拆碎；只要边界清晰，IO Task 内部充分使用 `async/await`。
- 不要求所有状态都由 Coordinator 实时同步镜像（拒绝把 `Internal State` 搬进 Coordinator）。
- 不为了消除必要同步而引入更复杂的消息系统。
- 不抽象出庞大的通用状态机框架。
- 不改变 `Frontend`、`Transport`、`Backend` 的职责和 wire 协议语义。
- 不在实验阶段追求全面替换旧实现。

---

## 13. 架构设计决策定稿（原待决问题收敛）

针对设计阶段提出的 9 个关键问题，经对照现有代码结构后收敛如下（作为第 14 节模型的前提约定）：

1. **Coordinator 粒度：每个 Tunnel 一个，还是多个共用？**
   - **定稿**：**每个 Tunnel 一个 Coordinator**。故障与关闭域天然隔离，符合单 Tunnel TCP Forward 最小原型的验证边界。
2. **IO Task 是长期存在的资源槽位，还是每次连接创建、结束后销毁？**
   - **定稿**：**按连接动态创建、结束后销毁**，由 Coordinator 通过单调递增的 `TaskId(u64)` 登记并持有其 `JoinHandle`（经 `JoinSet` 管理）。避免人为引入槽位池化与复用的复杂度。
3. **`EMPTY`、`IDLE`、`HALF_OPEN` 是否需要全部独立存在？**
   - **定稿**：**不需要**。`IDLE` 属于 `TransportPool` 的资源属性，不属于 IO Task；因 IO Task 不复用槽位，也不需要 `EMPTY` 和持久化的 `CLOSED`；`HALF_OPEN` 合并为 `Connecting`（尚未完成前后端配对）与 `Active`（配对完成，双向转发中）。
4. **Transport Pool 和 IO Task 集合是否应该是两个独立的资源管理系统？**
   - **定稿**：**是两个职责正交的子系统**。`TransportPool` 负责空闲流预热、保活、过期淘汰；流一旦被租出，所有权即移交对应 IO Task，不再受 Pool 回调计数（如旧版 `on_stream_closed`）耦合。Pool 的后台任务句柄同样纳入受监督的生命周期管理，绝不裸 `tokio::spawn`。
5. **External State 如何向其他 Task 发布？哪些状态实际上不需要被其他 Task 读取？**
   - **定稿**：**External State 仅由 Coordinator 内部持有，其他 IO Task 不需要读取外部状态表**。IO Task 仅通过单向信号（如 `watch::Receiver<StopMode>`）接收控制平面的状态变更指令（`Running` / `GracefulDrain` / `ForceStop`）。
6. **Graceful Shutdown 的排空边界在哪里？**
   - **定稿**：
     - 收到 Stop 后立即停止 `Frontend::accept`。
     - 处于 `Connecting` 的 Task 立即取消（不允许等待连接/握手超时）。
     - 处于 `Active` 的 Task 在 `GracefulDrain` 下停止从源端读取新数据，仅给予全局统一的短排空窗口将已缓冲尾部数据写出并调用 `poll_shutdown`；到达全局截止时间或直接收到 `ForceStop` 时立即结束所有 `Active` Task。
7. **是否需要 `generation/lease`？**
   - **定稿**：IO Task 采用单调递增 `TaskId(u64)`，天然无迟到事件污染，不需要 generation。仅在存在跨代际资源复用的部件（`TransportPool` 内部流管理、后续阶段的 Reverse 监听槽位）保留显式代际标识。
8. **Coordinator 退出时，如何保证所有 Task 和 Transport 都有确定的终止结果？**
   - **定稿**：Coordinator 唯一持有所有 IO Task 的 `JoinSet`、Accept Loop Handle 与 Pool 句柄。退出时依次执行：广播停止信号 → 停止并关闭 `Frontend` → 等待（或在截止时间切换 `ForceStop` 后等待）全部 `JoinHandle` 清空 → 等待 Pool Actor 退出。
9. **Tokio Task 的数量和 IO State Machine 的数量是否应该解耦？**
   - **定稿**：阶段二~五保持 `1 IO Task = 1 Tokio Task`（内部 `async/await`），不引入自建调度器或 M:N 协程复用。只有在明确测出 Tokio Task 开销成为瓶颈时再重新评估。

---

## 14. 阶段二交付设计：TCP Forward 最小状态模型与资源所有权

> 本节为阶段三原型实现前的锁定模型，覆盖单 Tunnel 下 TCP Forward（含无池直连与有池激活回退两条路径）。

### 14.1 模块与资源所有权拓扑

```text
                +-------------------------------+
                |      Tunnel Coordinator       |  <-- 唯一 External State 写入者
                |  - state: TunnelPhase         |      唯一持有 JoinSet 与子系统生命周期
                |  - tasks: HashMap<TaskId, ..> |
                |  - stop_tx: watch<StopMode>   |
                +-------+---------------+-------+
                        |               |
         (accept loop)  |               | (spawn / track / stop)
                        v               v
           +----------------+     +-----------------------------------+
           |    Frontend    |     |            IO Task (id)           |
           | ReusableFuture |     |  Owns:                            |
           +--------+-------+     |   - frontend: IoChannel (moved)   |
                    |             |   - backend: Option<IoChannel>    |
                    | Frontend    |   - stop_rx: watch<StopMode>      |
                    +------------>|  Phases (Internal async/await):   |
                       Accept     |   1. acquire/dial + 0xA5/0xAC ack |
                                  |   2. pure duplex relay + drain    |
                                  +-----------------+-----------------+
                                                    ^
                                    Acquire/Lease   | (moves Box<dyn TunnelStream>)
                                    (optional)      |
                                          +---------+---------+
                                          |   TransportPool   |
                                          | (idle inventory)  |
                                          +-------------------+
```

### 14.2 状态定义

#### A. Tunnel 级别状态（`TunnelPhase`）

| 状态 | 含义 |
|---|---|
| `Running` | 正常接收 `Frontend` 新连接并派发 IO Task |
| `Draining` | 已触发 Graceful Stop：停止 `Frontend::accept`，通知所有任务按排空规则退出 |
| `ForceStopping` | 排空超时或直接触发 Force Stop：通知所有任务立即终止内部 Future |
| `Stopped` | 所有 IO Task、`Frontend::shutdown` 与 Pool 已确定性退出完毕 |

#### B. IO Task 外部状态（`TaskState`）

| 状态 | 资源所有权 | 含义 |
|---|---|---|
| `Connecting` | Task 独占 `Frontend`；正在尝试取得并激活 `Backend` | 尚未配对成功，收到任何 Stop 应立即取消 |
| `Active` | Task 独占 `Frontend` + `Backend` | 正在执行纯双向数据转发 |
| `Closing` | Task 正在收尾并即将释放两侧资源 | 处于排空或退出阶段，等待退出回报 |

### 14.3 状态转换表（Coordinator 控制平面）

#### A. Tunnel 生命周期状态转换

| State | Event | Guard | Action | Next State | Failure |
|---|---|---|---|---|---|
| `Running` | `InboundAccepted(frontend)` | `tasks.len() < max_conns`（若有上限） | 分配新 `TaskId`，记录 `TaskState::Connecting`，spawn IO Task 移交 `frontend` 所有权 | `Running` | 超限或内部错误时直接 drop `frontend` |
| `Running` | `CmdGracefulStop` | 无 | `stop_tx.send(StopMode::GracefulDrain)`；停止 accept 并启动统一 drain 定时器；调用 `frontend.shutdown()` | `Draining` | 重复命令幂等忽略 |
| `Running` | `CmdForceStop` | 无 | `stop_tx.send(StopMode::ForceStop)`；停止 accept；调用 `frontend.shutdown()` | `ForceStopping` | 重复命令幂等忽略 |
| `Draining` | `DrainDeadlineReached` | `!tasks.is_empty()` | `stop_tx.send(StopMode::ForceStop)` | `ForceStopping` | 若 `tasks.is_empty()` 直接进 `Stopped` |
| `Draining` | `CmdForceStop` | 无 | `stop_tx.send(StopMode::ForceStop)` | `ForceStopping` | — |
| `Draining` / `ForceStopping` | `TaskFinished(id, outcome)` | `tasks.contains_key(&id)` | 从 `tasks` 移除并回收句柄；若 `tasks.is_empty()` 且 Pool/Frontend 已关闭，完成退出 | `Draining`/`ForceStopping` 或 `Stopped` | 未知 `id` 忽略并记录告警（不变量断言） |
| `Stopped` | 任何 `Stop` / 迟到事件 | 无 | 幂等忽略 | `Stopped` | — |

#### B. IO Task 外部状态转换（针对单个 `TaskId`）

| State | Event | Guard | Action | Next State | Failure |
|---|---|---|---|---|---|
| *(不存在)* | `SpawnTask(id, frontend)` | `TunnelPhase == Running` | 插入 `tasks[id] = Connecting`，将 `frontend` 移交该 Task | `Connecting` | 非 `Running` 时拒绝 spawn 并立即释放 `frontend` |
| `Connecting` | `TaskEvent::Paired(id)` | `tasks[id] == Connecting` | 确认 `Backend` 已就绪且已进入数据面，更新记录为 `Active` | `Active` | 若收到时已不在表内或状态非 `Connecting`，视为非法转换并告警 |
| `Connecting` | `TaskEvent::Failed(id, err)` / `Cancelled(id)` | `tasks[id] == Connecting` | 从 `tasks` 移除，Task 已内部 drop 临时资源 | *(已移除)* | 重复结束事件忽略 |
| `Active` | `StopMode` 变为 `GracefulDrain` | Task 正在运行 | Task 停止读新数据，进入有限排空与 `poll_shutdown` 流程（External 状态可标记为 `Closing` 或由 Task 收尾后直接结束） | `Closing` | — |
| `Active` / `Closing` | `TaskEvent::Finished(id, outcome)` | `tasks.contains_key(&id)` | 从 `tasks` 移除，JoinSet 回收完成 | *(已移除)* | 未知 `id` 忽略并记录告警 |

> **关键设计说明**：
> `TaskEvent::Paired(id)` 的提交顺序必须严格保证：**IO Task 先在内部成功拿到并激活 `Backend`（所有权已完全归自己），发出 `Paired(id)` 事件后，再开始转发；或者 Task 内部完成配对后发送非阻塞 `try_send(Paired(id))` 通知 Coordinator 记账。** Coordinator 不介入连接建立的阻塞过程，从而避免状态推进阻塞数据面。

### 14.4 不变量列表（测试与审计必检）

1. **单写者不变量**：除 Coordinator Actor 外，没有任何线程或任务可以修改 `TunnelPhase` 与 `tasks` 状态表。
2. **所有权守恒不变量**：任意时刻，任意 `IoChannel` / `TunnelStream` 至多只有一个所有者（Listener / Pool / 某个唯一 `TaskId`），一旦连接被移交给 IO Task 或在 `Connecting` 失败丢弃，Pool 与 Coordinator 不保留其访问路径。
3. **非阻塞边界不变量**：IO Task 在 `Connecting` 和 `Active` 内部禁止包含无界同步阻塞，外层必须通过 `tokio::select!` 响应 `stop_rx` 的状态切换，保证 `ForceStop` 在毫秒级退出（即使内部 read/write 永远 `Pending`）。
4. **无孤儿任务不变量**：Coordinator 返回（进入 `Stopped`）时，`JoinSet` 必须为空，Pool Actor 与 Frontend Listener 均已 `shutdown` 并 `await` 完毕，不存在任何 detached 后台任务。

---

## 最终设计原则

**让正确性成为架构的自然结果：**

1. **内部异步化，边界状态化。** 使用 `async/await` 管理内部 IO，显式状态机只管理关键生命周期。
2. **控制平面单写者，数据平面独立运行。** Coordinator 管理外部状态和资源关系，不介入每次 IO。
3. **资源所有权优先于锁和同步机制。** 先明确谁拥有资源，再决定是否需要同步。
4. **状态可以滞后，但资源不能失控。** External State 不必实时复制 Internal State，但必须维持安全的不变量。
5. **Stop 是架构契约，不是散落在每层的 timeout。** IO Task 必须非阻塞，并且能够在明确的生命周期边界及时停止。
6. **先建立正确模型，再让 AI 实现。** 不通过不断增加工程机制来弥补错误的架构模型。
