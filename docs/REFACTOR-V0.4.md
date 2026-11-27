Culvert 架构重构实验：State Machine + Coordinator**

独立实验分支

保留现有接口

不改变协议行为

## **1\. 重构目标与基本理念**

### **1.1 核心理念**

**内部异步化，边界状态化。**

*   内部 IO 功能单元充分使用 Rust `async fn`、`Future` 和 `async/await`。
    
*   只在关键生命周期边界使用显式状态机。
    
*   使用 Coordinator Actor 作为 External State 的唯一修改者。
    
*   通过明确的资源所有权和状态转换管理并发。
    
*   尽可能消除分散的取消、超时、清理和状态同步逻辑。
    

不追求状态机数量，不追求抽象层次，也不追求代码量最少。真正的目标是减少系统必须同时理解的状态、分支和隐含约束。

### **1.2 需要避免的设计惯性**

不要直接沿用常见 AI 生成工程的线性异步流程：

```
accept
  -> spawn
  -> connect
  -> create channels
  -> forward
  -> handle errors
  -> cleanup
  -> wait for timeout
```

这种模式容易产生：

*   生命周期分散在多个 async 函数中。
    
*   每个模块都需要理解 CancellationToken。
    
*   各层分别添加 timeout 和 select。
    
*   资源释放逻辑散落在错误分支。
    
*   多个 Task 通过 Channel、Mutex 和 Atomic 形成复杂依赖。
    
*   一个连接的完整行为必须阅读大量代码才能理解。
    

新架构应该先建立资源和生命周期模型，再推导具体的异步实现。

## **2\. 外部接口与重构边界**

**必须保留的现有抽象接口**

**Frontend**

**Transport**

**Backend**

**允许重新设计的内部实现**

*   IO Task 的创建和调度
    
*   Tunnel 内的 Task 集合
    
*   Transport Pool
    
*   Coordinator
    
*   生命周期状态机
    
*   Shutdown / Cancellation
    
*   内部资源所有权和回收
    

约束：

1.  不改变现有 Frontend、Transport、Backend 的抽象契约。
    
2.  不要求修改现有协议实现。
    
3.  不增加与本次目标无关的功能。
    
4.  不以兼容旧内部实现为目标，可以重新组织内部模块。
    
5.  旧实现保留，作为性能、复杂度和行为的对照组。
    

## **3\. 双层状态模型**

这是整个设计的核心。

### **3.1 Internal State**

Internal State 属于 IO Task 自己，主要描述内部异步操作的执行状态。

例如：

*   Read / Write Future。
    
*   Pending Buffer。
    
*   Flush 状态。
    
*   TCP EOF。
    
*   Half-close 处理。
    
*   Transport IO 状态。
    
*   内部错误和资源清理进度。
    
*   Future / Waker 相关执行状态。
    

这些状态尽可能由 Rust async/await 和 Future 自己管理，不应无意义地再构造一套显式状态机。

### **3.2 External State**

External State 由 Coordinator 统一管理，用于描述 IO Task 对其他 Task 和资源调度系统可见的状态。

初步状态可以是：

| 状态 | 含义 |
| --- | --- |
| EMPTY | 没有绑定有效 IO 资源 |
| IDLE | 持有可分配的 Transport |
| HALF\_OPEN | 已取得一侧资源，等待另一侧 |
| ACTIVE | 前后端资源已经关联 |
| CLOSING | 正在执行关闭和资源回收 |
| CLOSED | 生命周期结束 |

这些只是候选状态，不应提前把它们固化为最终设计。

尤其需要研究：

*   HALF\_OPEN 是否真的需要独立存在。
    
*   IDLE 是 Task 状态还是 Transport Pool 的资源状态。
    
*   CLOSED 是否需要保留，还是直接回收为 EMPTY。
    
*   HALF\_CLOSE 是 External State，还是只属于 Internal State。
    

**External State 应描述资源关系和调度资格，而不是复制内部 IO 进度。**

### **3.3 两层状态允许短暂不一致**

例如：

```
Internal State = EOF
External State = ACTIVE
```

IO Task 已经发现 EOF，但 Coordinator 尚未处理完成事件。

这种短暂滞后可以接受，但必须保证：

*   已失效的资源不会被重复分配。
    
*   旧 Task 不会误操作新 Task 的资源。
    
*   资源不会提前释放或丢失。
    
*   状态更新顺序不会破坏所有权。
    

这是需要通过不变量和测试保证的地方。

## **4\. Coordinator Actor**

### **4.1 职责**

Coordinator 是 External State 的唯一写入者。

负责：

*   管理 IO Task 集合。
    
*   管理 Transport Pool 的外部资源状态。
    
*   分配和回收资源。
    
*   建立和解除资源关联。
    
*   处理 Task 生命周期事件。
    
*   维护 External State 的一致性。
    
*   协调 Tunnel Shutdown。
    

不负责：

*   实际 TCP / QUIC IO。
    
*   内部 Buffer 管理。
    
*   每次 Read / Write 的调度。
    
*   具体协议处理。
    
*   代替 IO Task 执行内部异步流程。
    

### **4.2 单写者原则**

Coordinator 独占修改 External State，因此内部数据结构可以优先使用普通 `HashMap`、`Vec` 等结构。

不要为了并发访问而默认引入 Mutex、RwLock 或 Atomic。

但需要注意：单写者不等于其他 Task 可以无锁直接读取普通 HashMap。

其他 Task 如果需要读取 External State，应根据实际需求选择：

*   通过 Coordinator 消息获取状态。
    
*   通过 `watch` 等机制获取只读状态快照。
    
*   发布必要的原子状态。
    
*   通过资源所有权和事件通知避免读取状态。
    

优先让 Task 不需要频繁查询 External State。

### **4.3 Coordinator 不应该成为新的复杂中心**

它只负责外部资源协调和生命周期边界。

如果所有 IO Task 的内部行为都必须经过 Coordinator，那么就只是把原来的复杂性搬到了一个新的 Actor 中。

目标是控制平面单写者，数据平面独立运行。

## **5\. IO Task 与 Transport Pool**

### **5.1 IO Task 是完整的 IO 功能单元**

一个 IO Task 应当能够完整表达一组关联资源的 IO 功能。

例如：

```
Frontend TCP Stream
        |
        v
     IO Task
        |
        v
Backend Transport
```

它内部通过 async/await 完成数据转发，不需要让 Coordinator 参与每一次 IO 操作。

### **5.2 Tunnel 管理 Task 集合**

一个 Tunnel 持有 IO Task 集合。

新 Frontend TCP Connection 到来时：

1.  获取一个可用的 Task。
    
2.  将 Frontend Stream 交给 Task。
    
3.  获取或申请 Backend Transport。
    
4.  建立资源关联。
    
5.  Task 进入 ACTIVE。
    
6.  内部异步执行数据转发。
    

可以研究将原来的 EMPTY → HALF\_OPEN → ACTIVE 合并为更少的转换，避免不必要的中间状态。

### **5.3 Transport 生命周期与 IO Task 生命周期必须解耦**

这是重要约束。

不能假设一个 Transport 就对应一个 IO Task。

特别是 QUIC：

*   Connection 可以长期存在。
    
*   Stream 可以独立创建和关闭。
    
*   一个 Connection 可以承载多个 Stream。
    
*   Stream 生命周期不等于 Connection 生命周期。
    

因此需要明确区分：

*   IO Task 的生命周期。
    
*   Transport Connection 的生命周期。
    
*   Transport Stream 的生命周期。
    

Transport Pool 管理可复用资源，IO Task 管理具体 IO 功能，两者通过明确的资源所有权关系关联。

## **6\. Shutdown / Cancellation：本次最重要的验证目标**

核心架构契约

**每个 IO Task 功能单元必须是纯异步、非阻塞的；外部 Stop / Join 不应该等待内部 timeout 逐层耗尽。**

### **6.1 当前模式的问题**

现有常见模式是：

```
async fn operation(cancel: &CancellationToken) -> Result<()> {
    tokio::select! {
        _ = cancel.cancelled() => {
            return Err(Error::Cancelled);
        }
        result = do_operation() => result?,
    }

    Ok(())
}
```

如果每个内部模块都需要这样的包装：

*   CancellationToken 会向内部模块传播。
    
*   Timeout 会散布到整个调用链。
    
*   各模块的取消行为容易不一致。
    
*   清理逻辑会出现在大量错误分支中。
    

### **6.2 新模式**

内部 IO 功能单元不负责感知外部生命周期。

```
async fn io_task_function(...) -> Result<()> {
    forward(...).await?;
    Ok(())
}
```

在生命周期边界统一处理：

```
async fn run_io_task(...) -> Result<()> {
    tokio::select! {
        result = io_task_function(...) => result,
        _ = stop.cancelled() => {
            // 统一进入关闭流程
            Ok(())
        }
    }
}
```

实际实现需要进一步区分：

*   Graceful Stop。
    
*   Force Stop。
    
*   正常完成。
    
*   IO 错误。
    
*   Timeout。
    
*   资源回收。
    

不能把所有情况都简单映射为一个 Cancelled。

### **6.3 Cancellation Safety**

必须特别检查 Tokio `select!` 的取消语义。

丢弃 Future 不等于所有操作都能安全取消。

重点检查：

*   TCP Read 的部分读取。
    
*   TCP Write 的部分写入。
    
*   Buffer 的所有权。
    
*   Transport 操作的取消安全性。
    
*   Flush 是否完成。
    
*   Half-close 是否已经执行。
    
*   取消时是否存在正在转移的资源。
    

对于不具备取消安全性的操作，应通过所有权设计、可恢复的操作状态或其他机制解决，而不是简单增加 `select!`。

### **6.4 可测试的 Shutdown 契约**

至少验证：

1.  内部 Future 永远 Pending 时，Stop 仍能及时退出。
    
2.  Stop 不需要等待内部 timeout。
    
3.  停止后没有遗留 IO Task。
    
4.  所有资源都能正确归还或释放。
    
5.  部分读写不会导致数据或资源状态损坏。
    
6.  重复 Stop 不会造成错误。
    
7.  Shutdown 期间新连接不会进入错误状态。
    

注意：如果某个同步操作真正阻塞了 executor，外层 `select!` 无法解决。非阻塞是必须满足的前置条件。

## **7\. 并发与资源所有权**

状态机正确不代表并发正确。需要把资源所有权作为设计的第一原则，而不是依靠锁来补救。

### **7.1 明确所有权**

任何时刻，一个资源都应该有明确的所有者：

*   Frontend Stream：由一个 IO Task 持有。
    
*   Backend Stream：由一个 IO Task 持有。
    
*   可复用 Transport Connection：由 Transport Pool 或明确的共享所有者管理。
    
*   External State：由 Coordinator 独占修改。
    
*   Internal State：由对应 IO Task 管理。
    

资源交接应该是明确的操作，而不是多个模块同时持有可修改引用。

### **7.2 状态转换与资源交接必须一致**

例如：

```
IDLE
  |
  | acquire transport
  v
ACTIVE
```

不能先把 External State 改成 ACTIVE，再异步获取资源，导致其他 Task 看到一个实际上尚未成立的状态。

需要明确每次转换的提交点：

1.  资源是否已经取得？
    
2.  所有权是否已经转移？
    
3.  Coordinator 是否已经确认转换？
    
4.  失败时由谁负责回滚或释放？
    

这里不一定需要事务系统，但必须有清晰的转换语义。

### **7.3 迟到事件与 Task 重用**

Task 被回收后可能立即分配给新的连接。旧 IO 操作产生的延迟事件不能影响新连接。

可以考虑使用：

```
struct TaskId {
    index: usize,
    generation: u64,
}
```

Task 每次重用时更新 generation，事件携带完整 TaskId。

但这只是候选方案。先判断是否存在真实的迟到事件风险，以及 Rust 所有权和消息生命周期能否直接消除问题，不要为了形式而引入额外机制。

## **8\. 状态机设计与测试方法**

状态机应该首先是一个可以独立验证的模型，而不是异步代码的另一种写法。

### **8.1 状态转换表**

每个显式状态机至少需要定义：

| 项目 | 内容 |
| --- | --- |
| State | 当前 External State |
| Event | 允许接收的事件 |
| Guard | 转换前必须满足的条件 |
| Action | 资源交接或所有权变化 |
| Next State | 转换后的状态 |
| Failure | 转换失败后的处理方式 |

非法转换必须有明确语义，不能依赖隐含的执行顺序。

### **8.2 重要测试**

建议把测试分成四类：

**纯状态测试**

*   每个合法状态转换。
    
*   非法状态转换。
    
*   重复事件。
    
*   事件乱序。
    
*   迟到事件。
    
*   Task 重用。
    

**资源所有权测试**

*   成功分配。
    
*   分配失败。
    
*   取消时释放。
    
*   重复释放。
    
*   Transport 复用。
    
*   Task 回收后再次分配。
    

**异步行为测试**

*   永远 Pending 的 Future。
    
*   EOF。
    
*   Half Close。
    
*   部分读写。
    
*   Flush。
    
*   IO 错误。
    
*   Stop 与正常完成同时发生。
    

**集成与故障测试**

*   Backend 连接失败。
    
*   Transport 断开。
    
*   Frontend 提前关闭。
    
*   Tunnel Shutdown 期间新连接到达。
    
*   大量连接同时建立和关闭。
    
*   高并发下的资源回收。
    

重点是：状态机的核心正确性测试应尽可能不依赖真实网络。

## **9\. 实施路线**

不建议一开始就同时重构 TCP、UDP、QUIC 和整个 Tunnel Runtime。先用最小范围证明架构有价值。

1.  **阶段一：建立基线**
    
    *   从当前主线创建独立实验分支。
        
    *   记录现有模块结构和关键接口。
        
    *   记录代码量、测试、性能基线。
        
    *   梳理当前 TCP Forward 的生命周期和 Shutdown 路径。
        
    
    产出：旧架构行为与复杂度基线。
    
2.  **阶段二：设计最小状态模型**
    
    *   只考虑一个 Tunnel 下的 TCP Forward。
        
    *   定义 IO Task、Transport Pool 和 Coordinator 的职责。
        
    *   定义 External State、事件及资源所有权。
        
    *   明确 Stop、Graceful Shutdown、Force Stop 的语义。
        
    
    产出：状态转换表、资源所有权图和不变量。
    
3.  **阶段三：实现最小原型**
    
    *   保留 Frontend、Transport、Backend 接口。
        
    *   实现 Coordinator。
        
    *   实现 IO Task 生命周期。
        
    *   内部继续使用 async/await。
        
    *   实现统一的 Stop / Join 边界。
        
    
    产出：能够独立运行和测试的 TCP Forward 原型。
    
4.  **阶段四：验证正确性**
    
    *   单元测试状态转换。
        
    *   测试资源交接和回收。
        
    *   测试 Cancellation Safety。
        
    *   验证 Stop 不等待内部 timeout。
        
    *   使用并发和故障注入测试。
        
    
    产出：可以重复执行的测试集。
    
5.  **阶段五：对比与评估**
    
    *   比较旧实现与新实现的复杂度。
        
    *   比较性能和内存占用。
        
    *   检查是否产生新的 Coordinator 瓶颈。
        
    *   评估测试和故障定位难度。
        
    
    产出：是否值得继续迁移的结论。
    
6.  **阶段六：扩展验证**
    
    *   再考虑 UDP。
        
    *   再考虑 QUIC 的多 Stream / Connection 生命周期。
        
    *   验证不同 Frontend、Transport、Backend 组合。
        
    *   确认架构没有依赖 TCP 特有假设。
        
    
    产出：通用 IO Task 架构的可行性结论。
    

## **10\. 如何判断重构成功**

不能仅仅以测试通过或代码更整洁作为成功标准。

建议使用以下验收标准：

| 指标 | 成功标准 |
| --- | --- |
| 接口兼容 | 现有抽象接口不需要破坏性修改 |
| 功能行为 | 现有 TCP Forward 行为保持一致 |
| 生命周期 | 每个 Task 的状态和资源归属清晰 |
| Shutdown | 不依赖内部 timeout 逐层退出 |
| 并发安全 | 没有重复分配、资源泄漏和过期事件污染 |
| 可测试性 | 状态转换可脱离网络独立测试 |
| 可维护性 | 新增功能不需要到处修改生命周期代码 |
| 性能 | 没有不可接受的吞吐、延迟和内存退化 |

还应额外关注一个指标：

**理解一个 IO Task 的完整生命周期，需要阅读多少个模块？**

如果新架构仍然需要跨越大量模块才能理解一个连接的生命周期，那么即使使用了 Actor 和状态机，也没有达到预期。

## **11\. AI 参与开发的工作原则**

这次实验不应该让 AI 自由发挥，然后通过不断审计和打补丁来收敛。

应该反过来：

1.  人先确定架构目标、边界和不变量。
    
2.  AI 推导状态模型和转换表。
    
3.  人审查设计模型，尤其是资源所有权和协议语义。
    
4.  AI 根据已确认的模型实现。
    
5.  AI 生成状态转换和异常路径测试。
    
6.  使用不同模型独立审计设计与实现。
    
7.  人通过实际运行和异常行为发现遗漏。
    
8.  发现问题时优先检查模型，而不是直接增加补丁。
    

这里有一个重要约束：**不要让 AI 在实现过程中自行扩大设计范围。**

如果实现发现原模型无法解释某种情况，应先报告设计缺口，讨论并更新模型，再修改代码。

## **12\. 明确的非目标**

本次实验不做以下事情：

*   不把所有 async 函数改成显式状态机。
    
*   不把每个 IO Task 强制对应一个 Tokio Task。
    
*   不要求所有状态都由 Coordinator 管理。
    
*   不要求所有资源状态都实时同步。
    
*   不为了消除 Mutex 而引入更复杂的消息系统。
    
*   不为了形式上的统一而抽象出庞大的通用状态机框架。
    
*   不改变 Frontend、Transport、Backend 的职责和协议语义。
    
*   不在实验阶段追求全面替换旧实现。
    

尤其要避免一个陷阱：为了证明新架构先进，反而构造出比旧架构更复杂的抽象体系。

## **13\. 开始实现前需要回答的问题**

以下问题暂时不预设答案，应该在设计阶段逐一研究：

1.  Coordinator 是每个 Tunnel 一个，还是多个 Tunnel 共用？
    
2.  IO Task 是长期存在的资源槽位，还是每次连接创建、结束后销毁？
    
3.  EMPTY、IDLE、HALF\_OPEN 是否需要全部独立存在？
    
4.  Transport Pool 和 IO Task 集合是否应该是两个独立的资源管理系统？
    
5.  External State 如何向其他 Task 发布？哪些状态实际上不需要被其他 Task 读取？
    
6.  Graceful Shutdown 的排空边界在哪里？哪些操作允许继续完成，哪些操作必须立即取消？
    
7.  是否需要 generation/lease？哪些迟到事件能够通过资源所有权直接避免？
    
8.  Coordinator 退出时，如何保证所有 Task 和 Transport 都有确定的终止结果？
    
9.  Tokio Task 的数量和 IO State Machine 的数量是否应该解耦？
    

这些问题不必一次性全部解决，但不能在实现中偷偷做出未经讨论的架构假设。

# **最终设计原则**

我建议把这几句话作为整个实验的设计准则：

# **让正确性成为架构的自然结果**

**1\. 内部异步化，边界状态化。**

使用 async/await 管理内部 IO，显式状态机只管理关键生命周期。

**2\. 控制平面单写者，数据平面独立运行。**

Coordinator 管理外部状态和资源关系，不介入每次 IO。

**3\. 资源所有权优先于锁和同步机制。**

先明确谁拥有资源，再决定是否需要同步。

**4\. 状态可以滞后，但资源不能失控。**

External State 不必实时复制 Internal State，但必须维持安全的不变量。

**5\. Stop 是架构契约，不是散落在每层的 timeout。**

IO Task 必须非阻塞，并且能够在明确的生命周期边界及时停止。

**6\. 先建立正确模型，再让 AI 实现。**

不通过不断增加工程机制来弥补错误的架构模型。
