# runtime模块架构
相较于先前任务绑定式的agent运行，我们将agent定义为持续运行的软件实体，将任务作为agent运行生命中的一个部分而非整个生命周期。同时agent相当于OS中的线程受到控制。
# runtime职责
### 管理agent生命与注册
### 接受与调度任务
### 持久化状态
# 总体架构

Runtime 由以下核心模块组成：

- Runtime Core
- Agent Registry
- Lifecycle Manager
- Event System
- Scheduler
- Execution Context
- Executor
- State Manager

整体架构为：
```mermaid
flowchart TD

    subgraph Runtime[Agent Runtime]

        Core[Runtime Core<br/>runtime核心]

        Registry[Agent Registry<br/>Agent注册表]

        Lifecycle[Lifecycle Manager<br/>生命周期管理]

        EventSystem[Event System<br/>事件系统]

        EventQueue[Event Queue<br/>事件队列]

        Scheduler[Scheduler<br/>调度器]

        Context[Execution Context Manager<br/>执行上下文管理]

        StateManager[State Manager<br/>状态管理]

        Storage[Persistent Storage<br/>持久化存储]

        Executor[Executor<br/>执行管理]

    end


    Core --> Registry
    Core --> Lifecycle
    Core --> EventSystem
    Core --> Scheduler
    Core --> StateManager


    EventSystem --> EventQueue

    EventQueue --> Scheduler

    Scheduler --> Context

    Context --> Registry

    Context --> Lifecycle

    Context --> Executor

    Context --> StateManager

    StateManager --> Storage

```
# 运行时序
一次 Agent 执行过程由事件触发。

流程：

1. 外部事件进入 Runtime
2. Event System 接收并排队
3. Scheduler 调度目标 Agent
4. 创建 Execution Context
5. Agent 执行任务
6. Executor 执行动作
7. State Manager 保存状态
8. Agent 返回等待状态


```mermaid
sequenceDiagram

    participant Env as Environment<br/>环境
    participant Event as Event System<br/>事件系统
    participant Queue as Event Queue<br/>事件队列
    participant Scheduler as Scheduler<br/>调度器
    participant Context as Execution Context<br/>执行上下文
    participant Agent as Agent Instance<br/>Agent实例
    participant Executor as Executor<br/>执行器
    participant State as State Manager<br/>状态管理


    Env->>Event: 产生事件 Event

    Event->>Queue: 写入事件队列

    Queue->>Scheduler: 通知待处理事件

    Scheduler->>Context: 创建执行上下文

    Context->>Agent: 激活 Agent

    Agent->>Agent: 执行认知循环

    Agent->>Executor: 提交 Action

    Executor->>Env: 与环境交互

    Env-->>Executor: 返回结果

    Executor-->>Agent: 返回执行结果

    Agent-->>State: 更新运行状态

    State-->>Context: 保存检查点

    Context-->>Scheduler: 执行结束

    Scheduler-->>Agent: 返回 Idle 状态

```
