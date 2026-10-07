# agent-runtime

agent-runtime 将agent的长期身份与单次执行分开。agent保留身份与状态,execution处理一次事件，action描述需要执行的外部操作。运行时负责调度、状态提交、持久化与中断恢复；目标的定义、推理过程和任务完成判断则由上层agent实现。

### 环境要求
- GO 1.27.1
- Python 3.12或更新


