# agent-runtime

agent-runtime 将agent的长期身份与单次执行分开。agent保留身份与状态,execution处理一次事件，action描述需要执行的外部操作。运行时负责调度、状态提交、持久化与中断恢复；目标的定义、推理过程和任务完成判断则由上层agent实现。

### 环境要求
- GO 12.7.1
- Python 3.12或更新


## CLI

所有持久化子命令都需要 `--data-dir`。公共参数可以放在子命令前后。

| 命令 | 用途 |
| --- | --- |
| `init [--name echo]` | 创建一个 Echo Agent。 |
| `submit --agent <id> --event-id <id> --message <文本>` | 保存输入，不执行；允许显式传入空消息。 |
| `run` | 推进数据库中可执行的工作，直到没有可执行工作；输出执行前后的统计与状态。 |
| `status [--agent <id>]` | 列出 Agent，或查询指定 Agent 的状态、投递、动作与尝试记录。 |
| `retry --agent <id> --event-id <id>` | 将失败的 Delivery 重新排队，不立即执行。 |


常用公共参数：

| 参数 | 说明 |
| --- | --- |
| `--data-dir <目录>` | 自动创建数据目录，使用其中的 `store.db`。 |
| `--json` | 成功结果写入标准输出，结构化错误写入标准错误。 |
| `--python <路径>` | Python 可执行文件，默认 `python3`。 |
| `--python-source <目录>` | 包含 `agent_runtime` 包的源码目录，例如本仓库的 `python/src`。 |
| `--timeout <时长>` | 每次 Python 调用的期限，默认 `30s`，包括等待其他调用的时间。 |
| `--help` | 查看完整帮助。 |
