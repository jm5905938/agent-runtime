from typing import ClassVar

from OS.domain.agent import AgentInstance, AgentStatus


class LifecycleError(Exception):
    """
    非法生命周期转换。
    """


class LifecycleManager:
    """
    Agent 生命周期管理。
    """

    _transitions: ClassVar[dict[AgentStatus, set[AgentStatus]]] = {
        # 允许的状态变化
        AgentStatus.CREATED: {AgentStatus.ACTIVE},
        AgentStatus.ACTIVE: {AgentStatus.PAUSED, AgentStatus.TERMINATING},
        AgentStatus.PAUSED: {AgentStatus.ACTIVE, AgentStatus.TERMINATING},
        AgentStatus.TERMINATING: {AgentStatus.TERMINATED},
        AgentStatus.TERMINATED: set(),
    }

    def transition(
        self,
        agent: AgentInstance,
        target: AgentStatus,
    ) -> None:
        """
        尝试迁移 Agent 状态。
        """

        allowed = self._transitions.get(agent.status, set())

        if target not in allowed:
            raise LifecycleError(f"不能从{agent.status.value}切换到{target.value}")

        agent.status = target
