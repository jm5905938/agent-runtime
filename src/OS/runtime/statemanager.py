from OS.domain import AgentInstance
from OS.runtime.agent import ExecutionResult


class StateManager:
    """
    agent状态管理机
    """

    # 目前只负责result写回State

    def apply(self, agent: AgentInstance, result: ExecutionResult) -> None:
        agent.state.update(result.state_update)
