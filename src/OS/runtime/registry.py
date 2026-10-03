from uuid import UUID

from OS.domain.agent import AgentInstance


class AgentRegistry:
    """
    Agent 注册表。
    """

    def __init__(self):
        self._agents: dict[UUID, AgentInstance] = {}

    def register(self, agent: AgentInstance) -> None:
        """
        注册 Agent。
        """

        if agent.id in self._agents:
            raise ValueError(f"已存在agent{agent.id}。")

        self._agents[agent.id] = agent

    def get(self, agent_id: UUID) -> AgentInstance:
        """
        根据 ID 获取 Agent。
        """

        try:
            return self._agents[agent_id]

        except KeyError:
            raise ValueError(f"未找到agent{agent_id}。")

    def remove(self, agent_id: UUID) -> None:
        """
        删除 Agent。
        """

        if agent_id not in self._agents:
            raise ValueError(f"未找到{agent_id}。")

        del self._agents[agent_id]
