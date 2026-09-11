from dataclasses import dataclass, field
from enum import Enum
from uuid import UUID, uuid4


class AgentStatus(Enum):
    """
    Agent实例生命周期
    """

    CREATED = "created"
    ACTIVE = "active"
    PAUSED = "paused"
    TERMINATED = "terminated"


@dataclass
class AgentInstance:
    """
    Agent实例,拥有唯一身份和持久状态。
    """

    name: str
    id: UUID = field(default_factory=uuid4)
    status: AgentStatus = AgentStatus.CREATED
    state: dict = field(default_factory=dict)
