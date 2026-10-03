from dataclasses import dataclass, field
from typing import Any, Protocol

from OS.domain import Action, AgentInstance, Event


@dataclass
class ExecutionContext:
    """
    agent单次上下文。
    """

    agent: AgentInstance
    event: Event


@dataclass
class ExecutionResult:
    """
    agent单次执行结果。
    """

    state_update: dict[str, Any] = field(default_factory=dict)
    actions: list[Action] = field(default_factory=list)


class AgentRunner(Protocol):
    """
    agent执行接口。
    """

    async def run(self, context: ExecutionContext) -> ExecutionResult: ...
