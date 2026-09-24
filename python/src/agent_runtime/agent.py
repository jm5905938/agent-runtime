"""agent输入快照与状态增量，持久化由go runtime负责"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol

type JSONValue = (
    None | bool | int | float | Decimal | str | list[JSONValue] | dict[str, JSONValue]
)
type JSONObject = dict[str, JSONValue]


class BusinessError(Exception):
    """本轮业务拒绝，不提交状态或action"""


@dataclass(frozen=True)
class DefinitionRef:
    id: str
    version: str


@dataclass(frozen=True)
class AgentSnapshot:
    id: str
    name: str
    definition: DefinitionRef
    status: str
    state: JSONObject | None
    state_version: int
    binding_error: str | None = None


@dataclass(frozen=True)
class Event:
    id: str
    type: str
    payload: JSONObject | None
    created_at: str


@dataclass(frozen=True)
class ExecutionContext:
    agent: AgentSnapshot
    event: Event
    execution_id: str
    attempt_id: str


@dataclass(frozen=True)
class Action:
    id: str
    type: str
    payload: JSONObject
    execution_id: str | None = None


@dataclass(frozen=True)
class ExecutionResult:
    state_update: JSONObject = field(default_factory=dict)
    actions: list[Action] = field(default_factory=list)


class Runner(Protocol):
    def run(self, context: ExecutionContext) -> ExecutionResult: ...
