from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4


@dataclass
class Action:
    """
    Agent请求的外部操作
    """

    type: str
    payload: dict[str, Any]
    id: UUID = field(default_factory=uuid4)
    execution_id: UUID | None = None
