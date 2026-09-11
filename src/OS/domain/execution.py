from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4


class ExecutionStatus(Enum):
    """
    Execution状态
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Execution:
    """
    一次Agent执行过程
    """

    agent_id: UUID
    event_id: UUID
    id: UUID = field(default_factory=uuid4)
    status: ExecutionStatus = ExecutionStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
