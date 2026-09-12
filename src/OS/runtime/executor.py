import asyncio
from typing import Any, Protocol
from uuid import UUID

from OS.domain import Action, Event


class ActionHandler(Protocol):
    async def execute(self, action: Action) -> dict[str, Any]: ...


class EchoHandler:
    """
    返回action携带的数据。
    """

    async def execute(self, action: Action) -> dict[str, Any]:
        return dict(action.payload)


class Executor:
    """
    执行action并保存为event。
    """

    def __init__(self) -> None:
        self.handlers: dict[str, ActionHandler] = {}
        self.results: dict[UUID, Event] = {}
        self.statuses: dict[UUID, str] = {}
        self._lock = asyncio.Lock()

    def register(self, action_type: str, handler: ActionHandler) -> None:
        if action_type in self.handlers:
            raise ValueError(f"已存在Action类型{action_type}")
        self.handlers[action_type] = handler

    async def execute(self, action: Action) -> Event:
        async with self._lock:
            if action.id in self.results:
                return self.results[action.id]
            if self.statuses.get(action.id) == "unknown":
                raise RuntimeError(f"action{action.id}结果未知，需要确认后处理")

            self.statuses[action.id] = "running"
            payload: dict[str, Any] = {
                "action_id": str(action.id),
                "execution_id": str(action.execution_id),
                "action_type": action.type,
            }
            try:
                handler = self.handlers.get(action.type)
                if handler is None:
                    raise ValueError(f"未注册Action类型{action.type}")
                payload["result"] = await handler.execute(action)
            except asyncio.CancelledError:
                self.statuses[action.id] = "unknown"
                raise
            except Exception as error:
                payload.update(
                    status="failed", error=f"{type(error).__name__}: {error}"
                )
            else:
                payload["status"] = "succeeded"

            event = Event(type="action.result", payload=payload)
            self.results[action.id] = event
            self.statuses[action.id] = payload["status"]
            return event
