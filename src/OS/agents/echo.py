from OS.domain import Action
from OS.runtime.agent import ExecutionContext, ExecutionResult


class EchoAgent:
    """
    发出 echo 请求，在后续事件中保存结果。
    """

    async def run(self, context: ExecutionContext) -> ExecutionResult:
        event = context.event
        if event.type == "echo.request":
            action = Action(type="echo", payload=dict(event.payload))
            return ExecutionResult(
                state_update={"waiting_for": str(action.id), "status": "waiting"},
                actions=[action],
            )
        if event.type == "action.result" and event.payload[
            "action_id"
        ] == context.agent.state.get("waiting_for"):
            return ExecutionResult(
                state_update={
                    "waiting_for": None,
                    "status": event.payload["status"],
                    "result": event.payload.get("result"),
                    "error": event.payload.get("error"),
                }
            )
        return ExecutionResult()
