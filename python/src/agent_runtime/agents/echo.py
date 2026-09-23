"""每个实例同时处理一个echo请求"""

from uuid import uuid4

from ..agent import Action, BusinessError, ExecutionContext, ExecutionResult, JSONObject


def initial_state() -> JSONObject:
    return {
        "request_status": "idle",
        "request_event_id": None,
        "request_execution_id": None,
        "waiting_action_id": None,
        "result_event_id": None,
        "result": None,
        "error": None,
    }


class EchoAgent:
    def run(self, context: ExecutionContext) -> ExecutionResult:
        state = context.agent.state or initial_state()
        status = state.get("request_status")
        if status not in ("idle", "waiting", "succeeded", "failed"):
            raise ValueError("echo状态无效: request_status")
        if status == "waiting":
            for key in (
                "request_event_id",
                "request_execution_id",
                "waiting_action_id",
            ):
                if not isinstance(state.get(key), str) or not state[key]:
                    raise ValueError(f"echo状态无效: {key}")
        match context.event.type:
            case "echo.request":
                return self._on_request(context, state)
            case "action.result":
                return self._on_result(context, state)
            case _:
                raise BusinessError(f"不支持的事件类型: {context.event.type}")

    def _on_request(
        self, context: ExecutionContext, state: JSONObject
    ) -> ExecutionResult:
        if state["request_status"] == "waiting":
            raise BusinessError("echo agent正忙")
        payload = context.event.payload
        if not isinstance(payload, dict) or not isinstance(payload.get("message"), str):
            raise BusinessError("echo.request需要字符串消息")
        action = Action(
            id=str(uuid4()), type="echo", payload={"message": payload["message"]}
        )
        update = initial_state()
        update.update(
            request_status="waiting",
            request_event_id=context.event.id,
            request_execution_id=context.execution_id,
            waiting_action_id=action.id,
        )
        return ExecutionResult(state_update=update, actions=[action])

    def _on_result(
        self, context: ExecutionContext, state: JSONObject
    ) -> ExecutionResult:
        payload = context.event.payload
        if state["request_status"] != "waiting":
            raise BusinessError("echo没有等待中的操作")
        if not isinstance(payload, dict) or (
            payload.get("action_id") != state["waiting_action_id"]
            or payload.get("execution_id") != state["request_execution_id"]
            or payload.get("action_type") != "echo"
        ):
            raise BusinessError("action.result与echo等待中的操作不匹配")
        status = payload.get("status")
        if status == "succeeded":
            result = payload.get("result")
            if not isinstance(result, dict) or not isinstance(
                result.get("message"), str
            ):
                raise BusinessError("echo结果需要字符串消息")
            update = {
                "request_status": "succeeded",
                "result": result["message"],
                "error": None,
            }
        elif status == "failed":
            if not isinstance(payload.get("error"), str) or not payload["error"]:
                raise BusinessError("失败的echo结果需要错误消息")
            update = {
                "request_status": "failed",
                "result": None,
                "error": payload["error"],
            }
        else:
            raise BusinessError("echo操作缺少最终结果")
        update.update(waiting_action_id=None, result_event_id=context.event.id)
        return ExecutionResult(state_update=update)
