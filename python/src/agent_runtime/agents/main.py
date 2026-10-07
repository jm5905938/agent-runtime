"""持久化完整对话和工具调用轨迹，每个实例同时等待一个Action。"""

from copy import deepcopy
from uuid import uuid4

from ..agent import Action, BusinessError, ExecutionContext, ExecutionResult, JSONObject
from ..prompt import (
    MAX_PROMPT_BYTES,
    PromptBuilder,
    PromptTooLong,
    json_size as _json_size,
    json_text as _json_text,
)
from ..tool import MAX_TOOL_RESULT_BYTES, ToolRegistry, validate_tool_calls as _tool_calls
from ..tools import default_tools


MAX_HISTORY_TURNS = 20
MAX_HISTORY_BYTES = MAX_PROMPT_BYTES
MAX_MESSAGE_BYTES = 64 * 1024
MAX_PENDING_BYTES = 128 * 1024
MAX_TOOL_ROUNDS = 4


def initial_state() -> JSONObject:
    return {
        "messages": [],
        "pending_messages": [],
        "pending_tool_calls": [],
        "tool_rounds": 0,
        "request_status": "idle",
        "request_event_id": None,
        "request_execution_id": None,
        "waiting_action_id": None,
        "waiting_action_type": None,
        "waiting_execution_id": None,
        "result_event_id": None,
        "result": None,
        "error": None,
    }


def _turns(messages: object) -> list[list[JSONObject]]:
    """完整轮次始于user，终于不再请求工具的assistant。"""
    if not isinstance(messages, list):
        raise ValueError("main状态无效: messages需要完整对话轮次")
    turns = []
    index = 0
    while index < len(messages):
        start = index
        user = messages[index]
        if (
            not isinstance(user, dict)
            or set(user) != {"role", "content"}
            or user["role"] != "user"
            or not isinstance(user["content"], str)
        ):
            raise ValueError("main状态无效: messages需要user消息开始")
        index += 1
        while True:
            if index >= len(messages):
                raise ValueError("main状态无效: messages需要完整对话轮次")
            assistant = messages[index]
            if (
                not isinstance(assistant, dict)
                or assistant.get("role") != "assistant"
                or not isinstance(assistant.get("content"), str)
            ):
                raise ValueError("main状态无效: messages需要assistant消息")
            index += 1
            if set(assistant) == {"role", "content"}:
                turns.append(deepcopy(messages[start:index]))
                break
            if set(assistant) != {"role", "content", "tool_calls"}:
                raise ValueError("main状态无效: messages的assistant字段")
            try:
                calls = _tool_calls(assistant["tool_calls"])
            except ValueError as error:
                raise ValueError(f"main状态无效: messages {error}") from error
            for call in calls:
                if index >= len(messages):
                    raise ValueError("main状态无效: messages缺少工具结果")
                tool = messages[index]
                if (
                    not isinstance(tool, dict)
                    or set(tool) != {"role", "tool_call_id", "content"}
                    or tool["role"] != "tool"
                    or tool["tool_call_id"] != call["id"]
                    or not isinstance(tool["content"], str)
                ):
                    raise ValueError("main状态无效: messages工具结果不匹配")
                index += 1
    return turns


def _history(state: JSONObject) -> list[JSONObject]:
    return [message for turn in _turns(state.get("messages", [])) for message in turn]


def _trim_history(messages: list[JSONObject]) -> list[JSONObject]:
    turns = _turns(messages)[-MAX_HISTORY_TURNS:]
    while turns:
        history = [message for turn in turns for message in turn]
        if _json_size(history) <= MAX_HISTORY_BYTES:
            return history
        turns.pop(0)
    return []


def _tool_message(call: JSONObject, result: JSONObject) -> JSONObject:
    return {"role": "tool", "tool_call_id": call["id"], "content": _json_text(result)}


def _continue_state(state: JSONObject, history: list[JSONObject], legacy: bool) -> JSONObject:
    update = initial_state() | deepcopy(state)
    update["messages"] = history
    if legacy:
        update["pending_messages"] = []
        update["pending_tool_calls"] = []
        update["tool_rounds"] = 0
    elif "pending_messages" not in state:
        update["pending_messages"] = [{"role": "user", "content": state["pending_message"]}]
    return update


class MainAgent:
    def __init__(
        self, prompt_builder: PromptBuilder | None = None, tools: ToolRegistry | None = None
    ):
        self.prompt_builder = prompt_builder
        self.tools = default_tools() if tools is None else tools

    def run(self, context: ExecutionContext) -> ExecutionResult:
        state = context.agent.state or initial_state()
        status = state.get("request_status")
        if status not in ("idle", "waiting", "succeeded", "failed"):
            raise ValueError("main状态无效: request_status")
        history = _history(state)
        legacy = "messages" not in state and "pending_message" not in state
        if status == "waiting":
            for key in ("request_event_id", "request_execution_id", "waiting_action_id"):
                if not isinstance(state.get(key), str) or not state[key]:
                    raise ValueError(f"main状态无效: {key}")
            if not legacy and "pending_messages" not in state and not isinstance(state.get("pending_message"), str):
                raise ValueError("main状态无效: pending_messages")
            if not legacy and "waiting_action_type" in state:
                if state["waiting_action_type"] != "model.generate" and not self.tools.supports(state["waiting_action_type"]):
                    raise ValueError("main状态无效: waiting_action_type")
                if not isinstance(state.get("waiting_execution_id"), str) or not state["waiting_execution_id"]:
                    raise ValueError("main状态无效: waiting_execution_id")
                self._validate_pending(state)
        elif state.get("pending_message") is not None or state.get("pending_messages"):
            raise ValueError("main状态无效: pending_messages")
        match context.event.type:
            case "main.request":
                result = self._on_request(context, state, history)
            case "action.result":
                result = self._on_result(context, state, history, legacy)
            case "action.resolution":
                result = self._on_resolution(context, state, history, legacy)
            case _:
                raise BusinessError(f"不支持的事件类型: {context.event.type}")
        if "pending_message" in state:
            result.state_update["pending_message"] = None
        return result

    def _validate_pending(self, state: JSONObject) -> None:
        pending = state.get("pending_messages")
        queue = state.get("pending_tool_calls")
        rounds = state.get("tool_rounds")
        if (
            not isinstance(pending, list)
            or not pending
            or not isinstance(queue, list)
            or type(rounds) is not int
            or not 0 <= rounds <= MAX_TOOL_ROUNDS
        ):
            raise ValueError("main状态无效: pending_messages/tool_rounds")
        if queue:
            try:
                _tool_calls(queue)
            except ValueError as error:
                raise ValueError(f"main状态无效: pending_tool_calls {error}") from error
        if self.tools.supports(state["waiting_action_type"]) != bool(queue):
            raise ValueError("main状态无效: pending_tool_calls")
        if queue and self.tools.action_type(queue[0]["function"]["name"]) != state["waiting_action_type"]:
            raise ValueError("main状态无效: waiting_action_type与待处理工具不匹配")
        # 通过临时补齐剩余工具回复和最终回复，复用完整历史验证。
        completed = deepcopy(pending)
        completed.extend(_tool_message(call, {}) for call in queue)
        completed.append({"role": "assistant", "content": ""})
        _turns(completed)
        if state.get("pending_message") is not None and pending[0]["content"] != state["pending_message"]:
            raise ValueError("main状态无效: pending_messages与旧pending_message不匹配")

    def _on_request(
        self, context: ExecutionContext, state: JSONObject, history: list[JSONObject]
    ) -> ExecutionResult:
        if state["request_status"] == "waiting":
            raise BusinessError("main正忙，请稍后再试")
        payload = context.event.payload
        if not isinstance(payload, dict) or not isinstance(payload.get("message"), str):
            raise BusinessError("消息须为文本")
        message = payload["message"]
        if _json_size(message) > MAX_MESSAGE_BYTES:
            raise BusinessError("消息超过64KiB")
        update = initial_state()
        update.update(
            messages=_trim_history(history),
            pending_messages=[{"role": "user", "content": message}],
            request_event_id=context.event.id,
            request_execution_id=context.execution_id,
        )
        return self._model_action(context, update)

    def _model_action(self, context: ExecutionContext, update: JSONObject) -> ExecutionResult:
        if _json_size(update["pending_messages"]) > MAX_PENDING_BYTES:
            return self._finish(context, update, error="本轮工具记录超过128KiB")
        builder = self._prompt_builder(context)
        try:
            messages = builder.build(_turns(update["messages"]), update["pending_messages"])
        except PromptTooLong as error:
            return self._finish(context, update, error=str(error))
        payload = {"messages": messages}
        tools = self.tools.definitions()
        if tools:
            payload["tools"] = tools
        action = Action(id=str(uuid4()), type="model.generate", payload=payload)
        update.update(
            request_status="waiting",
            waiting_action_id=action.id,
            waiting_action_type=action.type,
            waiting_execution_id=context.execution_id,
        )
        return ExecutionResult(state_update=deepcopy(update), actions=[action])

    def _prompt_builder(self, context: ExecutionContext) -> PromptBuilder:
        return self.prompt_builder or PromptBuilder.from_env()

    def _on_resolution(
        self, context: ExecutionContext, state: JSONObject, history: list[JSONObject], legacy: bool
    ) -> ExecutionResult:
        payload = context.event.payload
        if state["request_status"] != "waiting":
            raise BusinessError("main没有待处理操作")
        action_type = state.get("waiting_action_type") or "model.generate"
        execution_id = state.get("waiting_execution_id") or state["request_execution_id"]
        is_tool = self.tools.supports(action_type)
        if not isinstance(payload, dict) or (
            (not is_tool and action_type != "model.generate")
            or payload.get("action_id") != state["waiting_action_id"]
            or payload.get("execution_id") != execution_id
            or payload.get("action_type") != action_type
        ):
            raise BusinessError("action.resolution与main等待中的操作不匹配")
        decision = payload.get("decision")
        if decision not in ("retry", "abandon"):
            raise BusinessError("请选择retry或abandon")
        reason = payload.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise BusinessError("请填写处理原因")
        update = _continue_state(state, history, legacy)
        if decision == "abandon":
            return self._finish(context, update, error="本轮已放弃: " + reason)
        retry_payload = payload.get("retry_payload")
        if is_tool:
            if not isinstance(retry_payload, dict):
                raise BusinessError("重试所需工具参数缺失或无效")
            retry = Action(id=str(uuid4()), type=action_type, payload=deepcopy(retry_payload))
            update.update(
                waiting_action_id=retry.id,
                waiting_action_type=retry.type,
                waiting_execution_id=context.execution_id,
                result_event_id=None,
                result=None,
                error=None,
            )
            return ExecutionResult(state_update=deepcopy(update), actions=[retry])
        retry_payload = payload.get("retry_payload")
        messages = retry_payload.get("messages") if isinstance(retry_payload, dict) else None
        if not isinstance(messages, list) or not messages:
            raise BusinessError("重试所需消息缺失或无效")
        for message in messages:
            if not isinstance(message, dict) or message.get("role") not in (
                "system", "developer", "user", "assistant", "tool"
            ) or not (
                isinstance(message.get("content"), str)
                or message.get("role") == "assistant"
                and message.get("content") is None
                and "tool_calls" in message
            ):
                raise BusinessError("重试消息格式无效")
        action = Action(
            id=str(uuid4()), type="model.generate",
            payload=deepcopy(retry_payload) | {"retry_of": state["waiting_action_id"]},
        )
        update.update(
            waiting_action_id=action.id,
            waiting_action_type=action.type,
            waiting_execution_id=context.execution_id,
            result_event_id=None,
            result=None,
            error=None,
        )
        if legacy:
            # 保留旧格式标记，后续结果仍按缺少原输入的兼容路径处理。
            del update["messages"]
        return ExecutionResult(state_update=deepcopy(update), actions=[action])

    def _on_result(
        self, context: ExecutionContext, state: JSONObject, history: list[JSONObject], legacy: bool
    ) -> ExecutionResult:
        payload = context.event.payload
        if state["request_status"] != "waiting":
            raise BusinessError("main没有待处理操作")
        action_type = state.get("waiting_action_type") or "model.generate"
        execution_id = state.get("waiting_execution_id") or state["request_execution_id"]
        if not isinstance(payload, dict) or (
            payload.get("action_id") != state["waiting_action_id"]
            or payload.get("execution_id") != execution_id
            or payload.get("action_type") != action_type
        ):
            raise BusinessError("action.result与main等待中的操作不匹配")
        status = payload.get("status")
        if status not in ("succeeded", "failed"):
            raise BusinessError("模型调用缺少最终结果")
        if status == "failed" and (
            not isinstance(payload.get("error"), str) or not payload["error"]
        ):
            raise BusinessError("模型调用失败但缺少错误信息")
        update = _continue_state(state, history, legacy)
        if self.tools.supports(action_type):
            call = update["pending_tool_calls"].pop(0)
            if status == "failed":
                tool_result = {"error": payload["error"]}
            else:
                tool_result = payload.get("result")
                if not isinstance(tool_result, dict):
                    raise BusinessError("工具结果须为JSON对象")
                if _json_size(tool_result) > MAX_TOOL_RESULT_BYTES:
                    tool_result = {"error": "工具结果超过8KiB"}
            update["pending_messages"].append(_tool_message(call, tool_result))
            return self._next_tool(context, update)
        if status == "failed":
            return self._finish(context, update, error=payload["error"])
        result = payload.get("result")
        if not isinstance(result, dict):
            raise BusinessError("模型回复须为文本")
        calls = result.get("tool_calls")
        if calls:
            if not isinstance(result.get("message", ""), str):
                raise BusinessError("模型回复须为文本")
            if legacy:
                return self._finish(context, update, error="旧请求缺少工具调用所需消息")
            if update["tool_rounds"] >= MAX_TOOL_ROUNDS:
                return self._finish(context, update, error="工具调用超过4轮")
            try:
                calls = _tool_calls(calls)
            except ValueError as error:
                return self._finish(context, update, error=str(error))
            update["tool_rounds"] += 1
            update["pending_messages"].append({
                "role": "assistant",
                "content": result.get("message", ""),
                "tool_calls": deepcopy(calls),
            })
            update["pending_tool_calls"] = calls
            return self._next_tool(context, update)
        if "tool_calls" in result and calls not in (None, []):
            return self._finish(context, update, error="工具调用格式无效")
        if not isinstance(result.get("message"), str):
            raise BusinessError("模型回复须为文本")
        return self._finish(context, update, result=result["message"])

    def _next_tool(self, context: ExecutionContext, update: JSONObject) -> ExecutionResult:
        while update["pending_tool_calls"]:
            if _json_size(update["pending_messages"]) > MAX_PENDING_BYTES:
                return self._finish(context, update, error="本轮工具记录超过128KiB")
            call = update["pending_tool_calls"][0]
            try:
                action = self.tools.create_action(call, context)
            except ValueError as error:
                update["pending_tool_calls"].pop(0)
                update["pending_messages"].append(_tool_message(call, {"error": str(error)}))
                continue
            update.update(
                request_status="waiting",
                waiting_action_id=action.id,
                waiting_action_type=action.type,
                waiting_execution_id=context.execution_id,
            )
            return ExecutionResult(state_update=deepcopy(update), actions=[action])
        return self._model_action(context, update)

    def _finish(
        self, context: ExecutionContext, update: JSONObject, *, result: str | None = None,
        error: str | None = None,
    ) -> ExecutionResult:
        if error is None:
            pending = update["pending_messages"]
            if pending:
                update["messages"] = _trim_history(update["messages"] + pending + [
                    {"role": "assistant", "content": result},
                ])
        update.update(
            request_status="failed" if error is not None else "succeeded",
            result=result,
            error=error,
            pending_messages=[],
            pending_tool_calls=[],
            tool_rounds=0,
            waiting_action_id=None,
            waiting_action_type=None,
            waiting_execution_id=None,
            result_event_id=context.event.id,
        )
        return ExecutionResult(state_update=deepcopy(update))
