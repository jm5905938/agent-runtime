"""持久化完整对话和工具调用轨迹，每个实例同时等待一个Action。"""

from copy import deepcopy
import json
import re
from uuid import uuid4

from ..agent import Action, BusinessError, ExecutionContext, ExecutionResult, JSONObject
from ..prompt import (
    MAX_PROMPT_BYTES,
    PromptBuilder,
    PromptTooLong,
    json_size as _json_size,
    json_text as _json_text,
)


MAX_HISTORY_TURNS = 20
MAX_HISTORY_BYTES = MAX_PROMPT_BYTES
MAX_MESSAGE_BYTES = 64 * 1024
MAX_PENDING_BYTES = 128 * 1024
MAX_TOOL_CALLS = 8
MAX_TOOL_CALL_BYTES = 32 * 1024
MAX_TOOL_CALL_ID_BYTES = 128
MAX_TOOL_NAME_BYTES = 64
MAX_TOOL_ROUNDS = 4
MAX_TOOL_RESULT_BYTES = 8 * 1024

TOOLS = [{
    "type": "function",
    "function": {
        "name": "agent_status",
        "description": "查询一个Agent的状态；省略agent_id时查询当前Agent。",
        "parameters": {
            "type": "object",
            "properties": {"agent_id": {"type": "string"}},
            "additionalProperties": False,
        },
    },
}]


def initial_state() -> JSONObject:
    return {
        "messages": [],
        "pending_message": None,
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


def _tool_calls(value: object) -> list[JSONObject]:
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_TOOL_CALLS:
        raise ValueError("工具调用数量需要在1到8之间")
    if _json_size(value) > MAX_TOOL_CALL_BYTES:
        raise ValueError("工具调用超过32KiB")
    seen = set()
    for call in value:
        if (
            not isinstance(call, dict)
            or set(call) != {"id", "type", "function"}
            or not isinstance(call["id"], str)
            or not call["id"].strip()
            or len(call["id"].encode("utf-8")) > MAX_TOOL_CALL_ID_BYTES
            or call["id"] in seen
            or call["type"] != "function"
        ):
            raise ValueError("工具调用格式无效")
        function = call["function"]
        if (
            not isinstance(function, dict)
            or set(function) != {"name", "arguments"}
            or not isinstance(function["name"], str)
            or not function["name"]
            or len(function["name"].encode("utf-8")) > MAX_TOOL_NAME_BYTES
            or re.fullmatch(r"[A-Za-z0-9_-]+", function["name"]) is None
            or not isinstance(function["arguments"], str)
        ):
            raise ValueError("工具调用function格式无效")
        seen.add(call["id"])
    return deepcopy(value)


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


def _arguments(call: JSONObject, agent_id: str) -> JSONObject:
    if call["function"]["name"] != "agent_status":
        raise ValueError("不支持的工具: " + call["function"]["name"])

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("工具参数包含重复字段")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("工具参数包含非法数字")

    try:
        arguments = json.loads(
            call["function"]["arguments"],
            object_pairs_hook=unique_object,
            parse_constant=invalid_constant,
        )
    except (ValueError, RecursionError) as error:
        raise ValueError("工具参数需要JSON对象") from error
    if not isinstance(arguments, dict) or arguments.keys() - {"agent_id"}:
        raise ValueError("agent_status只接受可选的agent_id参数")
    target = arguments.get("agent_id", agent_id)
    if not isinstance(target, str) or not target.strip():
        raise ValueError("agent_id需要非空字符串")
    try:
        target_bytes = target.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("agent_id需要有效UTF-8") from error
    if len(target_bytes) > 1024:
        raise ValueError("agent_id超过1024字节")
    return {"agent_id": target}


class MainAgent:
    def __init__(self, prompt_builder: PromptBuilder | None = None):
        self.prompt_builder = prompt_builder

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
            if not legacy and not isinstance(state.get("pending_message"), str):
                raise ValueError("main状态无效: pending_message")
            if not legacy and "waiting_action_type" in state:
                if state["waiting_action_type"] not in ("model.generate", "tool.agent_status"):
                    raise ValueError("main状态无效: waiting_action_type")
                if not isinstance(state.get("waiting_execution_id"), str) or not state["waiting_execution_id"]:
                    raise ValueError("main状态无效: waiting_execution_id")
                self._validate_pending(state)
        elif state.get("pending_message") is not None:
            raise ValueError("main状态无效: pending_message")
        match context.event.type:
            case "main.request":
                return self._on_request(context, state, history)
            case "action.result":
                return self._on_result(context, state, history, legacy)
            case "action.resolution":
                return self._on_resolution(context, state, history, legacy)
            case _:
                raise BusinessError(f"不支持的事件类型: {context.event.type}")

    def _validate_pending(self, state: JSONObject) -> None:
        pending = state.get("pending_messages")
        queue = state.get("pending_tool_calls")
        rounds = state.get("tool_rounds")
        if (
            not isinstance(pending, list)
            or not pending
            or pending[0] != {"role": "user", "content": state["pending_message"]}
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
        if (state["waiting_action_type"] == "tool.agent_status") != bool(queue):
            raise ValueError("main状态无效: pending_tool_calls")
        # 通过临时补齐剩余工具回复和最终回复，复用完整历史验证。
        completed = deepcopy(pending)
        completed.extend(_tool_message(call, {}) for call in queue)
        completed.append({"role": "assistant", "content": ""})
        _turns(completed)

    def _on_request(
        self, context: ExecutionContext, state: JSONObject, history: list[JSONObject]
    ) -> ExecutionResult:
        if state["request_status"] == "waiting":
            raise BusinessError("main agent正忙")
        payload = context.event.payload
        if not isinstance(payload, dict) or not isinstance(payload.get("message"), str):
            raise BusinessError("main.request需要字符串消息")
        message = payload["message"]
        if _json_size(message) > MAX_MESSAGE_BYTES:
            raise BusinessError("main.request消息超过64KiB")
        update = initial_state()
        update.update(
            messages=_trim_history(history),
            pending_message=message,
            pending_messages=[{"role": "user", "content": message}],
            request_event_id=context.event.id,
            request_execution_id=context.execution_id,
        )
        return self._model_action(context, update)

    def _model_action(self, context: ExecutionContext, update: JSONObject) -> ExecutionResult:
        if _json_size(update["pending_messages"]) > MAX_PENDING_BYTES:
            return self._finish(context, update, error="本轮工具调用轨迹超过128KiB")
        builder = self.prompt_builder or PromptBuilder.from_env()
        try:
            messages = builder.build(_turns(update["messages"]), update["pending_messages"])
        except PromptTooLong as error:
            return self._finish(context, update, error=str(error))
        action = Action(
            id=str(uuid4()),
            type="model.generate",
            payload={
                "messages": messages,
                "tools": deepcopy(TOOLS),
            },
        )
        update.update(
            request_status="waiting",
            waiting_action_id=action.id,
            waiting_action_type=action.type,
            waiting_execution_id=context.execution_id,
        )
        return ExecutionResult(state_update=deepcopy(update), actions=[action])

    def _on_resolution(
        self, context: ExecutionContext, state: JSONObject, history: list[JSONObject], legacy: bool
    ) -> ExecutionResult:
        payload = context.event.payload
        if state["request_status"] != "waiting":
            raise BusinessError("main没有等待中的操作")
        action_type = state.get("waiting_action_type") or "model.generate"
        execution_id = state.get("waiting_execution_id") or state["request_execution_id"]
        if not isinstance(payload, dict) or (
            action_type != "model.generate"
            or payload.get("action_id") != state["waiting_action_id"]
            or payload.get("execution_id") != execution_id
            or payload.get("action_type") != action_type
        ):
            raise BusinessError("action.resolution与main等待中的模型操作不匹配")
        decision = payload.get("decision")
        if decision not in ("retry", "abandon"):
            raise BusinessError("action.resolution需要retry或abandon决定")
        reason = payload.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise BusinessError("action.resolution需要非空原因")
        update = initial_state() | deepcopy(state)
        update["messages"] = history
        if legacy:
            update["pending_message"] = None
            update["pending_messages"] = []
            update["pending_tool_calls"] = []
            update["tool_rounds"] = 0
        elif "pending_messages" not in state:
            update["pending_messages"] = [{"role": "user", "content": state["pending_message"]}]
        if decision == "abandon":
            return self._finish(context, update, error="用户放弃本轮: " + reason)
        retry_payload = payload.get("retry_payload")
        messages = retry_payload.get("messages") if isinstance(retry_payload, dict) else None
        if not isinstance(messages, list) or not messages:
            raise BusinessError("重试模型操作需要已保存的非空messages列表")
        for message in messages:
            if not isinstance(message, dict) or message.get("role") not in (
                "system", "developer", "user", "assistant", "tool"
            ) or not (
                isinstance(message.get("content"), str)
                or message.get("role") == "assistant"
                and message.get("content") is None
                and "tool_calls" in message
            ):
                raise BusinessError("重试模型操作的消息格式无效")
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
            del update["pending_message"]
        return ExecutionResult(state_update=deepcopy(update), actions=[action])

    def _on_result(
        self, context: ExecutionContext, state: JSONObject, history: list[JSONObject], legacy: bool
    ) -> ExecutionResult:
        payload = context.event.payload
        if state["request_status"] != "waiting":
            raise BusinessError("main没有等待中的操作")
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
            raise BusinessError("模型操作缺少最终结果")
        if status == "failed" and (
            not isinstance(payload.get("error"), str) or not payload["error"]
        ):
            raise BusinessError("失败的模型结果需要错误消息")
        update = initial_state() | deepcopy(state)
        update["messages"] = history
        if legacy:
            update["pending_message"] = None
            update["pending_messages"] = []
            update["pending_tool_calls"] = []
            update["tool_rounds"] = 0
        elif "pending_messages" not in state:
            update["pending_messages"] = [{"role": "user", "content": state["pending_message"]}]
        if action_type == "tool.agent_status":
            call = update["pending_tool_calls"].pop(0)
            if status == "failed":
                tool_result = {"error": payload["error"]}
            else:
                tool_result = payload.get("result")
                if not isinstance(tool_result, dict):
                    raise BusinessError("工具结果需要JSON对象")
                if _json_size(tool_result) > MAX_TOOL_RESULT_BYTES:
                    tool_result = {"error": "工具结果超过8KiB"}
            update["pending_messages"].append(_tool_message(call, tool_result))
            return self._next_tool(context, update)
        if status == "failed":
            return self._finish(context, update, error=payload["error"])
        result = payload.get("result")
        if not isinstance(result, dict):
            raise BusinessError("模型结果需要字符串消息")
        calls = result.get("tool_calls")
        if calls:
            if not isinstance(result.get("message", ""), str):
                raise BusinessError("模型结果需要字符串消息")
            if legacy:
                return self._finish(context, update, error="旧请求缺少工具调用所需的输入消息")
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
            raise BusinessError("模型结果需要字符串消息")
        return self._finish(context, update, result=result["message"])

    def _next_tool(self, context: ExecutionContext, update: JSONObject) -> ExecutionResult:
        while update["pending_tool_calls"]:
            if _json_size(update["pending_messages"]) > MAX_PENDING_BYTES:
                return self._finish(context, update, error="本轮工具调用轨迹超过128KiB")
            call = update["pending_tool_calls"][0]
            try:
                arguments = _arguments(call, context.agent.id)
            except ValueError as error:
                update["pending_tool_calls"].pop(0)
                update["pending_messages"].append(_tool_message(call, {"error": str(error)}))
                continue
            action = Action(id=str(uuid4()), type="tool.agent_status", payload=arguments)
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
            pending_message=None,
            pending_messages=[],
            pending_tool_calls=[],
            tool_rounds=0,
            waiting_action_id=None,
            waiting_action_type=None,
            waiting_execution_id=None,
            result_event_id=context.event.id,
        )
        return ExecutionResult(state_update=deepcopy(update))
