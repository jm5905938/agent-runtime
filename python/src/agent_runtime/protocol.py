"""json lines协议，不把json数字转成有损浮点数"""

import json
import math
import re
from datetime import datetime
from decimal import Decimal

from .agent import (
    Action,
    AgentSnapshot,
    DefinitionRef,
    Event,
    ExecutionContext,
    ExecutionResult,
)

VERSION = 1
MAX_FRAME_BYTES = 1 << 20


class ProtocolError(ValueError):
    pass


def validate_json(value: object, active: set[int] | None = None) -> None:
    if value is None or type(value) in (bool, int):
        return
    if type(value) is str:
        value.encode("utf-8", errors="strict")
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ProtocolError("json数字不能为无穷大或非数字")
        return
    if type(value) is Decimal:
        if not value.is_finite():
            raise ProtocolError("json数字不能为无穷大或非数字")
        return
    if type(value) not in (dict, list):
        raise ProtocolError(f"不支持的json值类型: {type(value).__name__}")
    if active is None:
        active = set()
    if id(value) in active:
        raise ProtocolError("json值存在循环引用")
    active.add(id(value))
    try:
        if type(value) is dict:
            for key, item in value.items():
                if type(key) is not str:
                    raise ProtocolError("json对象键必须是字符串")
                validate_json(key, active)
                validate_json(item, active)
        else:
            for item in value:
                validate_json(item, active)
    finally:
        active.remove(id(value))


def _encode(value: object) -> str:
    if type(value) is Decimal:
        return str(value)
    if type(value) is dict:
        return (
            "{"
            + ",".join(
                json.dumps(key, ensure_ascii=False) + ":" + _encode(item)
                for key, item in value.items()
            )
            + "}"
        )
    if type(value) is list:
        return "[" + ",".join(_encode(item) for item in value) + "]"
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def encode_frame(value: object) -> bytes:
    validate_json(value)
    frame = (_encode(value) + "\n").encode("utf-8")
    if len(frame) > MAX_FRAME_BYTES:
        raise ProtocolError("响应超过最大帧长度")
    return frame


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError("json对象键重复")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ProtocolError(f"无效的json常量: {value}")


def decode_frame(frame: bytes) -> object:
    if len(frame) > MAX_FRAME_BYTES or not frame.endswith(b"\n"):
        raise ProtocolError("请求超过最大帧长度或缺少换行符")
    value = json.loads(
        frame.decode("utf-8", errors="strict"),
        parse_float=Decimal,
        parse_constant=_invalid_constant,
        object_pairs_hook=_unique_object,
    )
    validate_json(value)
    return value


def _object(
    value: object, label: str, required: set[str], optional: set[str] = frozenset()
) -> dict:
    if (
        type(value) is not dict
        or not required <= value.keys()
        or value.keys() - required - optional
    ):
        raise ProtocolError(f"{label}字段无效")
    return value


def _text(value: object, label: str, *, empty: bool = False) -> str:
    if type(value) is not str or (not empty and not value):
        raise ProtocolError(f"{label}无效")
    return value


def _data_object(value: object, label: str) -> dict | None:
    if value is not None and type(value) is not dict:
        raise ProtocolError(f"{label}必须是对象或null")
    return value


def parse_request(value: object) -> tuple[str, ExecutionContext]:
    request = _object(value, "request", {"version", "id", "context"})
    if type(request["version"]) is not int or request["version"] != VERSION:
        raise ProtocolError("不支持的协议版本")
    request_id = _text(request["id"], "请求id")
    source = _object(
        request["context"], "context", {"agent", "event", "execution_id", "attempt_id"}
    )
    agent = _object(
        source["agent"],
        "agent",
        {"id", "name", "definition", "status", "state", "state_version"},
        {"binding_error"},
    )
    definition = _object(agent["definition"], "definition", {"id", "version"})
    event = _object(source["event"], "event", {"id", "type", "payload", "created_at"})
    state_version = agent["state_version"]
    if type(state_version) is not int or not 0 <= state_version < 1 << 64:
        raise ProtocolError("state_version无效")
    if agent["status"] not in (
        "created",
        "active",
        "paused",
        "terminating",
        "terminated",
    ):
        raise ProtocolError("agent状态无效")
    created_at = _text(event["created_at"], "event.created_at")
    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})",
        created_at,
    ):
        raise ProtocolError("event.created_at必须符合rfc 3339")
    try:
        datetime.fromisoformat(created_at)
    except ValueError as error:
        raise ProtocolError("event.created_at无效") from error
    context = ExecutionContext(
        agent=AgentSnapshot(
            id=_text(agent["id"], "agent.id"),
            name=_text(agent["name"], "agent.name", empty=True),
            definition=DefinitionRef(
                _text(definition["id"], "definition.id"),
                _text(definition["version"], "definition.version"),
            ),
            status=agent["status"],
            state=_data_object(agent["state"], "agent.state"),
            state_version=state_version,
            binding_error=_text(
                agent["binding_error"], "agent.binding_error", empty=True
            )
            if "binding_error" in agent
            else None,
        ),
        event=Event(
            id=_text(event["id"], "event.id"),
            type=_text(event["type"], "event.type"),
            payload=_data_object(event["payload"], "event.payload"),
            created_at=created_at,
        ),
        execution_id=_text(source["execution_id"], "execution_id"),
        attempt_id=_text(source["attempt_id"], "attempt_id"),
    )
    if request_id != context.attempt_id:
        raise ProtocolError("请求id必须与attempt_id一致")
    return request_id, context


def result_to_wire(result: ExecutionResult, context: ExecutionContext) -> dict:
    if type(result) is not ExecutionResult or type(result.actions) is not list:
        raise ProtocolError("runner必须返回包含actions列表的ExecutionResult")
    if type(result.state_update) is not dict:
        raise ProtocolError("state_update必须是对象")
    actions = []
    action_ids = set()
    for action in result.actions:
        if type(action) is not Action:
            raise ProtocolError("结果actions必须包含Action对象")
        _text(action.id, "action.id")
        _text(action.type, "action.type")
        if type(action.payload) is not dict:
            raise ProtocolError("action.payload必须是对象")
        if action.id in action_ids:
            raise ProtocolError("action id重复")
        action_ids.add(action.id)
        record = {"id": action.id, "type": action.type, "payload": action.payload}
        if action.execution_id is not None:
            if action.execution_id != context.execution_id:
                raise ProtocolError("action execution_id与上下文不一致")
            record["execution_id"] = action.execution_id
        actions.append(record)
    wire = {"state_update": result.state_update, "actions": actions}
    validate_json(wire)
    return wire
