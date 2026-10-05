"""工具定义、参数处理与Action映射，外部操作由go Executor执行。"""

from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
import json
import re
from uuid import uuid4

from .agent import Action, ExecutionContext, JSONObject
from .prompt import json_size
from .protocol import validate_json


MAX_TOOL_CALLS = 8
MAX_TOOL_CALL_BYTES = 32 * 1024
MAX_TOOL_CALL_ID_BYTES = 128
MAX_TOOL_NAME_BYTES = 64
MAX_TOOL_RESULT_BYTES = 8 * 1024


def _valid_name(name: object) -> bool:
    return (
        isinstance(name, str)
        and 0 < len(name) <= MAX_TOOL_NAME_BYTES
        and re.fullmatch(r"[A-Za-z0-9_-]+", name) is not None
    )


def validate_tool_calls(value: object) -> list[JSONObject]:
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_TOOL_CALLS:
        raise ValueError("工具调用数量需要在1到8之间")
    if json_size(value) > MAX_TOOL_CALL_BYTES:
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
            or not _valid_name(function["name"])
            or not isinstance(function["arguments"], str)
        ):
            raise ValueError("工具调用function格式无效")
        seen.add(call["id"])
    return deepcopy(value)


def _arguments(value: str) -> JSONObject:
    def unique_object(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("工具参数包含重复字段")
            result[key] = item
        return result

    def invalid_constant(value):
        raise ValueError("工具参数包含非法数字")

    try:
        arguments = json.loads(
            value,
            parse_float=Decimal,
            object_pairs_hook=unique_object,
            parse_constant=invalid_constant,
        )
        if not isinstance(arguments, dict):
            raise ValueError("工具参数需要JSON对象")
        validate_json(arguments)
    except (ValueError, RecursionError) as error:
        raise ValueError("工具参数需要有效JSON对象") from error
    return arguments


@dataclass(frozen=True)
class Tool:
    """parameters提供模型声明，prepare校验参数并补齐Action所需的上下文。"""

    name: str
    description: str
    parameters: JSONObject
    prepare: Callable[[JSONObject, ExecutionContext], JSONObject]

    def __post_init__(self) -> None:
        if not _valid_name(self.name):
            raise ValueError("工具名称需要1至64个字母、数字、下划线或连字符")
        if not isinstance(self.description, str):
            raise ValueError("工具description需要字符串")
        if not isinstance(self.parameters, dict) or self.parameters.get("type") != "object":
            raise ValueError("工具parameters需要object类型JSON Schema")
        validate_json(self.description)
        validate_json(self.parameters)
        if not callable(self.prepare):
            raise ValueError("工具prepare需要参数处理函数")

    @property
    def action_type(self) -> str:
        return "tool." + self.name

    def definition(self) -> JSONObject:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": deepcopy(self.parameters),
            },
        }


class ToolRegistry:
    """初始化时固定可用工具，声明与分发使用同一份定义。"""

    def __init__(self, tools: Iterable[Tool] = ()):
        self._tools = {}
        for tool in tools:
            if tool.name in self._tools:
                raise ValueError("工具名称重复: " + tool.name)
            self._tools[tool.name] = deepcopy(tool)
        self._action_types = frozenset(tool.action_type for tool in self._tools.values())

    def definitions(self) -> list[JSONObject]:
        return [tool.definition() for tool in self._tools.values()]

    def supports(self, action_type: object) -> bool:
        return isinstance(action_type, str) and action_type in self._action_types

    def action_type(self, name: str) -> str:
        return self._tool(name).action_type

    def _tool(self, name: str) -> Tool:
        tool = self._tools.get(name)
        if tool is None:
            raise ValueError("不支持的工具: " + name)
        return tool

    def create_action(self, call: JSONObject, context: ExecutionContext) -> Action:
        call = validate_tool_calls([call])[0]
        tool = self._tool(call["function"]["name"])
        arguments = _arguments(call["function"]["arguments"])
        payload = tool.prepare(arguments, context)
        if not isinstance(payload, dict):
            raise TypeError("工具prepare需要返回JSON对象")
        validate_json(payload)
        return Action(id=str(uuid4()), type=tool.action_type, payload=deepcopy(payload))
