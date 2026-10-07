"""查询Agent状态的工具定义，查询由go runtime执行。"""

from ..agent import ExecutionContext, JSONObject
from ..tool import Tool


def _prepare(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if arguments.keys() - {"agent_id"}:
        raise ValueError("仅支持可选参数agent_id")
    target = arguments.get("agent_id", context.agent.id)
    if not isinstance(target, str) or not target.strip():
        raise ValueError("agent_id须为非空文本")
    try:
        target_bytes = target.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("agent_id须为有效UTF-8文本") from error
    if len(target_bytes) > 1024:
        raise ValueError("agent_id超过1024字节")
    return {"agent_id": target}


AGENT_STATUS = Tool(
    name="agent_status",
    description="查询agent状态，省略agent_id则查询当前agent",
    parameters={
        "type": "object",
        "properties": {"agent_id": {"type": "string"}},
        "additionalProperties": False,
    },
    prepare=_prepare,
)
