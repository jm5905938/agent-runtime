"""查询Agent状态的工具定义，查询由go runtime执行。"""

from ..agent import ExecutionContext, JSONObject
from ..tool import Tool


def _prepare(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if arguments.keys() - {"agent_id"}:
        raise ValueError("agent_status只接受可选的agent_id参数")
    target = arguments.get("agent_id", context.agent.id)
    if not isinstance(target, str) or not target.strip():
        raise ValueError("agent_id需要非空字符串")
    try:
        target_bytes = target.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("agent_id需要有效UTF-8") from error
    if len(target_bytes) > 1024:
        raise ValueError("agent_id超过1024字节")
    return {"agent_id": target}


AGENT_STATUS = Tool(
    name="agent_status",
    description="查询一个Agent的状态；省略agent_id时查询当前Agent。",
    parameters={
        "type": "object",
        "properties": {"agent_id": {"type": "string"}},
        "additionalProperties": False,
    },
    prepare=_prepare,
)
