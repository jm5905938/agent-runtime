"""一次性subagent工具，任务关系与等待由go runtime保存"""

from ..agent import ExecutionContext, JSONObject
from ..tool import Tool


def _spawn(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if arguments.keys() - {"message", "context", "name"}:
        raise ValueError("仅支持message、context和name参数")
    message = arguments.get("message")
    if not isinstance(message, str) or not message.strip():
        raise ValueError("message须为非空文本")
    for key in ("context", "name"):
        if key in arguments and not isinstance(arguments[key], str):
            raise ValueError(f"{key}须为文本")
    return arguments


def _task(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if set(arguments) != {"task_id"}:
        raise ValueError("仅支持task_id参数")
    task_id = arguments["task_id"]
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError("task_id须为非空文本")
    return arguments


SPAWN_SUBAGENT = Tool(
    name="spawn_subagent",
    description="创建独立subagent任务，返回task_id和child_id，完成结果需另行等待",
    parameters={
        "type": "object",
        "properties": {
            "message": {"type": "string", "minLength": 1},
            "context": {"type": "string"},
            "name": {"type": "string"},
        },
        "required": ["message"],
        "additionalProperties": False,
    },
    prepare=_spawn,
)

WAIT_SUBAGENT = Tool(
    name="wait_subagent",
    description="等待subagent任务结束，返回结果和task_status",
    parameters={
        "type": "object",
        "properties": {"task_id": {"type": "string", "minLength": 1}},
        "required": ["task_id"],
        "additionalProperties": False,
    },
    prepare=_task,
)

CANCEL_SUBAGENT = Tool(
    name="cancel_subagent",
    description="请求取消subagent任务，用wait_subagent确认结果",
    parameters={
        "type": "object",
        "properties": {"task_id": {"type": "string", "minLength": 1}},
        "required": ["task_id"],
        "additionalProperties": False,
    },
    prepare=_task,
)
