"""一次性subagent工具，任务关系与等待由go runtime保存"""

from ..agent import ExecutionContext, JSONObject
from ..tool import Tool


def _spawn(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if arguments.keys() - {"message", "context", "name"}:
        raise ValueError("spawn_subagent只接受message、context和name参数")
    message = arguments.get("message")
    if not isinstance(message, str) or not message.strip():
        raise ValueError("spawn_subagent需要非空message")
    for key in ("context", "name"):
        if key in arguments and not isinstance(arguments[key], str):
            raise ValueError(f"spawn_subagent的{key}需要字符串")
    return arguments


def _task(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if set(arguments) != {"task_id"}:
        raise ValueError("subagent工具只接受task_id参数")
    task_id = arguments["task_id"]
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError("task_id需要非空字符串")
    return arguments


SPAWN_SUBAGENT = Tool(
    name="spawn_subagent",
    description="创建独立上下文的一次性subagent任务，返回task_id和child_id，接受任务不代表任务完成",
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
    description="等待一次性subagent任务的最终结果，任务失败和取消也会返回task_status",
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
    description="请求取消一次性subagent任务，通过wait_subagent确认最终结果",
    parameters={
        "type": "object",
        "properties": {"task_id": {"type": "string", "minLength": 1}},
        "required": ["task_id"],
        "additionalProperties": False,
    },
    prepare=_task,
)
