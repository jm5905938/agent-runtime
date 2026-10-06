"""读取项目源码目录中文本文件的工具定义。"""

from ..agent import ExecutionContext, JSONObject
from ..tool import Tool


def _prepare(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if set(arguments) != {"path"}:
        raise ValueError("read_file需要path参数，且不接受其他参数")

    path = arguments["path"]
    if not isinstance(path, str) or not path.strip():
        raise ValueError("read_file的path需要非空字符串")

    return {"path": path}


READ_FILE = Tool(
    name="read_file",
    description="读取项目源码目录中的UTF-8文本文件。",
    parameters={
        "type": "object",
        "properties": {
            "path": {"type": "string"},
        },
        "required": ["path"],
        "additionalProperties": False,
    },
    prepare=_prepare,
)
