"""读取项目目录中文本文件的工具定义"""

from ..agent import ExecutionContext, JSONObject
from ..tool import Tool


def _prepare(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if set(arguments) != {"path"}:
        raise ValueError("仅支持path参数")

    path = arguments["path"]
    if not isinstance(path, str) or not path.strip():
        raise ValueError("path须为非空文本")

    return {"path": path}


READ_FILE = Tool(
    name="read_file",
    description="读取项目内的UTF-8文本文件，path相对项目根目录，最大6KiB",
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
