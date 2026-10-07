"""在项目源码目录创建UTF-8文本文件的工具定义。"""

from ..agent import ExecutionContext, JSONObject
from ..tool import Tool


def _prepare(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if set(arguments) != {"path", "content"}:
        raise ValueError("write_file需要path和content参数，且不接受其他参数")

    path = arguments["path"]
    content = arguments["content"]

    if not isinstance(path, str) or not path.strip():
        raise ValueError("write_file的path需要非空字符串")
    if not isinstance(content, str):
        raise ValueError("write_file的content需要字符串")
    if len(content.encode("utf-8")) > 4 * 1024:
        raise ValueError("write_file的content超过4KiB")

    return {"path": path, "content": content}


WRITE_FILE = Tool(
    name="write_file",
    description="在项目源码目录创建UTF-8文本文件，内容最大4KiB；已有文件不会被覆盖。",
    parameters={
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "content": {"type": "string"},
        },
        "required": ["path", "content"],
        "additionalProperties": False,
    },
    prepare=_prepare,
)
