"""在项目源码目录创建UTF-8文本文件的工具定义。"""

from ..agent import ExecutionContext, JSONObject
from ..tool import Tool


def _prepare(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if set(arguments) != {"path", "content"}:
        raise ValueError("仅支持path和content参数")

    path = arguments["path"]
    content = arguments["content"]

    if not isinstance(path, str) or not path.strip():
        raise ValueError("path须为非空文本")
    if not isinstance(content, str):
        raise ValueError("content须为文本")
    if len(content.encode("utf-8")) > 4 * 1024:
        raise ValueError("文件内容超过4KiB")

    return {"path": path, "content": content}


WRITE_FILE = Tool(
    name="write_file",
    description="在源码目录新建UTF-8文本文件，最大4KiB，不覆盖已有文件",
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
