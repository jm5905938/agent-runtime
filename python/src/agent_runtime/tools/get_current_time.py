"""获取当前UTC时间的工具定义。"""

from ..agent import ExecutionContext, JSONObject
from ..tool import Tool


def _prepare(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if arguments:
        raise ValueError("get_current_time不接受参数")
    return {}


GET_CURRENT_TIME = Tool(
    name="get_current_time",
    description="获取当前UTC时间",
    parameters={
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    prepare=_prepare,
)
