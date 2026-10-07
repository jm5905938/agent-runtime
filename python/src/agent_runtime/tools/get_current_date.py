"""获取当前UTC日期的工具定义。"""

from ..agent import ExecutionContext, JSONObject
from ..tool import Tool


def _prepare(arguments: JSONObject, context: ExecutionContext) -> JSONObject:
    if arguments:
        raise ValueError("get_current_date不接受参数")
    return {}


GET_CURRENT_DATE = Tool(
    name="get_current_date",
    description="获取当前UTC日期",
    parameters={
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    prepare=_prepare,
)
