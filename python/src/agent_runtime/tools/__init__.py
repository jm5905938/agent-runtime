"""MainAgent默认可用工具。"""

from ..tool import ToolRegistry
from .agent_status import AGENT_STATUS
from .get_current_time import GET_CURRENT_TIME


def default_tools() -> ToolRegistry:
    return ToolRegistry([AGENT_STATUS, GET_CURRENT_TIME])
