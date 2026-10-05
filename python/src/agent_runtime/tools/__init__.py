"""MainAgent默认可用工具。"""

from ..tool import ToolRegistry
from .agent_status import AGENT_STATUS


def default_tools() -> ToolRegistry:
    return ToolRegistry([AGENT_STATUS])
