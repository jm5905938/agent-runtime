"""MainAgent默认可用工具。"""

from ..tool import ToolRegistry
from .agent_status import AGENT_STATUS
from .subagent import CANCEL_SUBAGENT, SPAWN_SUBAGENT, WAIT_SUBAGENT


def default_tools() -> ToolRegistry:
    return ToolRegistry([AGENT_STATUS, SPAWN_SUBAGENT, WAIT_SUBAGENT, CANCEL_SUBAGENT])


def subagent_tools() -> ToolRegistry:
    return ToolRegistry([AGENT_STATUS])
