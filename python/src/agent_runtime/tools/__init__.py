"""MainAgent默认可用工具。"""

from ..tool import ToolRegistry
from .agent_status import AGENT_STATUS
from .get_current_time import GET_CURRENT_TIME
from .get_current_date import GET_CURRENT_DATE
from .read_file import READ_FILE
from .write_file import WRITE_FILE


def default_tools() -> ToolRegistry:
    return ToolRegistry([
        AGENT_STATUS,
        GET_CURRENT_TIME,
        GET_CURRENT_DATE,
        READ_FILE,
        WRITE_FILE,
    ])
