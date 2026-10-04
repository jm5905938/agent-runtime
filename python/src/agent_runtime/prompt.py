"""按整体预算组装模型消息，从最早的完整历史轮次开始裁剪。"""

from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
import json
import os
import re
import sys

from .agent import JSONObject
from .protocol import validate_json


DEFAULT_MAX_PROMPT_CHARS = 100_000
MAX_PROMPT_BYTES = 384 * 1024
MAX_SYSTEM_PROMPT_BYTES = 64 * 1024


def json_text(value: object) -> str:
    """保留协议中的无损Decimal数字。"""
    validate_json(value)

    def encode(item):
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, dict):
            return "{" + ",".join(
                json.dumps(key, ensure_ascii=False) + ":" + encode(child)
                for key, child in item.items()
            ) + "}"
        if isinstance(item, list):
            return "[" + ",".join(encode(child) for child in item) + "]"
        return json.dumps(item, ensure_ascii=False, allow_nan=False, separators=(",", ":"))

    return encode(value)


def json_size(value: object) -> int:
    """与Go的JSON编码一致，计入UTF-8、HTML和行分隔符转义。"""
    encoded = json_text(value)
    for character, escaped in (
        ("<", "\\u003c"), (">", "\\u003e"), ("&", "\\u0026"),
        ("\u2028", "\\u2028"), ("\u2029", "\\u2029"),
    ):
        encoded = encoded.replace(character, escaped)
    return len(encoded.encode("utf-8"))


def prompt_chars(messages: list[JSONObject]) -> int:
    """按Unicode字符计文本、工具调用JSON和工具结果ID，不估算token。"""
    total = 0
    for message in messages:
        total += len(message["content"])
        if "tool_calls" in message:
            total += len(json_text(message["tool_calls"]))
        if "tool_call_id" in message:
            total += len(message["tool_call_id"])
    return total


class PromptTooLong(ValueError):
    """固定system与本轮消息已经超限，不能通过裁剪历史解决。"""


@dataclass(frozen=True)
class PromptBuilder:
    system_prompt: str = ""
    max_chars: int = DEFAULT_MAX_PROMPT_CHARS

    def __post_init__(self) -> None:
        if not isinstance(self.system_prompt, str) or "\0" in self.system_prompt:
            raise ValueError("LLM_SYSTEM_PROMPT必须是有效UTF-8文本且不能包含NUL")
        try:
            system_bytes = json_size(self.system_prompt)
        except UnicodeEncodeError as error:
            raise ValueError("LLM_SYSTEM_PROMPT必须是有效UTF-8文本") from error
        if system_bytes > MAX_SYSTEM_PROMPT_BYTES:
            raise ValueError("LLM_SYSTEM_PROMPT超过64KiB")
        if type(self.max_chars) is not int or not 0 < self.max_chars <= sys.maxsize:
            raise ValueError("LLM_MAX_PROMPT_CHARS必须是正整数")

    @classmethod
    def from_env(cls) -> "PromptBuilder":
        value = os.environ.get("LLM_MAX_PROMPT_CHARS", "")
        if value and re.fullmatch(r"[0-9]+", value) is None:
            raise ValueError("LLM_MAX_PROMPT_CHARS必须是正整数")
        try:
            maximum = int(value) if value else DEFAULT_MAX_PROMPT_CHARS
        except ValueError as error:
            raise ValueError("LLM_MAX_PROMPT_CHARS必须是正整数") from error
        return cls(os.environ.get("LLM_SYSTEM_PROMPT", ""), maximum)

    def build(
        self, history_turns: list[list[JSONObject]], current_messages: list[JSONObject]
    ) -> list[JSONObject]:
        """history_turns已按时间排序且每项是一轮完整对话；本轮整体保留。"""
        if not current_messages:
            raise ValueError("prompt需要本轮消息")
        prefix = [{"role": "system", "content": self.system_prompt}] if self.system_prompt else []
        fixed = prefix + current_messages
        if prompt_chars(fixed) > self.max_chars:
            raise PromptTooLong("system与本轮消息超过LLM_MAX_PROMPT_CHARS限制")
        if json_size(fixed) > MAX_PROMPT_BYTES:
            raise PromptTooLong("system与本轮消息超过384KiB模型输入限制")
        for first in range(len(history_turns) + 1):
            history = [message for turn in history_turns[first:] for message in turn]
            messages = prefix + history + current_messages
            if prompt_chars(messages) <= self.max_chars and json_size(messages) <= MAX_PROMPT_BYTES:
                return deepcopy(messages)
        raise AssertionError("已验证本轮消息可放入prompt")
