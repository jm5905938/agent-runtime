from copy import deepcopy
import json
import os
import sys
import unittest
from unittest.mock import patch

from agent_runtime.prompt import (
    DEFAULT_MAX_PROMPT_CHARS,
    MAX_SYSTEM_PROMPT_BYTES,
    PromptBuilder,
    PromptTooLong,
    prompt_chars,
)


def turn(user, assistant):
    return [
        {"role": "user", "content": user},
        {"role": "assistant", "content": assistant},
    ]


def tool_turn():
    return [
        {"role": "user", "content": "old question"},
        {"role": "assistant", "content": "", "tool_calls": [{
            "id": "old-call", "type": "function",
            "function": {"name": "agent_status", "arguments": "{}"},
        }]},
        {"role": "tool", "tool_call_id": "old-call", "content": '{"status":"active"}'},
        {"role": "assistant", "content": "old answer"},
    ]


class PromptBuilderTests(unittest.TestCase):
    def test_orders_system_history_and_current_input(self):
        history = [turn("first", "first reply"), turn("second", "second reply")]
        current = [{"role": "user", "content": "third"}]
        expected = [{"role": "system", "content": "rules"}] + history[0] + history[1] + current
        self.assertEqual(PromptBuilder("rules", 100).build(history, current), expected)
        self.assertEqual(PromptBuilder("", 100).build(history, current), expected[1:])

    def test_larger_user_input_removes_more_old_turns(self):
        history = [turn("111", "aaa"), turn("222", "bbb"), turn("333", "ccc")]
        builder = PromptBuilder("sys", 24)
        small = [{"role": "user", "content": "now"}]
        larger = [{"role": "user", "content": "new question"}]
        self.assertEqual(builder.build(history, small), [
            {"role": "system", "content": "sys"},
        ] + history[0] + history[1] + history[2] + small)
        self.assertEqual(builder.build(history, larger), [
            {"role": "system", "content": "sys"},
        ] + history[2] + larger)

    def test_larger_system_prompt_also_reduces_history_budget(self):
        history = [turn("old", "one"), turn("new", "two")]
        current = [{"role": "user", "content": "ask"}]
        short = PromptBuilder("sys", 18).build(history, current)
        long = PromptBuilder("long rules", 18).build(history, current)
        self.assertEqual(short[1:-1], history[0] + history[1])
        self.assertEqual(long, [{"role": "system", "content": "long rules"}] + current)

    def test_unicode_counts_characters_instead_of_utf8_bytes(self):
        history = [turn("问题", "回答")]
        current = [{"role": "user", "content": "🌍?"}]
        result = PromptBuilder("你好", 8).build(history, current)
        self.assertEqual(result, [{"role": "system", "content": "你好"}] + history[0] + current)
        self.assertEqual(prompt_chars(result), 8)

    def test_at_limit_keeps_history_and_one_character_less_drops_complete_turn(self):
        history = [turn("aa", "bb")]
        current = [{"role": "user", "content": "xyz"}]
        system = [{"role": "system", "content": "s"}]
        self.assertEqual(PromptBuilder("s", 8).build(history, current), system + history[0] + current)
        self.assertEqual(PromptBuilder("s", 7).build(history, current), system + current)
        self.assertEqual(PromptBuilder("s", 4).build(history, current), system + current)
        with self.assertRaises(PromptTooLong):
            PromptBuilder("s", 3).build(history, current)

    def test_old_tool_turn_is_removed_without_leaving_orphaned_calls_or_results(self):
        old = tool_turn()
        recent = turn("recent", "reply")
        current = [{"role": "user", "content": "now"}]
        calls = json.dumps(old[1]["tool_calls"], ensure_ascii=False, separators=(",", ":"))
        full_chars = sum(len(item["content"]) for item in old + recent + current)
        full_chars += len(calls) + len("old-call")
        result = PromptBuilder("", full_chars - 1).build([old, recent], current)
        self.assertEqual(result, recent + current)
        self.assertFalse(any("tool_calls" in item or "tool_call_id" in item for item in result))

    def test_tool_metadata_consumes_character_budget(self):
        messages = [
            {"role": "assistant", "content": "分析", "tool_calls": [{
                "id": "c", "type": "function",
                "function": {"name": "f", "arguments": "{}"},
            }]},
            {"role": "tool", "tool_call_id": "c", "content": "好"},
        ]
        encoded_call = '[{"id":"c","type":"function","function":{"name":"f","arguments":"{}"}}]'
        self.assertEqual(prompt_chars(messages), 4 + len(encoded_call))

    def test_system_and_current_tool_trajectory_are_never_partially_cut(self):
        current = tool_turn()[:-1]
        builder = PromptBuilder("rules", 10)
        saved = deepcopy(current)
        with self.assertRaises(PromptTooLong):
            builder.build([turn("old", "answer")], current)
        self.assertEqual(current, saved)

    def test_byte_budget_still_applies_when_character_budget_is_large(self):
        history = [turn("甲" * 70_000, "a"), turn("乙" * 70_000, "b")]
        current = [{"role": "user", "content": "now"}]
        result = PromptBuilder("", 500_000).build(history, current)
        self.assertEqual(result, history[1] + current)
        with self.assertRaisesRegex(PromptTooLong, "384KiB"):
            PromptBuilder("", 500_000).build([], [{"role": "user", "content": "界" * 140_000}])

    def test_returns_independent_nested_snapshot(self):
        history = [tool_turn()]
        current = [{"role": "user", "content": "now"}]
        saved_history, saved_current = deepcopy(history), deepcopy(current)
        result = PromptBuilder("rules", 1_000).build(history, current)
        result[2]["tool_calls"][0]["function"]["arguments"] = '{"agent_id":"changed"}'
        result[-1]["content"] = "changed result"
        self.assertEqual(history, saved_history)
        self.assertEqual(current, saved_current)
        history[0][0]["content"] = "changed source"
        self.assertEqual(result[1]["content"], "old question")

    def test_environment_defaults_and_overrides(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(PromptBuilder.from_env(), PromptBuilder("", DEFAULT_MAX_PROMPT_CHARS))
        with patch.dict(os.environ, {"LLM_SYSTEM_PROMPT": "环境规则", "LLM_MAX_PROMPT_CHARS": "00012"}, clear=True):
            self.assertEqual(PromptBuilder.from_env(), PromptBuilder("环境规则", 12))
        with patch.dict(os.environ, {"LLM_SYSTEM_PROMPT": "rules", "LLM_MAX_PROMPT_CHARS": ""}, clear=True):
            self.assertEqual(PromptBuilder.from_env(), PromptBuilder("rules", DEFAULT_MAX_PROMPT_CHARS))

    def test_invalid_environment_budget_is_rejected(self):
        invalid = ("0", "-1", "+1", " 10", "10 ", "1.5", "١٠", str(sys.maxsize + 1), "9" * 5_000)
        for value in invalid:
            with self.subTest(value=value[:30]), patch.dict(os.environ, {"LLM_MAX_PROMPT_CHARS": value}, clear=True):
                with self.assertRaisesRegex(ValueError, "LLM_MAX_PROMPT_CHARS"):
                    PromptBuilder.from_env()
        for value in (False, True, 0, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                PromptBuilder("", value)

    def test_system_configuration_has_json_byte_limit_and_valid_utf8(self):
        PromptBuilder("x" * (MAX_SYSTEM_PROMPT_BYTES - 2))
        for value in ("x" * (MAX_SYSTEM_PROMPT_BYTES - 1), "界" * 22_000):
            with self.subTest(size=len(value)), self.assertRaisesRegex(ValueError, "64KiB"):
                PromptBuilder(value)
        for value in ("bad\0text", "\ud800", 42):
            with self.subTest(value=repr(value)), self.assertRaisesRegex(ValueError, "LLM_SYSTEM_PROMPT"):
                PromptBuilder(value)


if __name__ == "__main__":
    unittest.main()
