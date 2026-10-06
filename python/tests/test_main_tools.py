from copy import deepcopy
from decimal import Decimal
import json
import unittest

from agent_runtime import AgentSnapshot, BusinessError, DefinitionRef, Event, ExecutionContext
from agent_runtime.agents import MainAgent
from agent_runtime.agents.main import initial_state
from agent_runtime.protocol import encode_frame, result_to_wire
from agent_runtime.prompt import PromptBuilder
from agent_runtime.tool import Tool, ToolRegistry
from agent_runtime.tools.agent_status import AGENT_STATUS


def tool_call(call_id="call-1", name="agent_status", arguments="{}"):
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }


def json_bytes(value):
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    for character, escaped in (
        ("&", "\\u0026"), ("<", "\\u003c"), (">", "\\u003e"),
        ("\u2028", "\\u2028"), ("\u2029", "\\u2029"),
    ):
        encoded = encoded.replace(character, escaped)
    return encoded.encode("utf-8")


class MainToolTests(unittest.TestCase):
    def setUp(self):
        self.agent = MainAgent(PromptBuilder())
        self.sequence = 0

    def context(self, state=None, event_type="main.request", payload=None):
        self.sequence += 1
        return ExecutionContext(
            agent=AgentSnapshot("agent-1", "main", DefinitionRef("main", "1"), "active", state, 0),
            event=Event(
                f"event-{self.sequence}", event_type,
                {"message": "检查状态"} if payload is None else payload,
                "2026-10-04T00:00:00Z",
            ),
            execution_id=f"execution-{self.sequence}",
            attempt_id=f"attempt-{self.sequence}",
        )

    def request(self, message="检查状态", state=None):
        output = self.agent.run(self.context(state, payload={"message": message}))
        return (state or {}) | output.state_update, output

    def result_context(self, state, *, result=None, status="succeeded", error=None, **changes):
        payload = {
            "action_id": state["waiting_action_id"],
            "action_type": state.get("waiting_action_type") or "model.generate",
            "execution_id": state.get("waiting_execution_id") or state["request_execution_id"],
            "status": status,
            "result": {"message": "完成"} if result is None else result,
        }
        if error is not None:
            payload["error"] = error
        payload.update(changes)
        return self.context(state, "action.result", payload)

    def reply(self, state, **changes):
        output = self.agent.run(self.result_context(state, **changes))
        return state | output.state_update, output

    def call_tools(self, state, calls=None, message=""):
        return self.reply(state, result={
            "message": message,
            "tool_calls": [tool_call()] if calls is None else calls,
        })

    def tool_cycle(self, state, call_id="call-1", result=None):
        waiting, _ = self.call_tools(state, [tool_call(call_id)])
        return self.reply(waiting, result={"status": "active"} if result is None else result)

    def assert_finished(self, state, status="succeeded"):
        self.assertEqual(state["request_status"], status)
        self.assertIsNone(state.get("pending_message"))
        self.assertEqual(state["pending_messages"], [])
        self.assertEqual(state["pending_tool_calls"], [])
        self.assertIsNone(state["waiting_action_id"])
        self.assertIsNone(state["waiting_action_type"])
        self.assertIsNone(state["waiting_execution_id"])

    def test_initial_fields_and_declared_agent_status_tool(self):
        first = initial_state()
        second = initial_state()
        first["pending_messages"].append({"role": "user", "content": "changed"})
        first["pending_tool_calls"].append(tool_call())
        self.assertEqual(second["pending_messages"], [])
        self.assertEqual(second["pending_tool_calls"], [])
        state, output = self.request()
        self.assertEqual(state["pending_messages"], [{"role": "user", "content": "检查状态"}])
        self.assertEqual(state["waiting_execution_id"], state["request_execution_id"])
        self.assertEqual(state["waiting_action_type"], "model.generate")
        tools = output.actions[0].payload["tools"]
        self.assertEqual(len(tools), 1)
        self.assertEqual(tools[0]["type"], "function")
        self.assertEqual(tools[0]["function"]["name"], "agent_status")
        self.assertEqual(tools[0]["function"]["parameters"]["type"], "object")

    def test_complete_tool_trajectory_is_committed_only_after_final_reply(self):
        state, _ = self.request()
        request_execution = state["request_execution_id"]
        waiting, tool_output = self.call_tools(state, message="我先查询状态")
        self.assertEqual(waiting["messages"], [])
        self.assertEqual(tool_output.actions[0].type, "tool.agent_status")
        self.assertEqual(tool_output.actions[0].payload, {"agent_id": "agent-1"})
        self.assertNotEqual(waiting["waiting_execution_id"], request_execution)
        continued, model_output = self.reply(waiting, result={"agent_id": "agent-1", "status": "active"})
        self.assertEqual(continued["messages"], [])
        self.assertEqual(model_output.actions[0].type, "model.generate")
        self.assertEqual([message["role"] for message in model_output.actions[0].payload["messages"]], ["user", "assistant", "tool"])
        self.assertEqual(json.loads(model_output.actions[0].payload["messages"][-1]["content"]), {"agent_id": "agent-1", "status": "active"})
        final, output = self.reply(continued, result={"message": "Agent正在运行"})
        self.assert_finished(final)
        self.assertEqual(output.actions, [])
        self.assertEqual(final["messages"], continued["pending_messages"] + [{"role": "assistant", "content": "Agent正在运行"}])
        self.assertEqual(final["request_execution_id"], request_execution)
        restored = json.loads(json.dumps(final))
        next_state, next_output = self.request("再查一次", restored)
        self.assertEqual(next_output.actions[0].payload["messages"], final["messages"] + [{"role": "user", "content": "再查一次"}])
        self.assertEqual(next_state["messages"], final["messages"])

    def test_multiple_tools_execute_one_at_a_time_in_provider_order(self):
        state, _ = self.request()
        calls = [tool_call("first"), tool_call("second", arguments='{"agent_id":"agent-2"}'), tool_call("third")]
        state, output = self.call_tools(state, calls)
        for index, target in enumerate(("agent-1", "agent-2", "agent-1")):
            self.assertEqual(len(output.actions), 1)
            self.assertEqual(output.actions[0].type, "tool.agent_status")
            self.assertEqual(output.actions[0].payload, {"agent_id": target})
            state, output = self.reply(state, result={"index": index})
        self.assertEqual(output.actions[0].type, "model.generate")
        messages = output.actions[0].payload["messages"]
        self.assertEqual([item["tool_call_id"] for item in messages if item["role"] == "tool"], ["first", "second", "third"])
        self.assertEqual([json.loads(item["content"])["index"] for item in messages if item["role"] == "tool"], [0, 1, 2])
        self.assertEqual(state["tool_rounds"], 1)

    def custom_tools(self):
        def prepare(arguments, context):
            if set(arguments) != {"text"} or not isinstance(arguments["text"], str):
                raise ValueError("inspect需要text字符串")
            return arguments | {"source": context.agent.id}

        return ToolRegistry([AGENT_STATUS, Tool(
            name="inspect", description="检查测试文本",
            parameters={
                "type": "object", "properties": {"text": {"type": "string"}},
                "required": ["text"], "additionalProperties": False,
            },
            prepare=prepare,
        )])

    def test_registered_tools_share_dispatch_results_and_restored_queue(self):
        self.agent = MainAgent(PromptBuilder(), self.custom_tools())
        state, output = self.request()
        self.assertEqual(
            [item["function"]["name"] for item in output.actions[0].payload["tools"]],
            ["agent_status", "inspect"],
        )
        calls = [
            tool_call("one", "inspect", '{"text":"first"}'),
            tool_call("two"),
            tool_call("three", "inspect", '{"text":"last"}'),
        ]
        state, output = self.call_tools(state, calls)
        self.assertEqual(output.actions[0].type, "tool.inspect")
        self.assertEqual(output.actions[0].payload, {"text": "first", "source": "agent-1"})
        for index, expected in enumerate(("tool.agent_status", "tool.inspect", "model.generate")):
            state = json.loads(json.dumps(state))
            self.agent = MainAgent(PromptBuilder(), self.custom_tools())
            if index == 2:
                state, output = self.reply(state, status="failed", error="检查失败")
            else:
                state, output = self.reply(state, result={"index": index})
            self.assertEqual(output.actions[0].type, expected)
        messages = [item for item in output.actions[0].payload["messages"] if item["role"] == "tool"]
        self.assertEqual([item["tool_call_id"] for item in messages], ["one", "two", "three"])
        self.assertEqual([json.loads(item["content"]) for item in messages], [
            {"index": 0}, {"index": 1}, {"error": "检查失败"},
        ])
        state, _ = self.reply(state)
        self.assert_finished(state)

    def test_registered_parameter_error_continues_to_next_tool(self):
        self.agent = MainAgent(PromptBuilder(), self.custom_tools())
        state, _ = self.request()
        state, output = self.call_tools(state, [
            tool_call("bad", "inspect", '{"text":42}'), tool_call("good"),
        ])
        self.assertEqual(output.actions[0].type, "tool.agent_status")
        self.assertEqual(json.loads(state["pending_messages"][-1]["content"]), {"error": "inspect需要text字符串"})
        state, output = self.reply(state, result={"status": "active"})
        self.assertEqual(output.actions[0].type, "model.generate")

    def test_waiting_tool_must_match_queue_and_registered_definition(self):
        self.agent = MainAgent(PromptBuilder(), self.custom_tools())
        state, _ = self.request()
        state, _ = self.call_tools(state, [tool_call("inspect", "inspect", '{"text":"test"}')])
        mismatched = deepcopy(state)
        mismatched["waiting_action_type"] = "tool.agent_status"
        with self.assertRaisesRegex(ValueError, "待处理工具不匹配"):
            self.reply(mismatched, result={"status": "active"})
        self.agent = MainAgent(PromptBuilder())
        with self.assertRaisesRegex(ValueError, "waiting_action_type"):
            self.reply(state, result={"text": "test"})

    def test_empty_registry_omits_model_tools_and_rejects_unregistered_calls(self):
        self.agent = MainAgent(PromptBuilder(), ToolRegistry())
        state, output = self.request()
        self.assertNotIn("tools", output.actions[0].payload)
        state, output = self.call_tools(state)
        self.assertEqual(output.actions[0].type, "model.generate")
        self.assertNotIn("tools", output.actions[0].payload)
        self.assertEqual(json.loads(state["pending_messages"][-1]["content"]), {"error": "不支持的工具: agent_status"})
        state, _ = self.reply(state)
        self.assert_finished(state)

    def test_failed_tool_becomes_error_message_and_model_continues(self):
        state, _ = self.request()
        state, _ = self.call_tools(state)
        state, output = self.reply(state, status="failed", error="目标Agent不存在")
        self.assertEqual(state["request_status"], "waiting")
        self.assertEqual(output.actions[0].type, "model.generate")
        self.assertEqual(json.loads(output.actions[0].payload["messages"][-1]["content"]), {"error": "目标Agent不存在"})
        final, _ = self.reply(state, result={"message": "无法找到目标Agent"})
        self.assert_finished(final)
        self.assertEqual(final["messages"][-2]["role"], "tool")

    def test_invalid_arguments_and_unknown_tool_are_inline_errors(self):
        invalid = (
            ("agent_status", "not-json"), ("agent_status", "null"),
            ("agent_status", "[]"), ("agent_status", ""),
            ("agent_status", '{"agent_id":null}'),
            ("agent_status", '{"agent_id":42}'),
            ("agent_status", '{"agent_id":"  "}'),
            ("agent_status", json.dumps({"agent_id": "界" * 342})),
            ("agent_status", '{"agent_id":"\\ud800"}'),
            ("agent_status", '{"other":1}'),
            ("agent_status", '{"agent_id":"one","agent_id":"two"}'),
            ("agent_status", '{"agent_id":NaN}'),
            ("missing_tool", "{}"),
        )
        for name, arguments in invalid:
            with self.subTest(name=name, arguments=arguments):
                state, _ = self.request()
                call = tool_call(name=name, arguments=arguments)
                state, output = self.call_tools(state, [call])
                self.assertEqual(len(output.actions), 1)
                self.assertEqual(output.actions[0].type, "model.generate")
                messages = output.actions[0].payload["messages"]
                self.assertEqual(messages[1]["tool_calls"], [call])
                self.assertEqual(messages[-1]["tool_call_id"], call["id"])
                error = json.loads(messages[-1]["content"])["error"]
                self.assertIsInstance(error, str)
                self.assertTrue(error)
                self.assertEqual(state["messages"], [])

    def test_invalid_tool_between_valid_tools_keeps_batch_order(self):
        state, _ = self.request()
        state, output = self.call_tools(state, [tool_call("one"), tool_call("bad", name="missing_tool"), tool_call("two")])
        state, output = self.reply(state, result={"first": True})
        self.assertEqual(output.actions[0].type, "tool.agent_status")
        state, output = self.reply(state, result={"second": True})
        tools = [message for message in output.actions[0].payload["messages"] if message["role"] == "tool"]
        self.assertEqual([message["tool_call_id"] for message in tools], ["one", "bad", "two"])
        self.assertIn("error", json.loads(tools[1]["content"]))

    def test_each_action_uses_execution_that_emitted_it_and_stale_results_reject(self):
        state, _ = self.request()
        old_action = state["waiting_action_id"]
        request_execution = state["request_execution_id"]
        context = self.result_context(state, result={"message": "", "tool_calls": [tool_call()]})
        output = self.agent.run(context)
        waiting = state | output.state_update
        self.assertEqual(waiting["waiting_execution_id"], context.execution_id)
        self.assertEqual(waiting["request_execution_id"], request_execution)
        for changes in (
            {"action_id": old_action}, {"execution_id": request_execution},
            {"action_type": "model.generate"}, {"status": "pending"},
        ):
            with self.subTest(changes=changes):
                saved = deepcopy(waiting)
                with self.assertRaises(BusinessError):
                    self.agent.run(self.result_context(waiting, result={"status": "active"}, **changes))
                self.assertEqual(waiting, saved)
        duplicate = self.result_context(waiting, result={"status": "active"})
        output = self.agent.run(duplicate)
        continued = waiting | output.state_update
        self.assertEqual(continued["waiting_execution_id"], duplicate.execution_id)
        saved = deepcopy(continued)
        with self.assertRaises(BusinessError):
            self.agent.run(self.context(continued, "action.result", duplicate.event.payload))
        self.assertEqual(continued, saved)

    def test_failed_model_after_tool_preserves_successful_history(self):
        history = [{"role": "user", "content": "earlier"}, {"role": "assistant", "content": "earlier reply"}]
        state, _ = self.request(state=initial_state() | {"messages": deepcopy(history)})
        state, _ = self.tool_cycle(state)
        state, output = self.reply(state, status="failed", error="模型不可用")
        self.assert_finished(state, "failed")
        self.assertEqual(state["messages"], history)
        self.assertEqual(output.actions, [])
        self.assertEqual(state["error"], "模型不可用")
        _, next_output = self.request("重试", state)
        self.assertEqual(next_output.actions[0].payload["messages"], history + [{"role": "user", "content": "重试"}])

    def test_action_state_and_result_payload_do_not_share_mutable_snapshots(self):
        state, output = self.request()
        output.actions[0].payload["messages"][0]["content"] = "changed action"
        output.actions[0].payload["tools"][0]["function"]["name"] = "changed tool"
        self.assertEqual(state["pending_messages"][0]["content"], "检查状态")
        calls = [tool_call()]
        saved = deepcopy(state)
        waiting, _ = self.call_tools(state, calls)
        self.assertEqual(state, saved)
        calls[0]["function"]["arguments"] = "changed response"
        self.assertEqual(waiting["pending_messages"][-1]["tool_calls"][0]["function"]["arguments"], "{}")
        waiting["pending_tool_calls"][0]["function"]["arguments"] = "changed queue"
        self.assertEqual(waiting["pending_messages"][-1]["tool_calls"][0]["function"]["arguments"], "{}")
        another, output = self.request()
        self.assertEqual(output.actions[0].payload["tools"][0]["function"]["name"], "agent_status")
        another, _ = self.call_tools(another)
        result = {"status": "active", "nested": {"values": [1, 2]}}
        saved = deepcopy(another)
        continued, output = self.reply(another, result=result)
        self.assertEqual(another, saved)
        result["nested"]["values"].append(3)
        tool = output.actions[0].payload["messages"][-1]
        self.assertEqual(json.loads(tool["content"])["nested"]["values"], [1, 2])
        output.actions[0].payload["messages"][1]["tool_calls"][0]["function"]["name"] = "changed action"
        self.assertEqual(continued["pending_messages"][1]["tool_calls"][0]["function"]["name"], "agent_status")

    def test_decimal_tool_result_remains_exact_json_number(self):
        state, _ = self.request()
        state, _ = self.call_tools(state)
        value = Decimal("9007199254740993.1234567890123456789")
        state, output = self.reply(state, result={"exact": value, "nested": [Decimal("0.1")]})
        content = output.actions[0].payload["messages"][-1]["content"]
        result = json.loads(content, parse_float=Decimal)
        self.assertEqual(result, {"exact": value, "nested": [Decimal("0.1")]})
        self.assertNotIn('"9007199254740993', content)
        self.assertNotIn(" ", content)

    def test_oversized_tool_result_is_bounded_error_and_model_continues(self):
        state, _ = self.request()
        state, _ = self.call_tools(state)
        state, output = self.reply(state, result={"large": "x" * (8 * 1024)})
        self.assertEqual(output.actions[0].type, "model.generate")
        self.assertIn("error", json.loads(output.actions[0].payload["messages"][-1]["content"]))
        self.assertLess(len(output.actions[0].payload["messages"][-1]["content"].encode("utf-8")), 1024)

    def test_tool_round_limit_allows_four_batches_then_fails_fifth(self):
        state, _ = self.request()
        for number in range(4):
            state, _ = self.tool_cycle(state, call_id=f"round-{number}")
            self.assertEqual(state["tool_rounds"], number + 1)
        state, output = self.call_tools(state, [tool_call("fifth")])
        self.assert_finished(state, "failed")
        self.assertEqual(state["messages"], [])
        self.assertEqual(output.actions, [])
        self.assertIn("4", state["error"])

    def test_four_tool_batches_can_still_finish_with_final_text(self):
        state, _ = self.request()
        for number in range(4):
            state, _ = self.tool_cycle(state, call_id=f"round-{number}")
        state, _ = self.reply(state, result={"message": "已完成四次查询"})
        self.assert_finished(state)
        self.assertEqual(len([message for message in state["messages"] if message["role"] == "tool"]), 4)

    def test_batch_limit_accepts_eight_and_rejects_nine(self):
        state, _ = self.request()
        state, _ = self.call_tools(state, [tool_call(f"call-{index}") for index in range(8)])
        for index in range(8):
            state, output = self.reply(state, result={"index": index})
        self.assertEqual(output.actions[0].type, "model.generate")
        self.assertEqual(len([item for item in state["pending_messages"] if item["role"] == "tool"]), 8)
        state, _ = self.request()
        state, output = self.call_tools(state, [tool_call(f"call-{index}") for index in range(9)])
        self.assert_finished(state, "failed")
        self.assertEqual(output.actions, [])

    def test_batch_size_limit_preserves_previous_history(self):
        history = [{"role": "user", "content": "earlier"}, {"role": "assistant", "content": "reply"}]
        state, _ = self.request(state=initial_state() | {"messages": deepcopy(history)})
        state, output = self.call_tools(state, [tool_call(arguments="x" * (32 * 1024))])
        self.assert_finished(state, "failed")
        self.assertEqual(output.actions, [])
        self.assertEqual(state["messages"], history)
        self.assertIn("32KiB", state["error"])

    def test_call_identifiers_and_names_match_transport_limits(self):
        valid = (("界" * 42 + "ab", "agent_status"), ("call", "a" * 64))
        for call_id, name in valid:
            with self.subTest(valid=(call_id, name)):
                state, _ = self.request()
                state, output = self.call_tools(state, [tool_call(call_id, name)])
                self.assertEqual(state["request_status"], "waiting")
                self.assertEqual(len(output.actions), 1)
        invalid = (
            ("界" * 43, "agent_status"), ("", "agent_status"),
            (" \t\n", "agent_status"),
            ("call", "a" * 65), ("call", ""), ("call", "有中文"),
            ("call", "has.dot"), ("call", "has space"),
        )
        for call_id, name in invalid:
            with self.subTest(invalid=(call_id, name)):
                state, _ = self.request()
                state, output = self.call_tools(state, [tool_call(call_id, name)])
                self.assert_finished(state, "failed")
                self.assertEqual(output.actions, [])

    def test_duplicate_ids_within_batch_fail_but_closed_batch_can_reuse_id(self):
        state, _ = self.request()
        state, output = self.call_tools(state, [tool_call("duplicate"), tool_call("duplicate")])
        self.assert_finished(state, "failed")
        self.assertEqual(output.actions, [])
        state, _ = self.request()
        state, _ = self.tool_cycle(state, "reused")
        state, _ = self.tool_cycle(state, "reused")
        state, _ = self.reply(state)
        self.assert_finished(state)
        self.assertEqual([message["tool_call_id"] for message in state["messages"] if message["role"] == "tool"], ["reused", "reused"])

    def test_pending_trajectory_limit_fails_without_partial_history(self):
        history = [{"role": "user", "content": "earlier"}, {"role": "assistant", "content": "reply"}]
        state, _ = self.request(state=initial_state() | {"messages": deepcopy(history)})
        state, output = self.call_tools(state, message="x" * (128 * 1024))
        self.assert_finished(state, "failed")
        self.assertEqual(state["messages"], history)
        self.assertEqual(output.actions, [])
        self.assertIn("128KiB", state["error"])

    def test_final_reply_can_exceed_pending_budget_and_history_drops_whole_turn(self):
        state, _ = self.request()
        state, _ = self.tool_cycle(state)
        answer = "界" * (384 * 1024 // 3)
        state, output = self.reply(state, result={"message": answer})
        self.assert_finished(state)
        self.assertEqual(state["result"], answer)
        self.assertEqual(state["messages"], [])
        self.assertEqual(output.actions, [])

    def test_history_limit_counts_complete_tool_turns(self):
        state = initial_state()
        turns = []
        for index in range(22):
            message = f"question-{index}"
            state, _ = self.request(message, state)
            state, _ = self.tool_cycle(state, f"call-{index}")
            turn = deepcopy(state["pending_messages"])
            turn.append({"role": "assistant", "content": f"answer-{index}"})
            state, _ = self.reply(state, result={"message": f"answer-{index}"})
            turns.append(turn)
            self.assertEqual(state["messages"], [message for saved in turns[-20:] for message in saved])
        self.assertEqual(state["messages"][0]["content"], "question-2")
        self.assertEqual(state["messages"][1]["tool_calls"][0]["id"], "call-2")

    def test_legacy_pending_message_only_waiting_state_can_finish_or_start_tools(self):
        legacy = {
            "messages": [{"role": "user", "content": "old"}, {"role": "assistant", "content": "old reply"}],
            "pending_message": "old current",
            "request_status": "waiting", "request_event_id": "old-request",
            "request_execution_id": "old-execution", "waiting_action_id": "old-action",
            "result_event_id": None, "result": None, "error": None,
        }
        state, output = self.call_tools(legacy)
        self.assertEqual(output.actions[0].type, "tool.agent_status")
        self.assertEqual(state["pending_messages"][0], {"role": "user", "content": "old current"})
        self.assertIsNone(state["pending_message"])
        state = json.loads(json.dumps(state))
        state, _ = self.reply(state, result={"status": "active"})
        state, _ = self.reply(state)
        self.assert_finished(state)
        self.assertEqual(state["messages"][:2], legacy["messages"])
        self.assertEqual(state["messages"][2]["content"], "old current")

    def test_old_waiting_state_without_input_fails_tool_round_without_invented_history(self):
        legacy = {
            "request_status": "waiting", "request_event_id": "old-request",
            "request_execution_id": "old-execution", "waiting_action_id": "old-action",
            "result_event_id": None, "result": None, "error": None,
        }
        state, output = self.call_tools(legacy)
        self.assert_finished(state, "failed")
        self.assertEqual(output.actions, [])
        self.assertEqual(state["messages"], [])

    def test_waiting_queue_and_trajectory_survive_json_round_trip_between_actions(self):
        state, _ = self.request()
        calls = [tool_call("first"), tool_call("second", arguments='{"agent_id":"agent-2"}')]
        state, _ = self.call_tools(state, calls)
        state = json.loads(json.dumps(state))
        state, output = self.reply(state, result={"first": True})
        self.assertEqual(output.actions[0].type, "tool.agent_status")
        self.assertEqual(output.actions[0].payload, {"agent_id": "agent-2"})
        state = json.loads(json.dumps(state))
        state, output = self.reply(state, result={"second": True})
        self.assertEqual(output.actions[0].type, "model.generate")
        state = json.loads(json.dumps(state))
        state, _ = self.reply(state)
        self.assert_finished(state)
        self.assertEqual([message["tool_call_id"] for message in state["messages"] if message["role"] == "tool"], ["first", "second"])

    def test_invalid_tool_result_preserves_waiting_state(self):
        state, _ = self.request()
        state, _ = self.call_tools(state)
        for result in ([], "text", 42):
            with self.subTest(result=result):
                saved = deepcopy(state)
                with self.assertRaises(BusinessError):
                    self.reply(state, result=result)
                self.assertEqual(state, saved)

    def test_large_history_and_pending_fit_wire_and_failure_retains_history(self):
        # 单独覆盖协议字节上限，字数上限由prompt测试覆盖。
        self.agent = MainAgent(PromptBuilder(max_chars=384 * 1024))
        history = []
        for index in range(6):
            history.extend((
                {"role": "user", "content": f"old-{index}"},
                {"role": "assistant", "content": "a" * 64000},
            ))
        self.assertLess(len(json_bytes(history)), 384 * 1024)
        state, _ = self.request("u" * 64000, initial_state() | {"messages": deepcopy(history)})
        state, _ = self.call_tools(state, message="thinking " + "t" * 63000)
        context = self.result_context(state, result={"status": "active"})
        output = self.agent.run(context)
        state = state | output.state_update
        self.assertEqual(state["messages"], history)
        self.assertGreater(len(json_bytes(state["pending_messages"])), 120 * 1024)
        self.assertLess(len(json_bytes(state["pending_messages"])), 128 * 1024)
        provider_messages = output.actions[0].payload["messages"]
        self.assertLessEqual(len(json_bytes(provider_messages)), 384 * 1024)
        self.assertEqual(provider_messages[0]["content"], "old-2")
        frame = encode_frame(result_to_wire(output, context))
        self.assertLess(len(frame), 1 << 20)
        state, _ = self.reply(state, status="failed", error="模型失败")
        self.assert_finished(state, "failed")
        self.assertEqual(state["messages"], history)


if __name__ == "__main__":
    unittest.main()
