from copy import deepcopy
from dataclasses import replace
import json
import unittest
from unittest.mock import patch

from agent_runtime import AgentSnapshot, BusinessError, DefinitionRef, Event, ExecutionContext
from agent_runtime.agents import MainAgent
from agent_runtime.agents.main import initial_state
from agent_runtime.prompt import PromptBuilder
from agent_runtime.tools import default_tools
from agent_runtime.tool import Tool, ToolRegistry


def tool_call():
    return {
        "id": "call-1", "type": "function",
        "function": {"name": "agent_status", "arguments": "{}"},
    }


class MainResolutionTests(unittest.TestCase):
    def setUp(self):
        self.agent = MainAgent(PromptBuilder("冻结的system", 100_000))
        self.sequence = 0

    def context(self, state=None, event_type="main.request", payload=None):
        self.sequence += 1
        return ExecutionContext(
            agent=AgentSnapshot("agent-1", "main", DefinitionRef("main", "1"), "active", state, 0),
            event=Event(
                f"event-{self.sequence}", event_type,
                {"message": "当前输入"} if payload is None else payload,
                "2026-10-04T00:00:00Z",
            ),
            execution_id=f"execution-{self.sequence}", attempt_id=f"attempt-{self.sequence}",
        )

    def request(self, state=None, message="当前输入"):
        output = self.agent.run(self.context(state, payload={"message": message}))
        return (state or {}) | output.state_update, output.actions[0]

    def result_context(self, state, result=None, **changes):
        payload = {
            "action_id": state["waiting_action_id"],
            "action_type": state.get("waiting_action_type") or "model.generate",
            "execution_id": state.get("waiting_execution_id") or state["request_execution_id"],
            "status": "succeeded", "result": {"message": "完成"} if result is None else result,
        }
        payload.update(changes)
        return self.context(state, "action.result", payload)

    def resolution_context(self, state, action=None, *, decision="retry", **changes):
        payload = {
            "action_id": state["waiting_action_id"], "action_type": "model.generate",
            "execution_id": state.get("waiting_execution_id") or state["request_execution_id"],
            "decision": decision, "reason": "用户选择重试" if decision == "retry" else "不再等待",
        }
        if action is not None:
            payload["retry_payload"] = deepcopy(action.payload)
        payload.update(changes)
        return self.context(state, "action.resolution", payload)

    def tool_continuation(self, state):
        first = self.agent.run(self.result_context(state, {"message": "先查状态", "tool_calls": [tool_call()]}))
        waiting = state | first.state_update
        next_model = self.agent.run(self.result_context(waiting, {"agent_id": "agent-1", "status": "active"}))
        return waiting | next_model.state_update, next_model.actions[0]

    def test_retry_uses_frozen_payload_without_rebuilding_prompt(self):
        history = [{"role": "user", "content": "之前输入"}, {"role": "assistant", "content": "之前回复"}]
        state, action = self.request(initial_state() | {"messages": history})
        state.update(result="旧结果", error="旧错误", result_event_id="旧事件")
        saved = deepcopy(state)
        context = self.resolution_context(state, action)
        frozen = deepcopy(context.event.payload)
        self.agent.prompt_builder = PromptBuilder("改变后的system", 1)
        with patch.object(PromptBuilder, "build", side_effect=AssertionError("不得重新拼接prompt")) as builder:
            output = self.agent.run(context)
        builder.assert_not_called()
        retry = output.actions[0]
        self.assertNotEqual(retry.id, action.id)
        self.assertEqual(retry.type, "model.generate")
        self.assertEqual(retry.payload, action.payload | {"retry_of": action.id})
        self.assertEqual(state, saved)
        self.assertEqual(context.event.payload, frozen)
        update = output.state_update
        for key in ("messages", "pending_message", "pending_messages", "pending_tool_calls", "tool_rounds", "request_event_id", "request_execution_id"):
            self.assertEqual(update[key], state[key])
        self.assertEqual(update["request_status"], "waiting")
        self.assertEqual(update["waiting_action_id"], retry.id)
        self.assertEqual(update["waiting_execution_id"], context.execution_id)
        self.assertEqual(update["waiting_action_type"], "model.generate")
        for key in ("error", "result", "result_event_id"):
            self.assertIsNone(update[key])

    def test_retry_preserves_tools_and_full_trajectory_through_final_reply(self):
        state, _ = self.request()
        state, action = self.tool_continuation(state)
        source_request = state["request_execution_id"]
        saved = deepcopy(state)
        context = self.resolution_context(state, action)
        retry_output = self.agent.run(context)
        retried = state | retry_output.state_update
        retry = retry_output.actions[0]
        self.assertEqual(retried["pending_messages"], state["pending_messages"])
        self.assertEqual(retried["pending_tool_calls"], [])
        self.assertEqual(retried["tool_rounds"], 1)
        self.assertEqual(retried["request_execution_id"], source_request)
        self.assertEqual([item["role"] for item in retry.payload["messages"]], ["system", "user", "assistant", "tool"])
        self.assertEqual(retry.payload["messages"][2]["tool_calls"], [tool_call()])
        self.assertEqual(json.loads(retry.payload["messages"][3]["content"]), {"agent_id": "agent-1", "status": "active"})
        self.assertEqual(retry.payload["tools"], default_tools().definitions())
        self.assertEqual(state, saved)
        # 旧调用的迟到结果不得接管新的等待关系。
        with self.assertRaises(BusinessError):
            self.agent.run(self.result_context(retried, action_id=action.id))
        completed = self.agent.run(self.result_context(retried, {"message": "恢复后的回复"}))
        final = retried | completed.state_update
        self.assertEqual(final["request_status"], "succeeded")
        self.assertEqual(final["messages"], saved["pending_messages"] + [{"role": "assistant", "content": "恢复后的回复"}])
        self.assertEqual(final["request_execution_id"], source_request)

    def test_retry_deep_copies_action_state_and_input_payload(self):
        state, action = self.request()
        state, action = self.tool_continuation(state)
        context = self.resolution_context(state, action)
        saved_state = deepcopy(state)
        saved_payload = deepcopy(context.event.payload)
        output = self.agent.run(context)
        output.actions[0].payload["messages"][2]["tool_calls"][0]["function"]["arguments"] = "changed"
        output.actions[0].payload["tools"][0]["function"]["parameters"]["properties"]["new"] = {}
        output.state_update["pending_messages"][1]["tool_calls"][0]["id"] = "changed"
        self.assertEqual(state, saved_state)
        self.assertEqual(context.event.payload, saved_payload)
        self.assertEqual(action.payload, saved_payload["retry_payload"])

    def test_retry_of_is_replaced_when_retrying_a_retry(self):
        state, action = self.request()
        first = self.agent.run(self.resolution_context(state, action))
        retried = state | first.state_update
        second = self.agent.run(self.resolution_context(retried, first.actions[0]))
        self.assertEqual(second.actions[0].payload["retry_of"], first.actions[0].id)
        self.assertEqual(second.actions[0].payload["messages"], action.payload["messages"])

    def test_abandon_preserves_successful_history_and_next_request_skips_failed_turn(self):
        history = [{"role": "user", "content": "已完成输入"}, {"role": "assistant", "content": "已完成回复"}]
        state, _ = self.request(initial_state() | {"messages": history}, "放弃的输入")
        state, _ = self.tool_continuation(state)
        saved = deepcopy(state)
        context = self.resolution_context(state, decision="abandon", reason="服务状态无法确认")
        output = self.agent.run(context)
        self.assertEqual(output.actions, [])
        final = state | output.state_update
        self.assertEqual(final["messages"], history)
        self.assertEqual(final["request_status"], "failed")
        self.assertEqual(final["error"], "用户放弃本轮: 服务状态无法确认")
        self.assertEqual(final["result_event_id"], context.event.id)
        self.assertEqual(final["request_event_id"], state["request_event_id"])
        self.assertEqual(final["request_execution_id"], state["request_execution_id"])
        for key in ("pending_message", "waiting_action_id", "waiting_action_type", "waiting_execution_id", "result"):
            self.assertIsNone(final[key])
        self.assertEqual(final["pending_messages"], [])
        self.assertEqual(final["pending_tool_calls"], [])
        self.assertEqual(final["tool_rounds"], 0)
        self.assertEqual(state, saved)
        _, next_action = self.request(final, "下一条输入")
        self.assertEqual(next_action.payload["messages"], [{"role": "system", "content": "冻结的system"}] + history + [{"role": "user", "content": "下一条输入"}])

    def test_mismatched_and_invalid_resolutions_leave_state_unchanged(self):
        state, action = self.request()
        invalid = (
            {"action_id": "过期操作"}, {"execution_id": "过期执行"}, {"action_type": "tool.agent_status"},
            {"decision": "succeeded"}, {"decision": None}, {"reason": ""}, {"reason": "  "}, {"reason": 1},
            {"retry_payload": None}, {"retry_payload": {}}, {"retry_payload": {"messages": []}},
            {"retry_payload": {"messages": "消息"}}, {"retry_payload": {"messages": [None]}},
            {"retry_payload": {"messages": [{"role": "unknown", "content": "消息"}]}},
            {"retry_payload": {"messages": [{"role": "user", "content": None}]}},
        )
        saved = deepcopy(state)
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(BusinessError):
                self.agent.run(self.resolution_context(state, action, **changes))
            self.assertEqual(state, saved)
        context = self.resolution_context(state, action)
        with self.assertRaises(BusinessError):
            self.agent.run(replace(context, event=replace(context.event, payload=None)))
        self.assertEqual(state, saved)

    def test_nonwaiting_and_waiting_tool_cannot_be_resolved(self):
        state, action = self.request()
        context = self.resolution_context(state, action)
        for status in ("idle", "succeeded", "failed"):
            finished = initial_state() | {"request_status": status}
            with self.subTest(status=status), self.assertRaises(BusinessError):
                self.agent.run(replace(context, agent=replace(context.agent, state=finished)))
        tools = self.agent.run(self.result_context(state, {"message": "", "tool_calls": [tool_call()]}))
        waiting = state | tools.state_update
        saved = deepcopy(waiting)
        for decision in ("retry", "abandon"):
            with self.subTest(decision=decision), self.assertRaises(BusinessError):
                self.agent.run(self.resolution_context(waiting, action, decision=decision))
            self.assertEqual(waiting, saved)

    def waiting_tool_state(self):
        state, _ = self.request()
        output = self.agent.run(
            self.result_context(state, {"message": "", "tool_calls": [tool_call()]})
        )
        return state | output.state_update

    def test_waiting_tool_retry_rebuilds_same_action(self):
        waiting = self.waiting_tool_state()
        saved = deepcopy(waiting)
        self.assertEqual(waiting["waiting_action_type"], "tool.agent_status")

        context = self.resolution_context(waiting, decision="retry", action_type="tool.agent_status", retry_payload={"agent_id": "agent-1"})
        output = self.agent.run(context)

        self.assertEqual(waiting, saved)
        self.assertEqual(len(output.actions), 1)
        action = output.actions[0]
        self.assertEqual(action.type, "tool.agent_status")
        self.assertEqual(action.payload, {"agent_id": "agent-1"})
        self.assertNotIn("retry_of", action.payload)

        retried = waiting | output.state_update
        self.assertEqual(retried["request_status"], "waiting")
        self.assertEqual(retried["waiting_action_type"], "tool.agent_status")
        self.assertEqual(retried["waiting_action_id"], action.id)
        self.assertEqual(retried["waiting_execution_id"], context.execution_id)
        self.assertEqual(retried["pending_tool_calls"], waiting["pending_tool_calls"])

    def test_waiting_tool_retry_replays_payload_after_tool_rules_change(self):
        calls = []

        def prepare(arguments, context):
            calls.append(deepcopy(arguments))
            if len(calls) > 1:
                raise ValueError("工具规则已变化，不再接受这些参数")
            return arguments | {"source": context.agent.id}

        registry = ToolRegistry([Tool(
            name="inspect", description="检查测试文本",
            parameters={
                "type": "object", "properties": {"text": {"type": "string"}},
                "required": ["text"], "additionalProperties": False,
            },
            prepare=prepare,
        )])
        self.agent = MainAgent(PromptBuilder("冻结的system", 100_000), registry)

        state, _ = self.request()
        call = {
            "id": "call-1", "type": "function",
            "function": {"name": "inspect", "arguments": '{"text":"旧参数"}'},
        }
        waiting = state | self.agent.run(
            self.result_context(state, {"message": "", "tool_calls": [call]})
        ).state_update
        self.assertEqual(len(calls), 1)

        frozen = {"text": "旧参数", "source": "agent-1"}
        output = self.agent.run(self.resolution_context(
            waiting, decision="retry", action_type="tool.inspect", retry_payload=frozen,
        ))

        self.assertEqual(len(calls), 1)  # 重试没有再调用prepare
        self.assertEqual(output.actions[0].type, "tool.inspect")
        self.assertEqual(output.actions[0].payload, frozen)

    def test_waiting_tool_abandon_drops_incomplete_round(self):
        waiting = self.waiting_tool_state()
        saved = deepcopy(waiting)

        output = self.agent.run(
            self.resolution_context(waiting, decision="abandon", action_type="tool.agent_status")
        )

        self.assertEqual(waiting, saved)
        self.assertEqual(output.actions, [])
        final = waiting | output.state_update
        self.assertEqual(final["request_status"], "failed")
        self.assertIn("不再等待", final["error"])
        self.assertEqual(final["pending_tool_calls"], [])
        self.assertEqual(final["pending_messages"], [])
        self.assertEqual(final["messages"], [])
        self.assertIsNone(final["waiting_action_id"])
        self.assertIsNone(final["waiting_action_type"])

    def test_existing_state_and_history_validation_still_precede_resolution(self):
        state, action = self.request()
        for changes in (
            {"messages": [{"role": "user", "content": "不完整历史"}]},
            {"pending_messages": []}, {"request_status": "unknown"}, {"waiting_execution_id": None},
        ):
            invalid = state | changes
            saved = deepcopy(invalid)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.agent.run(self.resolution_context(invalid, action))
            self.assertEqual(invalid, saved)

    def test_legacy_without_input_can_retry_then_finish_without_inventing_history(self):
        state, action = self.request()
        del state["messages"]
        del state["pending_message"]
        del state["waiting_execution_id"]
        del state["waiting_action_type"]
        saved = deepcopy(state)
        output = self.agent.run(self.resolution_context(state, action))
        retried = state | output.state_update
        self.assertEqual(state, saved)
        self.assertNotIn("messages", retried)
        self.assertNotIn("pending_message", retried)
        restored = json.loads(json.dumps(retried))
        final = self.agent.run(self.result_context(restored)).state_update
        self.assertEqual(final["request_status"], "succeeded")
        self.assertEqual(final["messages"], [])
        self.assertIsNone(final["pending_message"])
        _, next_action = self.request(final, "新输入")
        self.assertEqual(next_action.payload["messages"][-1], {"role": "user", "content": "新输入"})

    def test_legacy_without_input_can_abandon(self):
        state, _ = self.request()
        del state["messages"]
        del state["pending_message"]
        output = self.agent.run(self.resolution_context(state, decision="abandon"))
        self.assertEqual(output.state_update["request_status"], "failed")
        self.assertEqual(output.state_update["messages"], [])
        self.assertIsNone(output.state_update["pending_message"])

    def test_legacy_with_pending_message_only_can_retry_then_record_completed_turn(self):
        state, action = self.request()
        for key in ("pending_messages", "pending_tool_calls", "tool_rounds", "waiting_action_type", "waiting_execution_id"):
            del state[key]
        output = self.agent.run(self.resolution_context(state, action))
        retried = state | output.state_update
        self.assertEqual(retried["pending_messages"], [{"role": "user", "content": "当前输入"}])
        final = self.agent.run(self.result_context(retried)).state_update
        self.assertEqual(final["messages"], [{"role": "user", "content": "当前输入"}, {"role": "assistant", "content": "完成"}])


if __name__ == "__main__":
    unittest.main()
