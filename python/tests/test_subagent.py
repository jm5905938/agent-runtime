from copy import deepcopy
from dataclasses import asdict, replace
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from agent_runtime import AgentSnapshot, BusinessError, DefinitionRef, Event, ExecutionContext, ExecutionResult
from agent_runtime.agents import MainAgent, SubagentAgent
from agent_runtime.agents.main import initial_state
from agent_runtime.agents.subagent import TASK_PROMPT
from agent_runtime.prompt import PromptBuilder
from agent_runtime.protocol import ProtocolError, decode_frame, encode_frame, result_to_wire


class SubagentTests(unittest.TestCase):
    def setUp(self):
        self.sequence = 0
        self.agent = SubagentAgent(PromptBuilder("共同规则"))

    def context(self, state=None, event_type="subagent.request", payload=None, agent_id="child-1"):
        self.sequence += 1
        return ExecutionContext(
            agent=AgentSnapshot(agent_id, "child", DefinitionRef("subagent", "1"), "active", state, 0),
            event=Event(
                f"event-{self.sequence}", event_type,
                {"message": "完成独立任务"} if payload is None else payload,
                "2026-10-07T00:00:00Z",
            ),
            execution_id=f"execution-{self.sequence}", attempt_id=f"attempt-{self.sequence}",
        )

    def request(self, state=None, **changes):
        return self.agent.run(self.context(state, **changes))

    def result(self, state, result=None, **changes):
        payload = {
            "action_id": state["waiting_action_id"],
            "action_type": state["waiting_action_type"],
            "execution_id": state["waiting_execution_id"],
            "status": "succeeded", "result": {"message": "完整结果"} if result is None else result,
        }
        return self.agent.run(self.context(state, "action.result", payload | changes))

    def test_independent_children_share_rules_and_main_tools_without_parent_history(self):
        first = self.request(payload={"message": "任务一", "context": "参考数据"})
        second = self.request(agent_id="child-2", payload={"message": "任务二"})
        self.assertEqual(first.state_update["messages"], [])
        self.assertEqual(second.state_update["messages"], [])
        first_messages = first.actions[0].payload["messages"]
        self.assertEqual(first_messages, [
            {"role": "system", "content": "共同规则\n\n" + TASK_PROMPT},
            {"role": "user", "content": "任务一\n\n任务上下文：\n参考数据"},
        ])
        self.assertEqual(second.actions[0].payload["messages"][-1]["content"], "任务二")
        parent = MainAgent(PromptBuilder("共同规则"))
        self.assertEqual(first.actions[0].payload["tools"], parent.tools.definitions())
        self.assertIsNone(first.task_result)
        first.state_update["pending_messages"][0]["content"] = "改变任务一"
        self.assertEqual(second.state_update["pending_messages"][0]["content"], "任务二")

    def test_environment_rules_are_used_for_child(self):
        with patch.dict(os.environ, {"LLM_SYSTEM_PROMPT": "环境规则", "LLM_MAX_PROMPT_CHARS": "1000"}):
            self.agent = SubagentAgent()
            output = self.request()
        self.assertEqual(output.actions[0].payload["messages"][0]["content"], "环境规则\n\n" + TASK_PROMPT)

    def test_only_final_reply_emits_complete_task_result_after_worker_restore(self):
        first = self.request()
        state = first.state_update
        call = {
            "id": "call-1", "type": "function",
            "function": {"name": "agent_status", "arguments": "{}"},
        }
        tool = self.result(state, {"message": "", "tool_calls": [call]})
        self.assertIsNone(tool.task_result)
        self.assertEqual(tool.actions[0].type, "tool.agent_status")
        restored = json.loads(json.dumps(tool.state_update))
        self.agent = SubagentAgent(PromptBuilder("共同规则"))
        continued = self.result(restored, {"status": "active"})
        self.assertIsNone(continued.task_result)
        message = "完整结果" * 4000
        done = self.result(continued.state_update, {"message": message})
        self.assertEqual(done.actions, [])
        self.assertEqual(done.state_update["request_status"], "succeeded")
        self.assertEqual(done.task_result, {"status": "succeeded", "output": {"message": message}})
        self.assertEqual(done.state_update["messages"][-1]["content"], message)
        wire = result_to_wire(done, self.context(done.state_update))
        self.assertEqual(decode_frame(encode_frame(wire))["task_result"], done.task_result)
        with self.assertRaisesRegex(BusinessError, "只接受一个任务"):
            self.request(done.state_update)

    def test_model_failure_and_prompt_limit_emit_failed_task_result(self):
        state = self.request().state_update
        failed = self.result(state, status="failed", error="模型失败")
        self.assertEqual(failed.task_result, {
            "status": "failed", "output": {}, "error": {"kind": "business", "message": "模型失败"},
        })
        self.agent = SubagentAgent(PromptBuilder("", 1))
        failed = self.request()
        self.assertEqual(failed.actions, [])
        self.assertEqual(failed.task_result["status"], "failed")
        self.assertEqual(failed.task_result["error"]["kind"], "business")

    def test_child_dispatches_spawn_and_returns_runtime_rejection_to_model(self):
        state = self.request().state_update
        call = {
            "id": "spawn", "type": "function",
            "function": {"name": "spawn_subagent", "arguments": '{"message":"递归任务"}'},
        }
        spawned = self.result(state, {"message": "", "tool_calls": [call]})
        self.assertEqual([action.type for action in spawned.actions], ["tool.spawn_subagent"])
        continued = self.result(spawned.state_update, status="failed", error="subagent不能递归委派")
        self.assertEqual([action.type for action in continued.actions], ["model.generate"])
        self.assertIsNone(continued.task_result)
        tool = continued.actions[0].payload["messages"][-1]
        self.assertEqual(tool["tool_call_id"], "spawn")
        self.assertEqual(json.loads(tool["content"]), {"error": "subagent不能递归委派"})

    def test_child_rejects_second_task_shared_history_and_invalid_payload(self):
        waiting = self.request().state_update
        with self.assertRaisesRegex(BusinessError, "只接受一个任务"):
            self.request(waiting)
        populated = initial_state() | {"messages": [{"role": "user", "content": "父消息"}]}
        with self.assertRaisesRegex(BusinessError, "空历史"):
            self.request(populated)
        for payload in ({}, {"message": " "}, {"message": 3}, {"message": "任务", "context": []}, {"message": "任务", "name": "不接受"}):
            with self.subTest(payload=payload), self.assertRaises(BusinessError):
                self.request(payload=payload)
        with self.assertRaisesRegex(BusinessError, "只接受subagent.request"):
            self.request(event_type="main.request")

    def test_parent_spawn_ack_and_failed_child_result_continue_as_separate_tools(self):
        parent = MainAgent(PromptBuilder())
        context = self.context(event_type="main.request", agent_id="parent")
        context = replace(context, agent=replace(context.agent, definition=DefinitionRef("main", "1")))
        started = parent.run(context)

        def reply(state, value):
            event = {
                "action_id": state["waiting_action_id"], "action_type": state["waiting_action_type"],
                "execution_id": state["waiting_execution_id"], "status": "succeeded", "result": value,
            }
            current = self.context(state, "action.result", event, agent_id="parent")
            return parent.run(replace(current, agent=replace(current.agent, definition=DefinitionRef("main", "1"))))

        spawn_call = {
            "id": "spawn-call", "type": "function",
            "function": {"name": "spawn_subagent", "arguments": '{"message":"任务"}'},
        }
        spawn = reply(started.state_update, {"message": "", "tool_calls": [spawn_call]})
        self.assertEqual(spawn.actions[0].type, "tool.spawn_subagent")
        ack = reply(spawn.state_update, {"task_id": "task-1", "child_id": "child-1"})
        self.assertEqual(ack.actions[0].type, "model.generate")
        wait_call = {
            "id": "wait-call", "type": "function",
            "function": {"name": "wait_subagent", "arguments": '{"task_id":"task-1"}'},
        }
        wait = reply(ack.state_update, {"message": "", "tool_calls": [wait_call]})
        self.assertEqual(wait.actions[0].type, "tool.wait_subagent")
        task_result = {"task_id": "task-1", "task_status": "failed", "output": {}, "error": {"kind": "business", "message": "子任务失败"}}
        continued = reply(wait.state_update, task_result)
        tools = [message for message in continued.actions[0].payload["messages"] if message["role"] == "tool"]
        self.assertEqual([message["tool_call_id"] for message in tools], ["spawn-call", "wait-call"])
        self.assertEqual(json.loads(tools[-1]["content"]), task_result)

    def test_worker_registers_subagent_and_returns_terminal_result(self):
        first = self.context()
        started = self.agent.run(first)
        waiting = started.state_update
        event = {
            "action_id": waiting["waiting_action_id"], "action_type": "model.generate",
            "execution_id": waiting["waiting_execution_id"], "status": "succeeded",
            "result": {"message": "worker结果"},
        }
        result = self.context(waiting, "action.result", event)
        value = asdict(result)
        value["agent"].pop("binding_error")
        source = Path(__file__).resolve().parents[1] / "src"
        output = subprocess.run(
            [sys.executable, "-m", "agent_runtime.worker"],
            input=encode_frame({"version": 1, "id": result.attempt_id, "context": value}),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10,
            env=os.environ | {"PYTHONPATH": str(source), "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self.assertEqual(output.returncode, 0, output.stderr)
        self.assertEqual(output.stderr, b"")
        self.assertEqual(decode_frame(output.stdout)["result"]["task_result"], {
            "status": "succeeded", "output": {"message": "worker结果"},
        })


class TaskResultProtocolTests(unittest.TestCase):
    def context(self):
        return ExecutionContext(
            AgentSnapshot("child", "child", DefinitionRef("subagent", "1"), "active", None, 0),
            Event("event", "subagent.request", {}, "2026-10-07T00:00:00Z"), "execution", "attempt",
        )

    def test_optional_task_result_keeps_old_wire_and_exact_output(self):
        self.assertEqual(result_to_wire(ExecutionResult(), self.context()), {"state_update": {}, "actions": []})
        for task in (
            {"status": "succeeded", "output": {"number": Decimal("9007199254740993.123456789")}},
            {"status": "failed", "output": {}, "error": {"kind": "business", "message": "失败"}},
            {"status": "cancelled", "output": {}},
            {"status": "cancelled", "output": {}, "error": {"kind": "interrupted", "message": "取消"}},
        ):
            with self.subTest(status=task["status"]):
                wire = result_to_wire(ExecutionResult(task_result=task), self.context())
                self.assertEqual(decode_frame(encode_frame(wire))["task_result"], task)

    def test_invalid_task_result_never_reaches_wire(self):
        good = {"status": "failed", "output": {}, "error": {"kind": "business", "message": "失败"}}
        cases = [
            [], {}, {"status": "running", "output": {}}, {"status": "succeeded", "output": []},
            {"status": "failed", "output": {}}, {"status": "failed", "output": {}, "error": None},
            {"status": "succeeded", "output": {}, "error": good["error"]},
            good | {"extra": True}, good | {"error": {"kind": "other", "message": "失败"}},
            good | {"error": {"kind": "business", "message": " "}},
            good | {"error": {"kind": "business", "message": 3}},
            good | {"error": {"kind": "business", "message": "失败", "extra": 1}},
            {"status": "succeeded", "output": {"value": float("nan")}},
        ]
        for task in cases:
            with self.subTest(task=task), self.assertRaises(ProtocolError):
                result_to_wire(ExecutionResult(task_result=deepcopy(task)), self.context())


if __name__ == "__main__":
    unittest.main()
