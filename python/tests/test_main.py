from copy import deepcopy
from dataclasses import asdict, replace
import os
from pathlib import Path
import subprocess
import sys
import unittest

from agent_runtime import AgentSnapshot, BusinessError, DefinitionRef, Event, ExecutionContext
from agent_runtime.agents import MainAgent
from agent_runtime.agents.main import initial_state
from agent_runtime.protocol import decode_frame, encode_frame


def context(state=None, *, event_type="main.request", payload=None, event_id="request-1", execution_id="execution-1"):
    return ExecutionContext(
        agent=AgentSnapshot("agent-1", "main", DefinitionRef("main", "1"), "active", state, 0),
        event=Event(event_id, event_type, payload if payload is not None else {"message": "hello"}, "2026-10-03T00:00:00Z"),
        execution_id=execution_id,
        attempt_id="attempt-1",
    )


class MainTests(unittest.TestCase):
    def setUp(self):
        self.agent = MainAgent()

    def waiting(self, message="hello"):
        output = self.agent.run(context(payload={"message": message}))
        return output.state_update, output.actions[0]

    def result_context(self, state, **changes):
        payload = {
            "action_id": state["waiting_action_id"],
            "execution_id": state["request_execution_id"],
            "action_type": "model.generate",
            "status": "succeeded",
            "result": {"message": "reply"},
        }
        payload.update(changes)
        return context(state, event_type="action.result", payload=payload, event_id="result-1", execution_id="execution-2")

    def test_request_and_result_preserve_snapshots(self):
        original = initial_state()
        saved = deepcopy(original)
        output = self.agent.run(context(original, payload={"message": "你好\n🌍"}))
        self.assertEqual(original, saved)
        self.assertEqual(len(output.actions), 1)
        action = output.actions[0]
        self.assertEqual(action.type, "model.generate")
        self.assertEqual(action.payload, {"messages": [{"role": "user", "content": "你好\n🌍"}]})
        waiting = output.state_update
        self.assertEqual(waiting["request_status"], "waiting")
        self.assertEqual(waiting["request_event_id"], "request-1")
        self.assertEqual(waiting["request_execution_id"], "execution-1")
        self.assertEqual(waiting["waiting_action_id"], action.id)
        saved = deepcopy(waiting)
        result = self.agent.run(self.result_context(waiting, result={"message": "模型回答"}))
        self.assertEqual(waiting, saved)
        self.assertEqual(result.actions, [])
        final = waiting | result.state_update
        self.assertEqual(final["request_status"], "succeeded")
        self.assertEqual(final["result"], "模型回答")
        self.assertIsNone(final["waiting_action_id"])
        self.assertIsNone(final["error"])
        self.assertEqual(final["result_event_id"], "result-1")
        self.assertEqual(final["request_event_id"], "request-1")

    def test_next_request_has_no_chat_history(self):
        waiting, _ = self.waiting("first")
        complete = self.agent.run(self.result_context(waiting))
        state = waiting | complete.state_update
        output = self.agent.run(context(state, payload={"message": "second"}, event_id="request-2", execution_id="execution-3"))
        self.assertEqual(output.actions[0].payload, {"messages": [{"role": "user", "content": "second"}]})
        self.assertEqual(set(output.state_update), set(initial_state()))
        self.assertIsNone(output.state_update["result"])
        self.assertIsNone(output.state_update["result_event_id"])
        self.assertEqual(output.state_update["request_event_id"], "request-2")

    def test_empty_message_and_null_or_empty_state(self):
        for state in (None, {}):
            with self.subTest(state=state):
                output = self.agent.run(context(state, payload={"message": ""}))
                self.assertEqual(output.actions[0].payload, {"messages": [{"role": "user", "content": ""}]})

    def test_busy_preserves_request(self):
        state, _ = self.waiting()
        saved = deepcopy(state)
        with self.assertRaisesRegex(BusinessError, "正忙"):
            self.agent.run(context(state, event_id="request-2"))
        self.assertEqual(state, saved)

    def test_stale_and_malformed_results_preserve_request(self):
        state, _ = self.waiting()
        for changes in (
            {"action_id": "stale"}, {"execution_id": "stale"}, {"action_type": "echo"},
            {"status": "unknown"}, {"status": "pending"}, {"status": None},
            {"result": {"message": 42}}, {"result": {}}, {"result": None},
            {"status": "failed", "error": None}, {"status": "failed", "error": ""},
        ):
            with self.subTest(changes=changes):
                saved = deepcopy(state)
                with self.assertRaises(BusinessError):
                    self.agent.run(self.result_context(state, **changes))
                self.assertEqual(state, saved)
        result = self.result_context(state)
        with self.assertRaises(BusinessError):
            self.agent.run(replace(result, event=replace(result.event, payload=None)))
        self.assertEqual(state, saved)

    def test_failed_action_finishes_request_and_accepts_next(self):
        state, _ = self.waiting()
        output = self.agent.run(self.result_context(state, status="failed", error="模型请求失败"))
        final = state | output.state_update
        self.assertEqual(final["request_status"], "failed")
        self.assertEqual(final["error"], "模型请求失败")
        self.assertIsNone(final["result"])
        self.assertIsNone(final["waiting_action_id"])
        self.assertEqual(final["result_event_id"], "result-1")
        self.assertEqual(output.actions, [])
        next_result = self.agent.run(context(final, event_id="request-2"))
        self.assertEqual(next_result.state_update["request_status"], "waiting")
        self.assertIsNone(next_result.state_update["error"])

    def test_duplicate_result_after_completion_is_rejected(self):
        state, _ = self.waiting()
        output = self.agent.run(self.result_context(state))
        with self.assertRaises(BusinessError):
            self.agent.run(self.result_context(state | output.state_update))

    def test_invalid_request_and_unsupported_event(self):
        for payload in ({}, {"message": 4}, {"message": True}, {"message": None}):
            with self.subTest(payload=payload), self.assertRaises(BusinessError):
                self.agent.run(context(payload=payload))
        request = context()
        with self.assertRaises(BusinessError):
            self.agent.run(replace(request, event=replace(request.event, payload=None)))
        with self.assertRaises(BusinessError):
            self.agent.run(context(event_type="echo.request"))

    def test_invalid_state_is_runtime_error(self):
        with self.assertRaisesRegex(ValueError, "request_status"):
            self.agent.run(context({"request_status": "invalid"}))
        for key in ("request_event_id", "request_execution_id", "waiting_action_id"):
            state, _ = self.waiting()
            state[key] = None
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, key):
                self.agent.run(context(state))

    def test_worker_registers_main_and_echo(self):
        main = context()
        echo = replace(
            main,
            agent=replace(main.agent, definition=DefinitionRef("echo", "1")),
            event=replace(main.event, type="echo.request"),
            attempt_id="attempt-2",
        )
        data = b""
        for request in (main, echo):
            value = asdict(request)
            value["agent"].pop("binding_error")
            data += encode_frame({"version": 1, "id": request.attempt_id, "context": value})
        source = Path(__file__).resolve().parents[1] / "src"
        output = subprocess.run(
            [sys.executable, "-m", "agent_runtime.worker"],
            input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10,
            env=os.environ | {"PYTHONPATH": str(source), "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self.assertEqual(output.returncode, 0, output.stderr)
        self.assertEqual(output.stderr, b"")
        replies = [decode_frame(frame) for frame in output.stdout.splitlines(keepends=True)]
        self.assertEqual([reply["id"] for reply in replies], ["attempt-1", "attempt-2"])
        main_action = replies[0]["result"]["actions"][0]
        self.assertEqual(main_action["type"], "model.generate")
        self.assertEqual(main_action["payload"], {"messages": [{"role": "user", "content": "hello"}]})
        self.assertEqual(replies[1]["result"]["actions"][0]["type"], "echo")


if __name__ == "__main__":
    unittest.main()
