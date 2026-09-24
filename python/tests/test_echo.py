from copy import deepcopy
from dataclasses import replace
import unittest

from agent_runtime import AgentSnapshot, BusinessError, DefinitionRef, Event, ExecutionContext
from agent_runtime.agents import EchoAgent, initial_state


def context(state=None, *, event_type="echo.request", payload=None, event_id="request-1", execution_id="execution-1"):
    return ExecutionContext(
        agent=AgentSnapshot("agent-1", "echo", DefinitionRef("echo", "1"), "active", state, 0),
        event=Event(event_id, event_type, payload if payload is not None else {"message": "hello"}, "2026-09-23T00:00:00Z"),
        execution_id=execution_id,
        attempt_id="attempt-1",
    )


class EchoTests(unittest.TestCase):
    def setUp(self):
        self.agent = EchoAgent()

    def waiting(self, message="hello"):
        output = self.agent.run(context(payload={"message": message}))
        return output.state_update, output.actions[0]

    def result_context(self, state, **changes):
        payload = {
            "action_id": state["waiting_action_id"],
            "execution_id": state["request_execution_id"],
            "action_type": "echo",
            "status": "succeeded",
            "result": {"message": "hello"},
        }
        payload.update(changes)
        return context(state, event_type="action.result", payload=payload, event_id="result-1", execution_id="execution-2")

    def test_request_and_result_without_mutating_snapshot(self):
        original = initial_state()
        snapshot = deepcopy(original)
        output = self.agent.run(context(original, payload={"message": "你好\n🌍"}))
        self.assertEqual(original, snapshot)
        self.assertEqual(len(output.actions), 1)
        self.assertEqual(output.actions[0].payload, {"message": "你好\n🌍"})
        waiting = output.state_update
        self.assertEqual(waiting["request_status"], "waiting")
        self.assertEqual(waiting["request_event_id"], "request-1")
        self.assertEqual(waiting["request_execution_id"], "execution-1")
        self.assertEqual(waiting["waiting_action_id"], output.actions[0].id)
        snapshot = deepcopy(waiting)
        result = self.agent.run(self.result_context(waiting, result={"message": "你好\n🌍"}))
        self.assertEqual(waiting, snapshot)
        self.assertEqual(result.actions, [])
        final = waiting | result.state_update
        self.assertEqual(final["request_status"], "succeeded")
        self.assertEqual(final["result"], "你好\n🌍")
        self.assertIsNone(final["waiting_action_id"])
        self.assertIsNone(final["error"])
        self.assertEqual(final["result_event_id"], "result-1")
        self.assertEqual(final["request_event_id"], "request-1")

    def test_empty_message_and_fresh_null_or_empty_state(self):
        for state in (None, {}):
            with self.subTest(state=state):
                output = self.agent.run(context(state, payload={"message": ""}))
                self.assertEqual(output.actions[0].payload, {"message": ""})

    def test_busy_preserves_request(self):
        state, _ = self.waiting()
        snapshot = deepcopy(state)
        with self.assertRaisesRegex(BusinessError, "正忙"):
            self.agent.run(context(state, payload={"message": "second"}, event_id="request-2"))
        self.assertEqual(state, snapshot)

    def test_stale_and_malformed_results_preserve_request(self):
        state, _ = self.waiting()
        for changes in (
            {"action_id": "stale"}, {"execution_id": "stale"}, {"action_type": "other"},
            {"status": "unknown"}, {"status": "pending"}, {"result": {"message": 42}},
            {"result": None}, {"status": "failed", "error": None}, {"status": "failed", "error": ""},
        ):
            with self.subTest(changes=changes):
                snapshot = deepcopy(state)
                with self.assertRaises(BusinessError):
                    self.agent.run(self.result_context(state, **changes))
                self.assertEqual(state, snapshot)

    def test_failed_action_finishes_request_and_accepts_next(self):
        state, _ = self.waiting()
        result = self.agent.run(self.result_context(state, status="failed", error="handler failed"))
        final = state | result.state_update
        self.assertEqual(final["request_status"], "failed")
        self.assertEqual(final["error"], "handler failed")
        self.assertIsNone(final["result"])
        self.assertIsNone(final["waiting_action_id"])
        self.assertEqual(result.actions, [])
        next_result = self.agent.run(context(final, event_id="request-2", execution_id="execution-3"))
        self.assertIsNone(next_result.state_update["error"])
        self.assertIsNone(next_result.state_update["result_event_id"])
        self.assertEqual(next_result.state_update["request_event_id"], "request-2")

    def test_duplicate_result_after_completion_is_rejected(self):
        state, _ = self.waiting()
        result = self.agent.run(self.result_context(state))
        with self.assertRaises(BusinessError):
            self.agent.run(self.result_context(state | result.state_update))

    def test_invalid_request_and_unsupported_event(self):
        for payload in ({}, {"message": 4}, {"message": True}, {"message": None}):
            with self.subTest(payload=payload), self.assertRaises(BusinessError):
                self.agent.run(context(payload=payload))
        ctx = context()
        with self.assertRaises(BusinessError):
            self.agent.run(replace(ctx, event=replace(ctx.event, payload=None)))
        with self.assertRaises(BusinessError):
            self.agent.run(context(event_type="other"))

    def test_missing_waiting_association_is_runtime_error(self):
        state, _ = self.waiting()
        state["request_execution_id"] = None
        with self.assertRaises(ValueError):
            self.agent.run(context(state))


if __name__ == "__main__":
    unittest.main()
