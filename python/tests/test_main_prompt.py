from copy import deepcopy
import json
import os
import unittest
from unittest.mock import patch

from agent_runtime import AgentSnapshot, DefinitionRef, Event, ExecutionContext
from agent_runtime.agents import MainAgent
from agent_runtime.agents.main import initial_state
from agent_runtime.prompt import PromptBuilder


def turn(user, assistant):
    return [
        {"role": "user", "content": user},
        {"role": "assistant", "content": assistant},
    ]


def tool_call():
    return {
        "id": "c", "type": "function",
        "function": {"name": "agent_status", "arguments": "{}"},
    }


class MainPromptTests(unittest.TestCase):
    def setUp(self):
        self.sequence = 0

    def context(self, state=None, event_type="main.request", payload=None):
        self.sequence += 1
        return ExecutionContext(
            agent=AgentSnapshot("agent-1", "main", DefinitionRef("main", "1"), "active", state, 0),
            event=Event(f"event-{self.sequence}", event_type, payload, "2026-10-04T00:00:00Z"),
            execution_id=f"execution-{self.sequence}",
            attempt_id=f"attempt-{self.sequence}",
        )

    def request(self, agent, message, state=None):
        output = agent.run(self.context(state, payload={"message": message}))
        return (state or {}) | output.state_update, output

    def result(self, agent, state, result):
        payload = {
            "action_id": state["waiting_action_id"],
            "action_type": state["waiting_action_type"],
            "execution_id": state["waiting_execution_id"],
            "status": "succeeded", "result": result,
        }
        output = agent.run(self.context(state, "action.result", payload))
        return state | output.state_update, output

    def assert_idle_failure(self, state, output, history):
        self.assertEqual(state["request_status"], "failed")
        self.assertEqual(output.actions, [])
        self.assertEqual(state["messages"], history)
        self.assertNotIn("pending_message", state)
        self.assertEqual(state["pending_messages"], [])
        self.assertEqual(state["pending_tool_calls"], [])
        self.assertIsNone(state["waiting_action_id"])
        self.assertIsNone(state["waiting_action_type"])
        self.assertIsNone(state["waiting_execution_id"])

    def test_environment_system_prompt_precedes_history_and_new_input(self):
        history = turn("past", "reply")
        state = initial_state() | {"messages": deepcopy(history)}
        with patch.dict(os.environ, {"LLM_SYSTEM_PROMPT": "rules", "LLM_MAX_PROMPT_CHARS": "100"}, clear=True):
            next_state, output = self.request(MainAgent(), "now", state)
        self.assertEqual(output.actions[0].payload["messages"], [
            {"role": "system", "content": "rules"},
        ] + history + [{"role": "user", "content": "now"}])
        self.assertEqual(next_state["messages"], history)
        self.assertEqual(next_state["pending_messages"], [{"role": "user", "content": "now"}])

    def test_prompt_pruning_preserves_stored_history_and_action_snapshot(self):
        history = turn("111", "aaa") + turn("222", "bbb") + turn("333", "ccc")
        state = initial_state() | {"messages": deepcopy(history)}
        saved = deepcopy(state)
        agent = MainAgent(PromptBuilder("sys", 24))
        next_state, output = self.request(agent, "new question", state)
        expected = [{"role": "system", "content": "sys"}] + history[-2:] + [
            {"role": "user", "content": "new question"},
        ]
        self.assertEqual(output.actions[0].payload["messages"], expected)
        self.assertEqual(state, saved)
        self.assertEqual(next_state["messages"], history)
        next_state["messages"][-2]["content"] = "changed state"
        self.assertEqual(output.actions[0].payload["messages"], expected)

    def test_fixed_prompt_over_budget_finishes_and_accepts_next_request(self):
        history = turn("old", "reply")
        state = initial_state() | {"messages": deepcopy(history)}
        agent = MainAgent(PromptBuilder("rules", 10))
        failed, output = self.request(agent, "too long", state)
        self.assert_idle_failure(failed, output, history)
        self.assertIn("LLM_MAX_PROMPT_CHARS", failed["error"])
        resumed, output = self.request(agent, "ok", json.loads(json.dumps(failed)))
        self.assertEqual(resumed["request_status"], "waiting")
        self.assertEqual(resumed["messages"], history)
        self.assertEqual(output.actions[0].payload["messages"], [
            {"role": "system", "content": "rules"},
            {"role": "user", "content": "ok"},
        ])

    def test_tool_result_growth_recomputes_budget_before_model_continuation(self):
        history = turn("a" * 10, "b" * 10) + turn("c" * 10, "d" * 10)
        agent = MainAgent(PromptBuilder("sys", 160))
        state, first = self.request(agent, "now", initial_state() | {"messages": deepcopy(history)})
        self.assertEqual(first.actions[0].payload["messages"][1:-1], history)
        state, tool = self.result(agent, state, {"message": "", "tool_calls": [tool_call()]})
        self.assertEqual(tool.actions[0].type, "tool.agent_status")
        state, continued = self.result(agent, state, {"status": "x" * 30})
        expected_current = [
            {"role": "user", "content": "now"},
            {"role": "assistant", "content": "", "tool_calls": [tool_call()]},
            {"role": "tool", "tool_call_id": "c", "content": '{"status":"' + "x" * 30 + '"}'},
        ]
        self.assertEqual(continued.actions[0].type, "model.generate")
        self.assertEqual(continued.actions[0].payload["messages"], [
            {"role": "system", "content": "sys"},
        ] + history[-2:] + expected_current)
        self.assertEqual(state["messages"], history)
        self.assertEqual(state["pending_messages"], expected_current)
        final, _ = self.result(agent, state, {"message": "done"})
        self.assertEqual(final["messages"], history + expected_current + [{"role": "assistant", "content": "done"}])

    def test_tool_trajectory_over_budget_finishes_without_cutting_current_messages(self):
        history = turn("old", "answer")
        agent = MainAgent(PromptBuilder("sys", 100))
        state, _ = self.request(agent, "now", initial_state() | {"messages": deepcopy(history)})
        state, _ = self.result(agent, state, {"message": "", "tool_calls": [tool_call()]})
        failed, output = self.result(agent, state, {"status": "x" * 30})
        self.assert_idle_failure(failed, output, history)
        self.assertIn("LLM_MAX_PROMPT_CHARS", failed["error"])
        resumed, output = self.request(agent, "next", failed)
        self.assertEqual(resumed["request_status"], "waiting")
        self.assertEqual(output.actions[0].payload["messages"][-1], {"role": "user", "content": "next"})


if __name__ == "__main__":
    unittest.main()
