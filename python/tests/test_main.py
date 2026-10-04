from copy import deepcopy
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from agent_runtime import AgentSnapshot, BusinessError, DefinitionRef, Event, ExecutionContext
from agent_runtime.agents import MainAgent
from agent_runtime.agents.main import MAX_HISTORY_BYTES, MAX_HISTORY_TURNS, MAX_MESSAGE_BYTES, TOOLS, initial_state
from agent_runtime.protocol import decode_frame, encode_frame


def context(state=None, *, event_type="main.request", payload=None, event_id="request-1", execution_id="execution-1"):
    return ExecutionContext(
        agent=AgentSnapshot("agent-1", "main", DefinitionRef("main", "1"), "active", state, 0),
        event=Event(event_id, event_type, payload if payload is not None else {"message": "hello"}, "2026-10-03T00:00:00Z"),
        execution_id=execution_id,
        attempt_id="attempt-1",
    )


def go_json_bytes(value):
    """Match encoding/json's compact UTF-8 output and default HTML escaping."""
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    for character, replacement in (
        ("&", "\\u0026"), ("<", "\\u003c"), (">", "\\u003e"),
        ("\u2028", "\\u2028"), ("\u2029", "\\u2029"),
    ):
        encoded = encoded.replace(character, replacement)
    return encoded.encode("utf-8")


class MainTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.dict(os.environ, {"LLM_SYSTEM_PROMPT": "", "LLM_MAX_PROMPT_CHARS": ""}))
        self.agent = MainAgent()

    def waiting(self, message="hello", state=None):
        output = self.agent.run(context(state, payload={"message": message}))
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

    def test_initial_history_is_empty_and_not_shared(self):
        first = initial_state()
        second = initial_state()
        self.assertEqual(first["messages"], [])
        self.assertIsNone(first["pending_message"])
        first["messages"].append({"role": "user", "content": "first"})
        self.assertEqual(second["messages"], [])

    def test_request_and_result_preserve_snapshots(self):
        original = initial_state()
        saved = deepcopy(original)
        output = self.agent.run(context(original, payload={"message": "你好\n🌍"}))
        self.assertEqual(original, saved)
        self.assertEqual(len(output.actions), 1)
        action = output.actions[0]
        self.assertEqual(action.type, "model.generate")
        self.assertEqual(action.payload, {"messages": [{"role": "user", "content": "你好\n🌍"}], "tools": TOOLS})
        waiting = output.state_update
        self.assertEqual(waiting["request_status"], "waiting")
        self.assertEqual(waiting["request_event_id"], "request-1")
        self.assertEqual(waiting["request_execution_id"], "execution-1")
        self.assertEqual(waiting["waiting_action_id"], action.id)
        self.assertEqual(waiting["messages"], [])
        self.assertEqual(waiting["pending_message"], "你好\n🌍")
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
        self.assertEqual(final["messages"], [
            {"role": "user", "content": "你好\n🌍"},
            {"role": "assistant", "content": "模型回答"},
        ])
        self.assertIsNone(final["pending_message"])

    def test_next_request_uses_completed_history_after_state_round_trip(self):
        waiting, _ = self.waiting("first")
        complete = self.agent.run(self.result_context(waiting))
        state = json.loads(json.dumps(waiting | complete.state_update))
        history = [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "reply"},
        ]
        saved = deepcopy(state)
        output = self.agent.run(context(state, payload={"message": "second"}, event_id="request-2", execution_id="execution-3"))
        self.assertEqual(state, saved)
        self.assertEqual(output.actions[0].payload, {"messages": history + [{"role": "user", "content": "second"}], "tools": TOOLS})
        self.assertEqual(output.state_update["messages"], history)
        self.assertEqual(output.state_update["pending_message"], "second")
        self.assertEqual(set(output.state_update), set(initial_state()))
        self.assertIsNone(output.state_update["result"])
        self.assertIsNone(output.state_update["result_event_id"])
        self.assertEqual(output.state_update["request_event_id"], "request-2")
        second = self.agent.run(self.result_context(output.state_update, result={"message": "second reply"}))
        final = output.state_update | second.state_update
        self.assertEqual(final["messages"], history + [
            {"role": "user", "content": "second"},
            {"role": "assistant", "content": "second reply"},
        ])
        self.assertIsNone(final["pending_message"])

    def test_request_history_and_action_are_independent_snapshots(self):
        history = [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "reply"},
        ]
        state = initial_state() | {"messages": deepcopy(history)}
        output = self.agent.run(context(state, payload={"message": "second"}))
        action_messages = output.actions[0].payload["messages"]
        state["messages"][0]["content"] = "changed input"
        self.assertEqual(output.state_update["messages"], history)
        self.assertEqual(action_messages, history + [{"role": "user", "content": "second"}])
        action_messages[1]["content"] = "changed action"
        self.assertEqual(output.state_update["messages"], history)
        output.state_update["messages"][0]["content"] = "changed update"
        self.assertEqual(action_messages[0], history[0])

    def test_completed_history_is_an_independent_snapshot(self):
        history = [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "reply"},
        ]
        state, _ = self.waiting("second", initial_state() | {"messages": deepcopy(history)})
        saved = deepcopy(state)
        complete = self.agent.run(self.result_context(state, result={"message": "second reply"}))
        self.assertEqual(state, saved)
        complete.state_update["messages"][0]["content"] = "changed completed state"
        self.assertEqual(state, saved)
        state["messages"][1]["content"] = "changed waiting state"
        self.assertEqual(complete.state_update["messages"][1], history[1])

    def test_empty_message_and_null_or_empty_state(self):
        for state in (None, {}):
            with self.subTest(state=state):
                output = self.agent.run(context(state, payload={"message": ""}))
                self.assertEqual(output.actions[0].payload, {"messages": [{"role": "user", "content": ""}], "tools": TOOLS})

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
        history = [
            {"role": "user", "content": "earlier"},
            {"role": "assistant", "content": "earlier reply"},
        ]
        state, _ = self.waiting("failed user message", initial_state() | {"messages": deepcopy(history)})
        saved = deepcopy(state)
        output = self.agent.run(self.result_context(state, status="failed", error="模型请求失败"))
        self.assertEqual(state, saved)
        final = state | output.state_update
        self.assertEqual(final["request_status"], "failed")
        self.assertEqual(final["error"], "模型请求失败")
        self.assertIsNone(final["result"])
        self.assertIsNone(final["waiting_action_id"])
        self.assertEqual(final["result_event_id"], "result-1")
        self.assertEqual(output.actions, [])
        self.assertEqual(final["messages"], history)
        self.assertIsNone(final["pending_message"])
        next_result = self.agent.run(context(final, event_id="request-2"))
        self.assertEqual(next_result.state_update["request_status"], "waiting")
        self.assertIsNone(next_result.state_update["error"])
        self.assertEqual(next_result.actions[0].payload["messages"], history + [{"role": "user", "content": "hello"}])
        output.state_update["messages"][0]["content"] = "changed failed state"
        self.assertEqual(state, saved)

    def test_duplicate_result_after_completion_is_rejected(self):
        state, _ = self.waiting()
        output = self.agent.run(self.result_context(state))
        final = state | output.state_update
        saved = deepcopy(final)
        with self.assertRaises(BusinessError):
            self.agent.run(self.result_context(final))
        self.assertEqual(final, saved)

    def test_history_limit_keeps_recent_complete_turns(self):
        state = initial_state()
        complete_history = []
        for number in range(MAX_HISTORY_TURNS + 3):
            user = {"role": "user", "content": f"question {number}"}
            assistant = {"role": "assistant", "content": f"answer {number}"}
            request = self.agent.run(context(
                state, payload={"message": user["content"]},
                event_id=f"request-{number}", execution_id=f"execution-{number}",
            ))
            self.assertEqual(request.actions[0].payload["messages"], state["messages"] + [user])
            result = self.agent.run(self.result_context(request.state_update, result={"message": assistant["content"]}))
            state = request.state_update | result.state_update
            complete_history.extend((user, assistant))
            self.assertEqual(state["messages"], complete_history[-MAX_HISTORY_TURNS * 2:])
            self.assertIsNone(state["pending_message"])

    def test_history_byte_limit_counts_utf8_and_go_json_escaping(self):
        state = initial_state()
        complete_history = []
        for number in range(7):
            user = {"role": "user", "content": f"{number}:" + "<界\n\u2028" * 3000}
            assistant = {"role": "assistant", "content": f"{number}:" + "&界\u2029" * 3000}
            self.assertLess(len(go_json_bytes(user["content"])), MAX_MESSAGE_BYTES)
            request = self.agent.run(context(state, payload={"message": user["content"]}))
            expected_input = deepcopy(state["messages"])
            while expected_input and len(go_json_bytes(expected_input)) + len(go_json_bytes([user])) > MAX_HISTORY_BYTES:
                expected_input = expected_input[2:]
            self.assertEqual(request.actions[0].payload["messages"], expected_input + [user])
            self.assertEqual(request.state_update["messages"], state["messages"])
            result = self.agent.run(self.result_context(request.state_update, result={"message": assistant["content"]}))
            state = request.state_update | result.state_update
            complete_history.extend((user, assistant))
            expected = complete_history[-8:]
            self.assertEqual(state["messages"], expected)
            self.assertLessEqual(len(go_json_bytes(state["messages"])), MAX_HISTORY_BYTES)
            if len(complete_history) >= 10:
                self.assertGreater(len(go_json_bytes(complete_history[-10:])), MAX_HISTORY_BYTES)

    def test_history_byte_limit_can_drop_an_oversized_complete_turn(self):
        state, _ = self.waiting("small question")
        answer = "界" * (MAX_HISTORY_BYTES // 3 + 1)
        result = self.agent.run(self.result_context(state, result={"message": answer}))
        final = state | result.state_update
        self.assertEqual(final["messages"], [])
        self.assertEqual(final["result"], answer)
        self.assertIsNone(final["pending_message"])
        next_request = self.agent.run(context(final, payload={"message": "next"}))
        self.assertEqual(next_request.actions[0].payload["messages"], [{"role": "user", "content": "next"}])

    def test_message_size_limit_uses_go_json_string_bytes(self):
        for character, byte_count in (("a", 1), ("界", 3), ("\n", 2), ("<", 6), ("\u2028", 6)):
            with self.subTest(character=character):
                repeats, remainder = divmod(MAX_MESSAGE_BYTES - 2, byte_count)
                message = character * repeats + "a" * remainder
                self.assertEqual(len(go_json_bytes(message)), MAX_MESSAGE_BYTES)
                state = initial_state()
                saved = deepcopy(state)
                request = self.agent.run(context(state, payload={"message": message}))
                self.assertEqual(request.actions[0].payload["messages"], [{"role": "user", "content": message}])
                self.assertEqual(request.state_update["pending_message"], message)
                with self.assertRaises(BusinessError):
                    self.agent.run(context(state, payload={"message": message + "a"}))
                self.assertEqual(state, saved)

    def test_legacy_completed_state_starts_with_empty_history(self):
        state = initial_state() | {"request_status": "succeeded", "result": "old reply"}
        del state["messages"]
        del state["pending_message"]
        saved = deepcopy(state)
        request = self.agent.run(context(state, payload={"message": "new request"}))
        self.assertEqual(state, saved)
        self.assertEqual(request.state_update["messages"], [])
        self.assertEqual(request.state_update["pending_message"], "new request")
        self.assertEqual(request.actions[0].payload["messages"], [{"role": "user", "content": "new request"}])

    def test_legacy_waiting_result_finishes_without_inventing_history(self):
        for changes in ({"result": {"message": "old reply"}}, {"status": "failed", "error": "old failure"}):
            with self.subTest(changes=changes):
                state, _ = self.waiting("unavailable legacy user message")
                del state["messages"]
                del state["pending_message"]
                saved = deepcopy(state)
                result = self.agent.run(self.result_context(state, **changes))
                self.assertEqual(state, saved)
                final = state | result.state_update
                self.assertEqual(final["messages"], [])
                self.assertIsNone(final["pending_message"])
                self.assertEqual(final["request_status"], changes.get("status", "succeeded"))
                next_request = self.agent.run(context(final, payload={"message": "next"}))
                self.assertEqual(next_request.actions[0].payload["messages"], [{"role": "user", "content": "next"}])

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

    def test_invalid_history_is_runtime_error(self):
        for messages in (
            None, {}, "history", ("user", "assistant"), [None],
            [{"role": "user", "content": "unpaired"}],
            [{"role": "assistant", "content": "wrong order"}, {"role": "user", "content": "wrong order"}],
            [{"role": "system", "content": "unsupported"}, {"role": "assistant", "content": "reply"}],
            [{"role": "user", "content": 42}, {"role": "assistant", "content": "reply"}],
            [{"role": "user", "content": "question"}, {"role": "assistant", "content": None}],
            [{"role": "user", "content": "question"}, {"role": "assistant"}],
        ):
            with self.subTest(messages=messages):
                state = initial_state() | {"messages": messages}
                saved = deepcopy(state)
                with self.assertRaisesRegex(ValueError, "messages"):
                    self.agent.run(context(state))
                self.assertEqual(state, saved)

    def test_new_waiting_state_requires_pending_message(self):
        for pending in (None, 42, True, []):
            with self.subTest(pending=pending):
                state, _ = self.waiting()
                state["pending_message"] = pending
                saved = deepcopy(state)
                with self.assertRaisesRegex(ValueError, "pending_message"):
                    self.agent.run(self.result_context(state))
                self.assertEqual(state, saved)
        state, _ = self.waiting()
        del state["pending_message"]
        with self.assertRaisesRegex(ValueError, "pending_message"):
            self.agent.run(self.result_context(state))

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
        self.assertEqual(main_action["payload"], {"messages": [{"role": "user", "content": "hello"}], "tools": TOOLS})
        self.assertEqual(replies[1]["result"]["actions"][0]["type"], "echo")


if __name__ == "__main__":
    unittest.main()
