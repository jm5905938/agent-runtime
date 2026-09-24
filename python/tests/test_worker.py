from contextlib import redirect_stderr
from decimal import Decimal
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

from agent_runtime import Action, ExecutionResult
from agent_runtime.protocol import MAX_FRAME_BYTES, ProtocolError, decode_frame, encode_frame, parse_request
from agent_runtime.worker import serve

ROOT = Path(__file__).resolve().parents[1]


def request(attempt="attempt-1"):
    return {
        "version": 1,
        "id": attempt,
        "context": {
            "agent": {
                "id": "agent-1", "name": "echo", "definition": {"id": "echo", "version": "1"},
                "status": "active", "state": None, "state_version": 0,
            },
            "event": {"id": "event-1", "type": "echo.request", "payload": {"message": "你好\n🌍"}, "created_at": "2026-09-23T00:00:00.123456789Z"},
            "execution_id": "execution-1", "attempt_id": attempt,
        },
    }


def run_worker(data, script=None):
    command = [sys.executable, "-m", "agent_runtime.worker"] if script is None else [sys.executable, "-c", script]
    return subprocess.run(command, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10,
                          env=os.environ | {"PYTHONPATH": str(ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1"})


class WorkerTests(unittest.TestCase):
    def test_process_is_persistent_and_returns_correlated_frames(self):
        result = run_worker(encode_frame(request()) + encode_frame(request("attempt-2")))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, b"")
        frames = result.stdout.splitlines(keepends=True)
        self.assertEqual(len(frames), 2)
        replies = [decode_frame(frame) for frame in frames]
        self.assertEqual([reply["id"] for reply in replies], ["attempt-1", "attempt-2"])
        for reply in replies:
            self.assertNotIn("error", reply)
            self.assertEqual(reply["result"]["actions"][0]["payload"], {"message": "你好\n🌍"})
        self.assertNotEqual(replies[0]["result"]["actions"][0]["id"], replies[1]["result"]["actions"][0]["id"])

    def test_business_error_does_not_kill_worker(self):
        invalid = request()
        invalid["context"]["event"]["payload"] = {"message": 3}
        result = run_worker(encode_frame(invalid) + encode_frame(request("attempt-2")))
        self.assertEqual(result.returncode, 0, result.stderr)
        first, second = map(json.loads, result.stdout.splitlines())
        self.assertEqual(first["error"]["kind"], "business")
        self.assertNotIn("result", first)
        self.assertIn("result", second)

    def test_unknown_definition_is_runtime_error(self):
        invalid = request()
        invalid["context"]["agent"]["definition"]["version"] = "missing"
        result = run_worker(encode_frame(invalid) + encode_frame(request("attempt-2")))
        first, second = map(json.loads, result.stdout.splitlines())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(first["error"]["kind"], "runtime")
        self.assertNotIn("result", first)
        self.assertIn("result", second)
        self.assertIn(b"Traceback", result.stderr)

    def test_agent_print_and_traceback_stay_on_stderr(self):
        script = """
import sys
from agent_runtime import ExecutionResult
from agent_runtime.worker import serve
class NoisyAgent:
    def run(self, context):
        print("agent diagnostic")
        if context.attempt_id == "attempt-1":
            raise RuntimeError("agent crashed")
        return ExecutionResult()
raise SystemExit(serve(sys.stdin.buffer, sys.stdout.buffer, {("echo", "1"): NoisyAgent()}))
"""
        result = run_worker(encode_frame(request()) + encode_frame(request("attempt-2")), script)
        first, second = map(json.loads, result.stdout.splitlines())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(first["error"]["kind"], "runtime")
        self.assertIn("result", second)
        self.assertNotIn(b"agent diagnostic", result.stdout)
        self.assertIn(b"agent diagnostic", result.stderr)
        self.assertIn(b"Traceback", result.stderr)

    def test_exact_number_round_trip_through_worker_process(self):
        script = """
import sys
from agent_runtime import ExecutionResult
from agent_runtime.worker import serve
class IdentityAgent:
    def run(self, context):
        return ExecutionResult(state_update=context.agent.state)
raise SystemExit(serve(sys.stdin.buffer, sys.stdout.buffer, {("echo", "1"): IdentityAgent()}))
"""
        data = request()
        data["context"]["agent"]["state"] = {
            "integer": 90071992547409931234567890,
            "decimal": Decimal("1.2345678901234567890123456789"),
            "huge": Decimal("1e10000"), "small": Decimal("1e-10000"),
        }
        result = run_worker(encode_frame(data), script)
        self.assertEqual(result.returncode, 0, result.stderr)
        reply = decode_frame(result.stdout)
        self.assertEqual(reply["result"]["state_update"], data["context"]["agent"]["state"])

    def test_malformed_frames_terminate_without_response(self):
        valid = encode_frame(request())
        cases = [
            b"not json\n", valid[:-1], b"\xff\n", b'{"a":"\\ud800"}\n',
            b'{"version":NaN}\n', b'{"version":1,"version":1}\n', b"x" * MAX_FRAME_BYTES + b"\n",
        ]
        for frame in cases:
            with self.subTest(frame=frame[:40]):
                result = run_worker(frame)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(result.stdout, b"")
                self.assertIn("worker请求无效".encode(), result.stderr)

    def test_invalid_envelopes_and_contexts(self):
        changes = [
            lambda x: x.update(version=True), lambda x: x.update(version=2),
            lambda x: x.update(id="different"), lambda x: x.update(extra=True),
            lambda x: x["context"]["agent"].update(state_version=True),
            lambda x: x["context"]["agent"].update(state_version=1 << 64),
            lambda x: x["context"]["agent"].update(state=[]),
            lambda x: x["context"]["event"].update(payload=[]),
            lambda x: x["context"]["event"].update(created_at="2026-09-23"),
            lambda x: x["context"].update(execution_id=""),
        ]
        for change in changes:
            data = request()
            change(data)
            with self.subTest(data=data), self.assertRaises(ProtocolError):
                parse_request(decode_frame(encode_frame(data)))

    def test_output_validation_returns_runtime_error(self):
        cyclic = {}
        cyclic["self"] = cyclic
        outputs = [
            None, ExecutionResult(state_update=None), ExecutionResult(actions=[Action("a", "echo", None)]),
            ExecutionResult(state_update={"x": float("nan")}),
            ExecutionResult(state_update={1: "invalid key"}),
            ExecutionResult(state_update={"x": "\ud800"}),
            ExecutionResult(state_update={"x": Decimal("Infinity")}),
            ExecutionResult(state_update={"x": object()}),
            ExecutionResult(state_update=cyclic),
            ExecutionResult(state_update={"x": "a" * MAX_FRAME_BYTES}),
            ExecutionResult(actions=[Action("same", "echo", {}), Action("same", "echo", {})]),
            ExecutionResult(actions=[Action("a", "echo", {}, execution_id="other")]),
        ]
        class FixedAgent:
            def __init__(self, output):
                self.output = output
            def run(self, context):
                return self.output
        for output in outputs:
            with self.subTest(output_type=type(output).__name__):
                destination = io.BytesIO()
                with redirect_stderr(io.StringIO()):
                    code = serve(io.BytesIO(encode_frame(request())), destination, {("echo", "1"): FixedAgent(output)})
                self.assertEqual(code, 0)
                reply = decode_frame(destination.getvalue())
                self.assertNotIn("result", reply)
                self.assertEqual(reply["error"]["kind"], "runtime")

    def test_frame_size_counts_newline(self):
        value = "a" * (MAX_FRAME_BYTES - 3)
        frame = encode_frame(value)
        self.assertEqual(len(frame), MAX_FRAME_BYTES)
        self.assertEqual(decode_frame(frame), value)
        with self.assertRaises(ProtocolError):
            encode_frame(value + "a")


if __name__ == "__main__":
    unittest.main()
