from copy import deepcopy
from decimal import Decimal
import json
import unittest

from agent_runtime import AgentSnapshot, DefinitionRef, Event, ExecutionContext, ExecutionResult
from agent_runtime.protocol import encode_frame, result_to_wire
from agent_runtime.tool import Tool, ToolRegistry
from agent_runtime.tools import default_tools


def tool_call(name="inspect", arguments="{}"):
    return {
        "id": "call-1", "type": "function",
        "function": {"name": name, "arguments": arguments},
    }


def prepare(arguments, context):
    return arguments | {"agent_id": context.agent.id}


def tool(**changes):
    fields = {
        "name": "inspect", "description": "检查测试数据",
        "parameters": {"type": "object", "properties": {"value": {"type": "number"}}},
        "prepare": prepare,
    }
    return Tool(**(fields | changes))


class ToolTests(unittest.TestCase):
    def setUp(self):
        self.context = ExecutionContext(
            agent=AgentSnapshot("agent-1", "main", DefinitionRef("main", "1"), "active", None, 0),
            event=Event("event-1", "main.request", {"message": "检查"}, "2026-10-04T00:00:00Z"),
            execution_id="execution-1", attempt_id="attempt-1",
        )

    def test_definitions_and_actions_use_same_registered_tools(self):
        registry = ToolRegistry([tool(), tool(name="other")])
        definitions = registry.definitions()
        self.assertEqual([item["function"]["name"] for item in definitions], ["inspect", "other"])
        for name in ("inspect", "other"):
            with self.subTest(name=name):
                action = registry.create_action(tool_call(name, '{"value":3}'), self.context)
                self.assertEqual(action.type, "tool." + name)
                self.assertEqual(action.payload, {"value": 3, "agent_id": "agent-1"})
                self.assertTrue(registry.supports(action.type))
        self.assertFalse(registry.supports("tool.missing"))
        self.assertFalse(registry.supports("model.generate"))
        with self.assertRaisesRegex(ValueError, "不支持的工具"):
            registry.create_action(tool_call("missing"), self.context)

    def test_registry_and_returned_definitions_own_parameter_snapshots(self):
        definition = tool()
        registry = ToolRegistry([definition])
        saved = deepcopy(registry.definitions())
        definition.parameters["properties"]["value"]["type"] = "string"
        exported = registry.definitions()
        exported[0]["function"]["parameters"]["properties"]["value"]["type"] = "boolean"
        exported[0]["function"]["name"] = "changed"
        self.assertEqual(registry.definitions(), saved)
        self.assertEqual(registry.create_action(tool_call(), self.context).type, "tool.inspect")

    def test_invalid_definitions_and_duplicate_names_reject_at_registration(self):
        for changes in (
            {"name": ""}, {"name": "has.dot"}, {"name": "名字"}, {"name": "x" * 65},
            {"description": None}, {"description": "\ud800"},
            {"parameters": []}, {"parameters": {"type": "array"}},
            {"parameters": {"type": "object", "default": float("nan")}},
            {"prepare": None},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                tool(**changes)
        with self.assertRaisesRegex(ValueError, "工具名称重复"):
            ToolRegistry([tool(), tool()])

    def test_invalid_json_never_reaches_parameter_handler(self):
        calls = []

        def capture(arguments, context):
            calls.append(arguments)
            return arguments

        registry = ToolRegistry([tool(prepare=capture)])
        for arguments in (
            "not-json", "null", "[]", '{"value":NaN}', '{"value":Infinity}',
            '{"value":1,"value":2}', '{"value":{"nested":1,"nested":2}}',
            '{"value":"\\ud800"}',
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                registry.create_action(tool_call(arguments=arguments), self.context)
        self.assertEqual(calls, [])

    def test_numeric_arguments_remain_exact_through_action_wire(self):
        registry = ToolRegistry([tool()])
        value = Decimal("9007199254740993.1234567890123456789")
        action = registry.create_action(tool_call(arguments='{"value":' + str(value) + '}'), self.context)
        self.assertEqual(action.payload["value"], value)
        wire = result_to_wire(ExecutionResult(actions=[action]), self.context)
        restored = json.loads(encode_frame(wire), parse_float=Decimal)
        self.assertEqual(restored["actions"][0]["payload"]["value"], value)

    def test_prepared_payload_is_copied_before_becoming_action(self):
        payload = {"nested": [1]}
        registry = ToolRegistry([tool(prepare=lambda arguments, context: payload)])
        first = registry.create_action(tool_call(), self.context)
        second = registry.create_action(tool_call(), self.context)
        self.assertNotEqual(first.id, second.id)
        payload["nested"].append(2)
        first.payload["nested"].append(3)
        self.assertEqual(second.payload, {"nested": [1]})

    def test_default_agent_status_keeps_action_contract(self):
        registry = default_tools()
        action = registry.create_action(tool_call("agent_status"), self.context)
        self.assertEqual(action.type, "tool.agent_status")
        self.assertEqual(action.payload, {"agent_id": "agent-1"})
        action = registry.create_action(tool_call("agent_status", '{"agent_id":"agent-2"}'), self.context)
        self.assertEqual(action.payload, {"agent_id": "agent-2"})


if __name__ == "__main__":
    unittest.main()
