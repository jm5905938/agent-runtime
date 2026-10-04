package python

import (
	"os"
	"strings"
	"testing"
)

const environmentFixture = `import json, os, sys
for line in sys.stdin:
    request = json.loads(line)
    keys = ["AGENT_RUNTIME_TEST_INHERITED", "AGENT_RUNTIME_TEST_OVERRIDE", "AGENT_RUNTIME_TEST_EMPTY", "LLM_SYSTEM_PROMPT", "LLM_MAX_PROMPT_CHARS", "PYTHONPATH", "PYTHONDONTWRITEBYTECODE"]
    state = {key: os.environ.get(key) for key in keys}
    print(json.dumps({"version": 1, "id": request["id"], "result": {"state_update": state, "actions": []}}), flush=True)
`

func TestRunnerEnvironmentOverridesAreCopied(t *testing.T) {
	t.Setenv("AGENT_RUNTIME_TEST_INHERITED", "parent")
	t.Setenv("AGENT_RUNTIME_TEST_OVERRIDE", "parent-value")
	t.Setenv("AGENT_RUNTIME_TEST_EMPTY", "parent-value")
	t.Setenv("LLM_SYSTEM_PROMPT", "parent-prompt")
	t.Setenv("PYTHONPATH", "parent-python-path")
	environment := map[string]string{
		"AGENT_RUNTIME_TEST_OVERRIDE": "worker-value",
		"AGENT_RUNTIME_TEST_EMPTY":    "",
		"LLM_SYSTEM_PROMPT":           "主agent\n当前环境",
		"LLM_MAX_PROMPT_CHARS":        "12345",
		"PYTHONPATH":                  "worker-python-path",
	}
	runner := newTestRunner(t, environmentFixture, Options{Env: environment})
	environment["LLM_SYSTEM_PROMPT"] = "mutated"
	environment["AGENT_RUNTIME_TEST_INHERITED"] = "injected-after-construction"
	result, err := runner.Run(input("ok", "environment"))
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"AGENT_RUNTIME_TEST_INHERITED": "parent",
		"AGENT_RUNTIME_TEST_OVERRIDE":  "worker-value",
		"AGENT_RUNTIME_TEST_EMPTY":     "",
		"LLM_SYSTEM_PROMPT":            "主agent\n当前环境",
		"LLM_MAX_PROMPT_CHARS":         "12345",
		"PYTHONDONTWRITEBYTECODE":      "1",
	} {
		if got := result.StateUpdate[key]; got != want {
			t.Fatalf("worker env %s = %v; want %q", key, got, want)
		}
	}
	if got := result.StateUpdate["PYTHONPATH"]; got != runner.options.SourceDir+string(os.PathListSeparator)+"worker-python-path" {
		t.Fatalf("PYTHONPATH没有在覆盖值前加源码目录: %v", got)
	}
	if os.Getenv("AGENT_RUNTIME_TEST_OVERRIDE") != "parent-value" || os.Getenv("LLM_SYSTEM_PROMPT") != "parent-prompt" {
		t.Fatal("worker env修改了父进程环境")
	}
}

func TestRunnerRejectsInvalidEnvironment(t *testing.T) {
	for _, environment := range []map[string]string{{"": "value"}, {"A=B": "value"}, {"A\x00": "value"}, {"A": "value\x00"}} {
		_, err := NewRunner(Options{Env: environment})
		if err == nil || strings.Contains(err.Error(), "value") {
			t.Fatalf("runner没有安全拒绝无效环境: %v", err)
		}
	}
}
