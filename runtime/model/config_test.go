package model

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func clearConfigEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range configKeys {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfigQuotesAndComments(t *testing.T) {
	clearConfigEnvironment(t)
	path := writeConfig(t, "\ufeff#模型配置\r\n\r\nexport LLM_BASE_URL = \"https://example.com/v1\" #地址\r\nLLM_API_KEY='test#key' #密钥\r\nLLM_MODEL = model#version #模型\r\nLLM_TIMEOUT=15s\r\nOTHER_VALUE=literal\r\n")
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.BaseURL != "https://example.com/v1" || config.APIKey != "test#key" || config.Model != "model#version" || config.Timeout != 15*time.Second {
		t.Fatal("引号、注释或超时解析不符")
	}
}

func TestLoadConfigPreservesLiteralValues(t *testing.T) {
	clearConfigEnvironment(t)
	path := writeConfig(t, `LLM_BASE_URL=https://example.com/v1
LLM_API_KEY="literal ${KEY} $(command) # key"
LLM_MODEL='model\name'
`)
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.APIKey != "literal ${KEY} $(command) # key" || config.Model != `model\name` || config.Timeout != 60*time.Second {
		t.Fatal("配置值被插值或默认超时不符")
	}
}

func TestLoadConfigEnvironmentOverridesFile(t *testing.T) {
	clearConfigEnvironment(t)
	path := writeConfig(t, "LLM_BASE_URL=https://file.example/v1\nLLM_API_KEY=file-key\nLLM_MODEL=file-model\nLLM_TIMEOUT=15s\n")
	t.Setenv("LLM_BASE_URL", "https://env.example/v1")
	t.Setenv("LLM_API_KEY", "env-key")
	t.Setenv("LLM_MODEL", "env-model")
	t.Setenv("LLM_TIMEOUT", "30s")
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.BaseURL != "https://env.example/v1" || config.APIKey != "env-key" || config.Model != "env-model" || config.Timeout != 30*time.Second {
		t.Fatal("环境变量没有覆盖文件配置")
	}
	t.Setenv("LLM_API_KEY", "")
	_, err = LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "LLM_API_KEY") {
		t.Fatal("显式空环境变量没有报告缺失配置")
	}
	if strings.Contains(err.Error(), "file-key") || strings.Contains(err.Error(), "env-key") {
		t.Fatal("配置错误泄露密钥")
	}
}

func TestLoadConfigMissingFileCanUseEnvironment(t *testing.T) {
	clearConfigEnvironment(t)
	path := filepath.Join(t.TempDir(), "absent.env")
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "未找到.env") || !strings.Contains(err.Error(), "LLM_BASE_URL") {
		t.Fatal("配置文件缺失没有明确错误")
	}
	t.Setenv("LLM_BASE_URL", "https://env.example/v1")
	t.Setenv("LLM_API_KEY", "env-key")
	t.Setenv("LLM_MODEL", "env-model")
	config, err := LoadConfig(path)
	if err != nil || config.Model != "env-model" || config.Timeout != 60*time.Second {
		t.Fatal("配置文件缺失时没有使用完整环境配置")
	}
}

func TestLoadConfigMissingFieldsAndInvalidTimeout(t *testing.T) {
	clearConfigEnvironment(t)
	_, err := LoadConfig(writeConfig(t, "LLM_API_KEY=test-key\n"))
	if err == nil || !strings.Contains(err.Error(), "LLM_BASE_URL") || !strings.Contains(err.Error(), "LLM_MODEL") || strings.Contains(err.Error(), "test-key") {
		t.Fatal("缺失配置项错误不符")
	}
	for _, timeout := range []string{"bad-secret-duration", "0s", "-1s", "999999999999999999999s"} {
		t.Run(timeout, func(t *testing.T) {
			path := writeConfig(t, "LLM_BASE_URL=https://example.com/v1\nLLM_API_KEY=test-key\nLLM_MODEL=test-model\nLLM_TIMEOUT="+timeout+"\n")
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), "LLM_TIMEOUT") || strings.Contains(err.Error(), timeout) || strings.Contains(err.Error(), "test-key") {
				t.Fatal("无效时长错误不符或泄露配置值")
			}
		})
	}
}

func TestLoadConfigSyntaxErrorsDoNotRevealValues(t *testing.T) {
	clearConfigEnvironment(t)
	for _, line := range []string{"secret-without-assignment", "LLM_API_KEY='secret-unclosed", `LLM_API_KEY="secret" trailing`, "1SECRET=secret-value", strings.Repeat("s", 64*1024)} {
		_, err := LoadConfig(writeConfig(t, "#合法注释\n"+line+"\n"))
		if err == nil || !strings.Contains(err.Error(), "第2行") || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), line) {
			t.Fatal("配置语法错误没有报告行号或泄露配置内容")
		}
	}
}

func TestParseConfigQuotedEscapes(t *testing.T) {
	_, value, err := parseConfigLine(`LLM_MODEL="model\"name\\path\nnext" #模型`)
	if err != nil || value != "model\"name\\path\nnext" {
		t.Fatal("双引号转义解析不符")
	}
}

func TestLoadConfigPromptSettings(t *testing.T) {
	clearConfigEnvironment(t)
	path := writeConfig(t, `LLM_BASE_URL=https://example.com/v1
LLM_API_KEY=test-key
LLM_MODEL=test-model
LLM_SYSTEM_PROMPT="你是主agent。\n记住当前任务。"
LLM_MAX_PROMPT_CHARS=4321
`)
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.SystemPrompt != "你是主agent。\n记住当前任务。" || config.MaxPromptChars != 4321 {
		t.Fatalf("prompt配置解析错误: %#v", config)
	}
	t.Setenv("LLM_SYSTEM_PROMPT", "环境中的prompt\n第二行")
	t.Setenv("LLM_MAX_PROMPT_CHARS", "42")
	config, err = LoadConfig(path)
	if err != nil || config.SystemPrompt != "环境中的prompt\n第二行" || config.MaxPromptChars != 42 {
		t.Fatal("prompt环境变量没有覆盖文件配置")
	}
	t.Setenv("LLM_SYSTEM_PROMPT", "")
	t.Setenv("LLM_MAX_PROMPT_CHARS", "")
	config, err = LoadConfig(path)
	if err != nil || config.SystemPrompt != "" || config.MaxPromptChars != DefaultMaxPromptChars {
		t.Fatal("空prompt配置默认值不符")
	}
}

func TestLoadConfigRejectsInvalidPromptLimit(t *testing.T) {
	clearConfigEnvironment(t)
	path := writeConfig(t, "LLM_BASE_URL=https://example.com/v1\nLLM_API_KEY=test-key\nLLM_MODEL=test-model\n")
	for _, limit := range []string{"0", "-1", "+1", "1.5", " 12", "12 ", "\n12", "１", "999999999999999999999999999999"} {
		t.Run(limit, func(t *testing.T) {
			t.Setenv("LLM_MAX_PROMPT_CHARS", limit)
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), "LLM_MAX_PROMPT_CHARS") || strings.Contains(err.Error(), "test-key") {
				t.Fatalf("无效prompt限制没有安全拒绝: %v", err)
			}
		})
	}
}

func TestSystemPromptValidation(t *testing.T) {
	clearConfigEnvironment(t)
	path := writeConfig(t, "LLM_BASE_URL=https://example.com/v1\nLLM_API_KEY=test-key\nLLM_MODEL=test-model\n")
	boundary := strings.Repeat("a", maxSystemPromptBytes-2)
	for _, test := range []struct {
		name   string
		prompt string
		valid  bool
	}{
		{"empty", "", true},
		{"multiline unicode", "系统指令\n第二行", true},
		{"encoded boundary", boundary, true},
		{"over boundary", boundary + "a", false},
		{"HTML escaping", strings.Repeat("<", maxSystemPromptBytes/6+1), false},
		{"invalid utf8", string([]byte{0xff}), false},
		{"NUL", "private\x00prompt", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.valid {
				encoded, err := json.Marshal(test.prompt)
				if err != nil || len(encoded) > maxSystemPromptBytes {
					t.Fatal("测试中的合法prompt超过编码限制")
				}
			}
			config := Config{BaseURL: "https://example.com/v1", APIKey: "test-key", Model: "test-model", SystemPrompt: test.prompt}
			_, err := NewHandler(config)
			if (err == nil) != test.valid {
				t.Fatalf("NewHandler验证prompt: valid=%v err=%v", test.valid, err)
			}
			if strings.ContainsRune(test.prompt, '\x00') {
				return // Process environments cannot contain NUL.
			}
			t.Setenv("LLM_SYSTEM_PROMPT", test.prompt)
			_, err = LoadConfig(path)
			if (err == nil) != test.valid {
				t.Fatalf("LoadConfig验证prompt: valid=%v err=%v", test.valid, err)
			}
		})
	}
}

func TestNewHandlerRejectsNegativePromptLimit(t *testing.T) {
	_, err := NewHandler(Config{BaseURL: "https://example.com/v1", APIKey: "test-key", Model: "test-model", MaxPromptChars: -1})
	if err == nil || !strings.Contains(err.Error(), "LLM_MAX_PROMPT_CHARS") {
		t.Fatal("负数prompt限制没有拒绝")
	}
}
