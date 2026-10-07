package memory

import (
	"agent-runtime/model"
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

func writeConfig(t *testing.T, extra string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	content := `EMBEDDING_BASE_URL="https://embedding.example/v1" #地址
EMBEDDING_API_KEY='embedding-test-key'
EMBEDDING_MODEL=embedding-model
EMBEDDING_DIMENSIONS=1024
` + extra
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfigFileAndDefaults(t *testing.T) {
	clearConfigEnvironment(t)
	config, err := LoadConfig(writeConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if config.Embedding.BaseURL != "https://embedding.example/v1" || config.Embedding.APIKey != "embedding-test-key" ||
		config.Embedding.Model != "embedding-model" || config.Embedding.Dimensions != 1024 || config.Embedding.Timeout != 60*time.Second {
		t.Fatal("embedding配置或默认超时不符")
	}
	if config.Qdrant.URL != "http://127.0.0.1:6333" || config.Qdrant.APIKey != "" ||
		config.Qdrant.Collection != "memories" || config.Qdrant.Distance != "Cosine" || config.Qdrant.Timeout != 10*time.Second {
		t.Fatal("Qdrant默认配置不符")
	}
}

func TestLoadConfigEnvironmentOverridesFile(t *testing.T) {
	clearConfigEnvironment(t)
	path := writeConfig(t, "QDRANT_URL=https://file.example\nQDRANT_API_KEY=file-qdrant-key\nQDRANT_COLLECTION=file_memories\nQDRANT_DISTANCE=Dot\n")
	for key, value := range map[string]string{
		"EMBEDDING_BASE_URL": "https://env.example/v1", "EMBEDDING_API_KEY": "env-key",
		"EMBEDDING_MODEL": "env-model", "EMBEDDING_DIMENSIONS": "768", "EMBEDDING_TIMEOUT": "15s",
		"QDRANT_URL": "https://env-qdrant.example", "QDRANT_API_KEY": "env-qdrant-key",
		"QDRANT_COLLECTION": "env_memories", "QDRANT_DISTANCE": "Euclid", "QDRANT_TIMEOUT": "3s",
	} {
		t.Setenv(key, value)
	}
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Embedding.BaseURL != "https://env.example/v1" || config.Embedding.APIKey != "env-key" ||
		config.Embedding.Model != "env-model" || config.Embedding.Dimensions != 768 || config.Embedding.Timeout != 15*time.Second ||
		config.Qdrant.URL != "https://env-qdrant.example" || config.Qdrant.APIKey != "env-qdrant-key" ||
		config.Qdrant.Collection != "env_memories" || config.Qdrant.Distance != "Euclid" || config.Qdrant.Timeout != 3*time.Second {
		t.Fatal("环境变量没有覆盖文件配置")
	}
	t.Setenv("QDRANT_API_KEY", "")
	config, err = LoadConfig(path)
	if err != nil || config.Qdrant.APIKey != "" {
		t.Fatal("显式空环境变量没有清除可选密钥")
	}
	t.Setenv("EMBEDDING_API_KEY", "")
	_, err = LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "EMBEDDING_API_KEY") || strings.Contains(err.Error(), "env-key") {
		t.Fatal("显式空环境变量没有报告缺失密钥，或错误泄露配置值")
	}
}

func TestLoadConfigMissingFileCanUseEnvironment(t *testing.T) {
	clearConfigEnvironment(t)
	path := filepath.Join(t.TempDir(), "missing.env")
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "未找到.env") || !strings.Contains(err.Error(), "EMBEDDING_DIMENSIONS") {
		t.Fatal("配置文件缺失没有明确错误")
	}
	t.Setenv("EMBEDDING_BASE_URL", "https://env.example/v1")
	t.Setenv("EMBEDDING_API_KEY", "env-key")
	t.Setenv("EMBEDDING_MODEL", "env-model")
	t.Setenv("EMBEDDING_DIMENSIONS", "1536")
	config, err := LoadConfig(path)
	if err != nil || config.Embedding.Dimensions != 1536 {
		t.Fatal("缺少文件时没有使用完整环境配置")
	}
}

func TestLoadConfigRejectsInvalidValuesWithoutExposingThem(t *testing.T) {
	clearConfigEnvironment(t)
	path := writeConfig(t, "")
	for _, test := range []struct{ key, value string }{
		{"EMBEDDING_DIMENSIONS", "0"}, {"EMBEDDING_DIMENSIONS", "-1"},
		{"EMBEDDING_DIMENSIONS", "+1"}, {"EMBEDDING_DIMENSIONS", "1.5"},
		{"EMBEDDING_DIMENSIONS", " 12"}, {"EMBEDDING_DIMENSIONS", "99999999999999999999999"},
		{"EMBEDDING_TIMEOUT", "0s"}, {"EMBEDDING_TIMEOUT", "-1s"},
		{"EMBEDDING_TIMEOUT", "private-invalid-duration"}, {"QDRANT_TIMEOUT", "0s"},
		{"QDRANT_TIMEOUT", "-1s"}, {"QDRANT_TIMEOUT", "private-invalid-duration"},
		{"EMBEDDING_BASE_URL", "file:///private-value"},
		{"EMBEDDING_BASE_URL", "https://user:private-value@example.com/v1"},
		{"EMBEDDING_BASE_URL", "https://example.com/v1?key=private-value"},
		{"QDRANT_URL", "http://"}, {"QDRANT_URL", "https://example.com/#private-value"},
		{"QDRANT_URL", "https://user:private-value@example.com"},
		{"EMBEDDING_API_KEY", "private-value\n"}, {"EMBEDDING_MODEL", "private-value\r"},
		{"QDRANT_API_KEY", "private-value\n"}, {"QDRANT_COLLECTION", "private-value/path"},
		{"QDRANT_COLLECTION", "private-value\n"}, {"QDRANT_DISTANCE", "private-value"},
	} {
		t.Run(test.key+"/"+test.value, func(t *testing.T) {
			t.Setenv(test.key, test.value)
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), test.key) ||
				strings.Contains(err.Error(), "private-value") || strings.Contains(err.Error(), "embedding-test-key") {
				t.Fatal("无效配置没有报告配置项，或错误泄露配置值")
			}
		})
	}
}

func TestLoadConfigDoesNotUseChatSettings(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("LLM_BASE_URL", "https://chat.example/v1")
	t.Setenv("LLM_API_KEY", "chat-key")
	t.Setenv("LLM_MODEL", "chat-model")
	t.Setenv("LLM_TIMEOUT", "")
	t.Setenv("LLM_SYSTEM_PROMPT", "")
	t.Setenv("LLM_MAX_PROMPT_CHARS", "")
	path := writeConfig(t, "EMBEDDING_DIMENSIONS=invalid\n")
	chat, err := model.LoadConfig(path)
	if err != nil || chat.Model != "chat-model" {
		t.Fatal("无效embedding配置影响了聊天配置读取")
	}
	_, err = LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "EMBEDDING_DIMENSIONS") {
		t.Fatal("memory配置没有独立校验")
	}
	path = filepath.Join(t.TempDir(), "missing.env")
	_, err = LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "EMBEDDING_MODEL") {
		t.Fatal("embedding错误地使用了聊天配置")
	}
}

func TestLoadConfigSyntaxErrorDoesNotRevealValues(t *testing.T) {
	clearConfigEnvironment(t)
	_, err := LoadConfig(writeConfig(t, "QDRANT_API_KEY='private-unclosed-key\n"))
	if err == nil || !strings.Contains(err.Error(), "第5行") || strings.Contains(err.Error(), "private-unclosed-key") {
		t.Fatal("语法错误没有报告行号，或错误泄露配置值")
	}
}
