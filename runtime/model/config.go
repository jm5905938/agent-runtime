package model

import (
	"agent-runtime/internal/envconfig"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const DefaultMaxPromptChars = 100000

const maxSystemPromptBytes = 64 << 10

var configKeys = []string{"LLM_BASE_URL", "LLM_API_KEY", "LLM_MODEL", "LLM_TIMEOUT", "LLM_SYSTEM_PROMPT", "LLM_MAX_PROMPT_CHARS"}

func LoadConfig(path string) (Config, error) {
	values, missingFile, err := envconfig.Load(path, configKeys)
	if err != nil {
		return Config{}, err
	}
	var missing []string
	for _, key := range configKeys[:3] {
		if strings.TrimSpace(values[key]) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		prefix := "缺少模型配置项"
		if missingFile {
			prefix = "未找到.env配置文件，缺少模型配置项"
		}
		return Config{}, fmt.Errorf("%s%s", prefix, strings.Join(missing, "、"))
	}
	timeout := 60 * time.Second
	if value := values["LLM_TIMEOUT"]; value != "" {
		timeout, err = time.ParseDuration(value)
		if err != nil || timeout <= 0 {
			return Config{}, errors.New("LLM_TIMEOUT必须是正数时长")
		}
	}
	maxPromptChars := DefaultMaxPromptChars
	if value := values["LLM_MAX_PROMPT_CHARS"]; value != "" {
		for _, char := range value {
			if char < '0' || char > '9' {
				return Config{}, errors.New("LLM_MAX_PROMPT_CHARS必须是正整数")
			}
		}
		maxPromptChars, err = strconv.Atoi(value)
		if err != nil || maxPromptChars <= 0 {
			return Config{}, errors.New("LLM_MAX_PROMPT_CHARS必须是正整数")
		}
	}
	config := Config{
		BaseURL: values["LLM_BASE_URL"], APIKey: values["LLM_API_KEY"], Model: values["LLM_MODEL"], Timeout: timeout,
		SystemPrompt: values["LLM_SYSTEM_PROMPT"], MaxPromptChars: maxPromptChars,
	}
	if err := validatePromptConfig(config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func validatePromptConfig(config Config) error {
	if !utf8.ValidString(config.SystemPrompt) || strings.ContainsRune(config.SystemPrompt, '\x00') {
		return errors.New("LLM_SYSTEM_PROMPT必须是有效UTF-8文本且不能包含NUL")
	}
	encoded, err := json.Marshal(config.SystemPrompt)
	if err != nil || len(encoded) > maxSystemPromptBytes {
		return errors.New("LLM_SYSTEM_PROMPT的JSON编码不能超过64KiB")
	}
	if config.MaxPromptChars < 0 {
		return errors.New("LLM_MAX_PROMPT_CHARS必须是正整数")
	}
	return nil
}
