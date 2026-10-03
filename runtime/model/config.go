package model

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"
)

var configKeys = []string{"LLM_BASE_URL", "LLM_API_KEY", "LLM_MODEL", "LLM_TIMEOUT"}

func LoadConfig(path string) (Config, error) {
	values, missingFile, err := readConfigFile(path)
	if err != nil {
		return Config{}, err
	}
	for _, key := range configKeys {
		if value, exists := os.LookupEnv(key); exists {
			values[key] = value
		}
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
	return Config{BaseURL: values["LLM_BASE_URL"], APIKey: values["LLM_API_KEY"], Model: values["LLM_MODEL"], Timeout: timeout}, nil
}

func readConfigFile(path string) (map[string]string, bool, error) {
	values := make(map[string]string)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return values, true, nil
	}
	if err != nil {
		return nil, false, errors.New("读取.env配置文件失败")
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 64*1024)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		if lineNumber == 1 {
			line = strings.TrimPrefix(line, "\ufeff")
		}
		key, value, err := parseConfigLine(line)
		if err != nil {
			return nil, false, fmt.Errorf(".env第%d行格式无效", lineNumber)
		}
		if key != "" {
			values[key] = value
		}
	}
	if scanner.Err() != nil {
		return nil, false, fmt.Errorf(".env第%d行过长或读取失败", lineNumber+1)
	}
	return values, false, nil
}

func parseConfigLine(line string) (string, string, error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", nil
	}
	if strings.HasPrefix(line, "export") && len(line) > len("export") && unicode.IsSpace(rune(line[len("export")])) {
		line = strings.TrimSpace(line[len("export"):])
	}
	key, value, hasValue := strings.Cut(line, "=")
	key = strings.TrimSpace(key)
	if !hasValue || !validConfigKey(key) {
		return "", "", errors.New("配置项格式无效")
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return key, "", nil
	}
	if value[0] != '\'' && value[0] != '"' {
		for i, char := range value {
			if char == '#' && (i == 0 || value[i-1] == ' ' || value[i-1] == '\t') {
				value = strings.TrimSpace(value[:i])
				break
			}
		}
		return key, value, nil
	}
	quote := value[0]
	var result strings.Builder
	for i := 1; i < len(value); i++ {
		if value[i] == quote {
			trailing := strings.TrimSpace(value[i+1:])
			if trailing != "" && !strings.HasPrefix(trailing, "#") {
				return "", "", errors.New("引号后格式无效")
			}
			return key, result.String(), nil
		}
		if quote == '"' && value[i] == '\\' {
			i++
			if i == len(value) {
				break
			}
			switch value[i] {
			case 'n':
				result.WriteByte('\n')
			case 'r':
				result.WriteByte('\r')
			case 't':
				result.WriteByte('\t')
			case '\\', '"':
				result.WriteByte(value[i])
			default:
				result.WriteByte('\\')
				result.WriteByte(value[i])
			}
		} else {
			result.WriteByte(value[i])
		}
	}
	return "", "", errors.New("引号未闭合")
}

func validConfigKey(key string) bool {
	for i, char := range key {
		if char != '_' && (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (i == 0 || char < '0' || char > '9') {
			return false
		}
	}
	return key != ""
}
