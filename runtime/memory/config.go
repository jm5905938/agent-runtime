package memory

import (
	"agent-runtime/internal/envconfig"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type EmbeddingConfig struct {
	BaseURL    string
	APIKey     string
	Model      string
	Dimensions int
	Timeout    time.Duration
}

type QdrantConfig struct {
	URL        string
	APIKey     string
	Collection string
	Distance   string
	Timeout    time.Duration
}

type Config struct {
	Embedding EmbeddingConfig
	Qdrant    QdrantConfig
}

var configKeys = []string{
	"EMBEDDING_BASE_URL", "EMBEDDING_API_KEY", "EMBEDDING_MODEL", "EMBEDDING_DIMENSIONS", "EMBEDDING_TIMEOUT",
	"QDRANT_URL", "QDRANT_API_KEY", "QDRANT_COLLECTION", "QDRANT_DISTANCE", "QDRANT_TIMEOUT",
}

// memory配置按需加载，embedding模型与聊天模型分别配置。
func LoadConfig(path string) (Config, error) {
	values, missingFile, err := envconfig.Load(path, configKeys)
	if err != nil {
		return Config{}, err
	}
	var missing []string
	for _, key := range configKeys[:4] {
		if strings.TrimSpace(values[key]) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		prefix := "缺少memory配置项"
		if missingFile {
			prefix = "未找到.env配置文件，缺少memory配置项"
		}
		return Config{}, fmt.Errorf("%s%s", prefix, strings.Join(missing, "、"))
	}
	config := Config{
		Embedding: EmbeddingConfig{
			BaseURL: strings.TrimSpace(values["EMBEDDING_BASE_URL"]),
			APIKey:  values["EMBEDDING_API_KEY"], Model: values["EMBEDDING_MODEL"],
		},
		Qdrant: QdrantConfig{
			URL:    strings.TrimSpace(valueOrDefault(values, "QDRANT_URL", "http://127.0.0.1:6333")),
			APIKey: values["QDRANT_API_KEY"], Collection: valueOrDefault(values, "QDRANT_COLLECTION", "memories"),
			Distance: valueOrDefault(values, "QDRANT_DISTANCE", "Cosine"),
		},
	}
	for key, value := range map[string]string{"EMBEDDING_BASE_URL": config.Embedding.BaseURL, "QDRANT_URL": config.Qdrant.URL} {
		endpoint, err := url.Parse(value)
		if err != nil || !utf8.ValidString(value) || endpoint.Hostname() == "" || endpoint.Opaque != "" ||
			(endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil ||
			endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" {
			return Config{}, fmt.Errorf("%s必须是没有凭据、查询参数和片段的http或https地址", key)
		}
	}
	if !validText(config.Embedding.APIKey) || strings.TrimSpace(config.Embedding.APIKey) == "" {
		return Config{}, errors.New("EMBEDDING_API_KEY不能为空或包含非法字符")
	}
	if !validText(config.Embedding.Model) || strings.TrimSpace(config.Embedding.Model) == "" {
		return Config{}, errors.New("EMBEDDING_MODEL不能为空或包含非法字符")
	}
	if !validText(config.Qdrant.APIKey) {
		return Config{}, errors.New("QDRANT_API_KEY不能包含非法字符")
	}
	if !validText(config.Qdrant.Collection) || strings.TrimSpace(config.Qdrant.Collection) == "" ||
		strings.ContainsAny(config.Qdrant.Collection, "/\\") {
		return Config{}, errors.New("QDRANT_COLLECTION需要非空名称且不能包含路径分隔符或控制字符")
	}
	switch config.Qdrant.Distance {
	case "Cosine", "Dot", "Euclid", "Manhattan":
	default:
		return Config{}, errors.New("QDRANT_DISTANCE仅支持Cosine、Dot、Euclid或Manhattan")
	}
	for _, char := range values["EMBEDDING_DIMENSIONS"] {
		if char < '0' || char > '9' {
			return Config{}, errors.New("EMBEDDING_DIMENSIONS必须是正整数")
		}
	}
	config.Embedding.Dimensions, err = strconv.Atoi(values["EMBEDDING_DIMENSIONS"])
	if err != nil || config.Embedding.Dimensions <= 0 {
		return Config{}, errors.New("EMBEDDING_DIMENSIONS必须是正整数")
	}
	config.Embedding.Timeout, err = configTimeout(values, "EMBEDDING_TIMEOUT", 60*time.Second)
	if err != nil {
		return Config{}, err
	}
	config.Qdrant.Timeout, err = configTimeout(values, "QDRANT_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	return config, nil
}

func valueOrDefault(values map[string]string, key, fallback string) string {
	if value := values[key]; value != "" {
		return value
	}
	return fallback
}

func configTimeout(values map[string]string, key string, fallback time.Duration) (time.Duration, error) {
	if values[key] == "" {
		return fallback, nil
	}
	timeout, err := time.ParseDuration(values[key])
	if err != nil || timeout <= 0 {
		return 0, fmt.Errorf("%s必须是正数时长", key)
	}
	return timeout, nil
}

func validText(value string) bool {
	return utf8.ValidString(value) && strings.IndexFunc(value, unicode.IsControl) == -1
}
