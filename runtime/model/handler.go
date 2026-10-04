package model

import (
	"agent-runtime/domain"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	defaultTimeout   = 60 * time.Second
	maxResponseBytes = 1 << 20
	maxMessageBytes  = 256 << 10
)

type Config struct {
	BaseURL string
	APIKey  string
	Model   string
	Timeout time.Duration
}

type Handler struct {
	endpoint string
	apiKey   string
	model    string
	client   *http.Client
}

type message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCalls  []toolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

func NewHandler(config Config) (*Handler, error) {
	endpoint, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || endpoint.Hostname() == "" || endpoint.Opaque != "" ||
		(endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" {
		return nil, errors.New("LLM_BASE_URL必须是没有凭据、查询参数和片段的http或https地址")
	}
	if strings.TrimSpace(config.APIKey) == "" || strings.ContainsAny(config.APIKey, "\r\n") || !utf8.ValidString(config.APIKey) {
		return nil, errors.New("LLM_API_KEY不能为空或包含非法字符")
	}
	if strings.TrimSpace(config.Model) == "" || !utf8.ValidString(config.Model) {
		return nil, errors.New("LLM_MODEL不能为空或包含非法字符")
	}
	if config.Timeout < 0 {
		return nil, errors.New("模型请求超时必须大于0")
	}
	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/chat/completions"
	endpoint.RawPath = ""
	return &Handler{
		endpoint: endpoint.String(), apiKey: config.APIKey, model: config.Model,
		client: &http.Client{
			Timeout: config.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (handler *Handler) Execute(action domain.Action) (map[string]any, error) {
	messages, err := actionMessages(action.Payload["messages"])
	if err != nil {
		return nil, err
	}
	tools, err := actionTools(action.Payload)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(struct {
		Model    string           `json:"model"`
		Messages []message        `json:"messages"`
		Stream   bool             `json:"stream"`
		Tools    []toolDefinition `json:"tools,omitempty"`
	}{Model: handler.model, Messages: messages, Tools: tools})
	if err != nil {
		return nil, errors.New("模型请求编码失败")
	}
	request, err := http.NewRequest(http.MethodPost, handler.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("模型请求创建失败")
	}
	request.Header.Set("Authorization", "Bearer "+handler.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := handler.client.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, errors.New("模型请求超时")
		}
		return nil, errors.New("模型请求失败")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("模型请求失败，http状态码%d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, errors.New("模型回复读取超时")
		}
		return nil, errors.New("模型回复读取失败")
	}
	if len(data) > maxResponseBytes {
		return nil, errors.New("模型回复响应超过1MiB")
	}
	if !utf8.Valid(data) {
		return nil, errors.New("模型回复包含非法utf-8编码")
	}
	var result struct {
		Choices []struct {
			Message struct {
				Content   *string    `json:"content"`
				ToolCalls []toolCall `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, errors.New("模型回复不是有效的chat completions响应")
	}
	if len(result.Choices) > 0 && len(result.Choices[0].Message.ToolCalls) > 0 {
		choice := result.Choices[0].Message
		if err := validateToolCalls(choice.ToolCalls); err != nil {
			return nil, err
		}
		content := ""
		if choice.Content != nil {
			content = *choice.Content
		}
		output := map[string]any{"message": content, "tool_calls": toolCallData(choice.ToolCalls)}
		encoded, err := json.Marshal(output)
		if err != nil || len(encoded) > maxMessageBytes {
			return nil, errors.New("模型工具回复超过256KiB")
		}
		return output, nil
	}
	if len(result.Choices) == 0 || result.Choices[0].Message.Content == nil ||
		strings.TrimSpace(*result.Choices[0].Message.Content) == "" {
		return nil, errors.New("模型回复缺少文本内容")
	}
	content := *result.Choices[0].Message.Content
	encoded, err := json.Marshal(content)
	if err != nil || len(encoded) > maxMessageBytes {
		return nil, errors.New("模型回复文本超过256KiB")
	}
	return map[string]any{"message": content}, nil
}
