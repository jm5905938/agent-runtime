package model

import (
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	maxToolCalls     = 8
	maxToolIDBytes   = 128
	maxToolNameBytes = 64
)

type toolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function toolFunction `json:"function"`
}

type toolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type toolDefinition struct {
	Type     string             `json:"type"`
	Function functionDefinition `json:"function"`
}

type functionDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters"`
	Strict      *bool          `json:"strict,omitempty"`
}

func validToolName(name string) bool {
	if len(name) == 0 || len(name) > maxToolNameBytes {
		return false
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '_' || character == '-') {
			return false
		}
	}
	return true
}

func validToolID(id string) bool {
	return strings.TrimSpace(id) != "" && len(id) <= maxToolIDBytes && utf8.ValidString(id)
}

func validateToolCalls(calls []toolCall) error {
	if len(calls) == 0 || len(calls) > maxToolCalls {
		return errors.New("模型工具调用需要1至8个function调用")
	}
	ids := make(map[string]bool, len(calls))
	for _, call := range calls {
		if call.Type != "function" || !validToolID(call.ID) || ids[call.ID] ||
			!validToolName(call.Function.Name) || !utf8.ValidString(call.Function.Arguments) {
			return errors.New("模型工具调用需要唯一有效id、function类型和有效名称")
		}
		ids[call.ID] = true
	}
	return nil
}

func actionToolCalls(value any) ([]toolCall, error) {
	items, ok := value.([]any)
	if !ok || len(items) == 0 || len(items) > maxToolCalls {
		return nil, errors.New("模型工具调用需要1至8个function调用")
	}
	calls := make([]toolCall, len(items))
	for index, item := range items {
		fields, ok := item.(map[string]any)
		if !ok {
			return nil, errors.New("模型工具调用需要function对象")
		}
		id, idOK := fields["id"].(string)
		callType, typeOK := fields["type"].(string)
		function, functionOK := fields["function"].(map[string]any)
		name, nameOK := function["name"].(string)
		arguments, argumentsOK := function["arguments"].(string)
		if !idOK || !typeOK || !functionOK || !nameOK || !argumentsOK {
			return nil, errors.New("模型工具调用需要id、type、name和arguments字符串")
		}
		calls[index] = toolCall{ID: id, Type: callType, Function: toolFunction{Name: name, Arguments: arguments}}
	}
	if err := validateToolCalls(calls); err != nil {
		return nil, err
	}
	return calls, nil
}

// JSON业务数据使用map和slice，避免把Go结构体放入持久化ActionResult。
func toolCallData(calls []toolCall) []any {
	data := make([]any, len(calls))
	for index, call := range calls {
		data[index] = map[string]any{
			"id": call.ID, "type": call.Type,
			"function": map[string]any{"name": call.Function.Name, "arguments": call.Function.Arguments},
		}
	}
	return data
}

func actionTools(payload map[string]any) ([]toolDefinition, error) {
	value, exists := payload["tools"]
	if !exists {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return nil, errors.New("模型tools需要非空function定义列表")
	}
	tools := make([]toolDefinition, len(items))
	names := make(map[string]bool, len(items))
	for index, item := range items {
		fields, ok := item.(map[string]any)
		if !ok || fields["type"] != "function" {
			return nil, errors.New("模型tools仅支持function定义")
		}
		function, ok := fields["function"].(map[string]any)
		if !ok {
			return nil, errors.New("模型tools需要function对象")
		}
		name, ok := function["name"].(string)
		if !ok || !validToolName(name) || names[name] {
			return nil, errors.New("模型tools需要唯一有效function名称")
		}
		parameters, ok := function["parameters"].(map[string]any)
		if !ok || parameters == nil || parameters["type"] != "object" || !validJSONText(parameters) {
			return nil, errors.New("模型tools的parameters需要object类型JSON Schema")
		}
		definition := functionDefinition{Name: name, Parameters: parameters}
		if value, exists := function["description"]; exists {
			description, ok := value.(string)
			if !ok || !utf8.ValidString(description) {
				return nil, errors.New("模型tools的description需要utf-8字符串")
			}
			definition.Description = description
		}
		if value, exists := function["strict"]; exists {
			strict, ok := value.(bool)
			if !ok {
				return nil, errors.New("模型tools的strict需要布尔值")
			}
			definition.Strict = &strict
		}
		tools[index] = toolDefinition{Type: "function", Function: definition}
		names[name] = true
	}
	return tools, nil
}

func validJSONText(value any) bool {
	switch value := value.(type) {
	case string:
		return utf8.ValidString(value)
	case map[string]any:
		for key, child := range value {
			if !utf8.ValidString(key) || !validJSONText(child) {
				return false
			}
		}
	case []any:
		for _, child := range value {
			if !validJSONText(child) {
				return false
			}
		}
	}
	_, err := json.Marshal(value)
	return err == nil
}

func actionMessages(value any) ([]message, error) {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return nil, errors.New("模型action需要非空messages列表")
	}
	messages := make([]message, len(items))
	pending := make(map[string]bool)
	for index, item := range items {
		fields, ok := item.(map[string]any)
		if !ok {
			return nil, errors.New("模型消息必须包含role和content字符串")
		}
		role, roleOK := fields["role"].(string)
		content, contentOK := fields["content"].(string)
		if !roleOK {
			return nil, errors.New("模型消息必须包含role和utf-8文本content")
		}
		var calls []toolCall
		if value, exists := fields["tool_calls"]; exists {
			if role != "assistant" {
				return nil, errors.New("仅assistant消息可以包含tool_calls")
			}
			var err error
			calls, err = actionToolCalls(value)
			if err != nil {
				return nil, err
			}
			if !contentOK && fields["content"] == nil {
				content, contentOK = "", true
			}
		}
		if !contentOK || !utf8.ValidString(content) {
			return nil, errors.New("模型消息必须包含role和utf-8文本content")
		}
		if len(pending) > 0 && role != "tool" {
			return nil, errors.New("模型工具调用缺少对应tool结果")
		}
		switch role {
		case "system", "developer", "user", "assistant":
			if _, exists := fields["tool_call_id"]; exists {
				return nil, errors.New("仅tool消息可以包含tool_call_id")
			}
			for _, call := range calls {
				pending[call.ID] = true
			}
		case "tool":
			id, ok := fields["tool_call_id"].(string)
			if !ok || !validToolID(id) || !pending[id] {
				return nil, errors.New("模型tool消息缺少匹配的待处理tool_call_id")
			}
			delete(pending, id)
			messages[index].ToolCallID = id
		default:
			return nil, errors.New("模型消息role仅支持system、developer、user、assistant和tool")
		}
		messages[index].Role, messages[index].Content, messages[index].ToolCalls = role, content, calls
	}
	if len(pending) > 0 {
		return nil, errors.New("模型工具调用缺少对应tool结果")
	}
	return messages, nil
}
