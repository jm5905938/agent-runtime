package python

import (
	"encoding/json"
	"fmt"
	"strings"

	"agent-runtime/codec"
	"agent-runtime/core"
	"agent-runtime/domain"
)

// valid区分agent错误和协议交换故障
func decodeResponse(data []byte, input core.ExecutionContext) (result core.ExecutionResult, err error, valid bool) {
	invalid := func(message string) (core.ExecutionResult, error, bool) {
		return core.ExecutionResult{}, failure(domain.ErrorKindRuntime, "python响应无效: "+message, nil), false
	}
	if err := validateJSON(data); err != nil {
		return invalid(shortError(err))
	}
	var fields map[string]any
	if err := codec.Decode(data, &fields); err != nil {
		return invalid(shortError(err))
	}
	if !objectFields(fields, []string{"version", "id"}, "result", "error") {
		return invalid("响应包字段无效")
	}
	_, hasResult := fields["result"]
	_, hasError := fields["error"]
	if hasResult == hasError {
		return invalid("result和error必须且只能出现一个")
	}
	if fields["version"] != json.Number("1") {
		return invalid("version缺失或不受支持")
	}
	if fields["id"] != string(input.AttemptID) {
		return invalid("请求id不匹配")
	}
	if hasError {
		problem, ok := fields["error"].(map[string]any)
		if !ok || !objectFields(problem, []string{"kind", "message"}) {
			return invalid("error字段无效")
		}
		message, ok := problem["message"].(string)
		if !ok || strings.TrimSpace(message) == "" {
			return invalid("error需要kind和message")
		}
		kind, ok := problem["kind"].(string)
		if !ok || (kind != string(domain.ErrorKindBusiness) && kind != string(domain.ErrorKindRuntime)) {
			return invalid("不支持的错误类别")
		}
		return core.ExecutionResult{}, failure(domain.ErrorKind(kind), message, nil), true
	}
	reply, ok := fields["result"].(map[string]any)
	if !ok || !objectFields(reply, []string{"state_update", "actions"}, "task_result") {
		return invalid("result字段无效")
	}
	state, hasState := reply["state_update"].(map[string]any)
	requests, hasActions := reply["actions"].([]any)
	if !hasState || !hasActions {
		return invalid("result需要state_update对象和actions数组")
	}
	result.StateUpdate = state
	result.Actions = make([]domain.Action, 0, len(requests))
	ids := make(map[domain.ID]bool)
	for i, request := range requests {
		fields, ok := request.(map[string]any)
		if !ok || !objectFields(fields, []string{"id", "type", "payload"}, "execution_id") {
			return invalid(fmt.Sprintf("action%d字段无效", i))
		}
		id, hasID := fields["id"].(string)
		typeName, hasType := fields["type"].(string)
		payload, hasPayload := fields["payload"].(map[string]any)
		if !hasID || strings.TrimSpace(id) == "" || !hasType || strings.TrimSpace(typeName) == "" || !hasPayload {
			return invalid(fmt.Sprintf("action%d需要id、type和payload对象", i))
		}
		action := domain.Action{ID: domain.ID(id), Type: typeName, Payload: payload}
		if ids[action.ID] {
			return invalid("action id重复")
		}
		ids[action.ID] = true
		if origin, exists := fields["execution_id"]; exists {
			if origin != string(input.ExecutionID) {
				return invalid("action execution_id不匹配")
			}
			action.BindExecution(input.ExecutionID)
		}
		result.Actions = append(result.Actions, action)
	}
	if value, exists := reply["task_result"]; exists {
		task, ok := value.(map[string]any)
		if !ok || !objectFields(task, []string{"status", "output"}, "error") {
			return invalid("task_result字段无效")
		}
		status, hasStatus := task["status"].(string)
		output, hasOutput := task["output"].(map[string]any)
		if !hasStatus || !hasOutput || (status != string(domain.SubagentStatusSucceeded) &&
			status != string(domain.SubagentStatusFailed) && status != string(domain.SubagentStatusCancelled)) {
			return invalid("task_result需要最终status和output对象")
		}
		result.TaskResult = &domain.SubagentResult{Status: domain.SubagentStatus(status), Output: output}
		problem, hasError := task["error"]
		if status == string(domain.SubagentStatusFailed) && !hasError {
			return invalid("失败的task_result需要error")
		}
		if hasError {
			fields, ok := problem.(map[string]any)
			if status == string(domain.SubagentStatusSucceeded) || !ok || !objectFields(fields, []string{"kind", "message"}) {
				return invalid("task_result.error字段无效")
			}
			kind, hasKind := fields["kind"].(string)
			message, hasMessage := fields["message"].(string)
			if !hasKind || !hasMessage || strings.TrimSpace(message) == "" ||
				(kind != string(domain.ErrorKindBusiness) && kind != string(domain.ErrorKindRuntime) &&
					kind != string(domain.ErrorKindInterrupted) && kind != string(domain.ErrorKindUnknown)) {
				return invalid("task_result.error需要有效kind和非空message")
			}
			result.TaskResult.Error = &domain.Failure{Kind: domain.ErrorKind(kind), Message: message}
		}
	}
	return result, nil, true
}

func objectFields(fields map[string]any, required []string, optional ...string) bool {
	allowed := make(map[string]bool, len(required)+len(optional))
	for _, key := range required {
		if _, exists := fields[key]; !exists {
			return false
		}
		allowed[key] = true
	}
	for _, key := range optional {
		allowed[key] = true
	}
	for key := range fields {
		if !allowed[key] {
			return false
		}
	}
	return true
}
