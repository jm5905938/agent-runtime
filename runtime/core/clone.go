package runtime

import "agent-runtime/runtime/domain"

func cloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneMap(value)

	case []any:
		result := make([]any, len(value))
		for i, item := range value {
			result[i] = cloneValue(item)
		}
		return result

	default:
		return value
	}
}

func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}

	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = cloneValue(value)
	}
	return result
}

func cloneEvent(event domain.Event) domain.Event {
	event.Payload = cloneMap(event.Payload)
	return event
}

func cloneActions(source []domain.Action) []domain.Action {
	result := make([]domain.Action, len(source))

	for i, action := range source {
		result[i] = action
		result[i].Payload = cloneMap(action.Payload)

		if action.ExecutionID != nil {
			result[i].BindExecution(*action.ExecutionID)
		}
	}

	return result
}
func cloneAgent(agent domain.AgentInstance) domain.AgentInstance {
	agent.State = cloneMap(agent.State)
	return agent
}
