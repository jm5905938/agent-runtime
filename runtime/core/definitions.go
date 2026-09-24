package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"context"
	"fmt"
	"reflect"
)

//agent定义
//好吧我实在想不到可以用什么更好的词语，那就放点洋屁吧
func (r *Runtime) RegisterDefinition(ref domain.DefinitionRef, runner AgentRunner) error {
	done, err := r.enter()
	if err != nil {
		return err
	}
	defer done()
	if err := ref.Validate(); err != nil {
		return err
	}
	if _, err := codec.Encode(ref); err != nil {
		return fmt.Errorf("definition记录: %w", err)
	}
	if nilRunner(runner) {
		return fmt.Errorf("注册definition出错: runner不能为空")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.definitions[ref]; exists {
		return fmt.Errorf("definition %s@%s已注册", ref.ID, ref.Version)
	}
	r.definitions[ref] = runner
	return nil
}

func (r *Runtime) CreateAgent(name string, ref domain.DefinitionRef, initialState map[string]any) (AgentSnapshot, error) {
	return r.CreateAgentContext(context.Background(), name, ref, initialState)
}

func (r *Runtime) CreateAgentContext(ctx context.Context, name string, ref domain.DefinitionRef, initialState map[string]any) (AgentSnapshot, error) {
	done, err := r.enter()
	if err != nil {
		return AgentSnapshot{}, err
	}
	defer done()
	if err := ref.Validate(); err != nil {
		return AgentSnapshot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.bindingError(ref); err != nil {
		return AgentSnapshot{}, err
	}
	agent := domain.NewAgentInstance(name)
	agent.Definition, agent.State, agent.Status = ref, initialState, domain.AgentStatusActive
	if err := r.store.CreateAgent(ctx, agent); err != nil {
		return AgentSnapshot{}, err
	}
	return snapshotAgent(agent), nil
}

func (r *Runtime) RestoreAgent(agent domain.AgentInstance) error {
	done, err := r.enter()
	if err != nil {
		return err
	}
	defer done()
	if err := agent.Definition.Validate(); err != nil {
		return err
	}
	if err := validateAgentStatus(agent.Status); err != nil {
		return err
	}
	return r.store.CreateAgent(context.Background(), agent)
}

func validateAgentStatus(status domain.AgentStatus) error {
	switch status {
	case domain.AgentStatusCreated, domain.AgentStatusActive, domain.AgentStatusPaused,
		domain.AgentStatusTerminating, domain.AgentStatusTerminated:
		return nil
	default:
		return fmt.Errorf("agent生命周期无效%q", status)
	}
}

func (r *Runtime) bindingError(ref domain.DefinitionRef) error {
	if _, ok := r.definitions[ref]; !ok {
		return fmt.Errorf("%w: 缺少definition绑定%s@%s", ErrAgentUnavailable, ref.ID, ref.Version)
	}
	return nil
}

func nilRunner(runner AgentRunner) bool {
	if runner == nil {
		return true
	}
	v := reflect.ValueOf(runner)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}
