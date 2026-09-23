package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"agent-runtime/runtime/domain"
)

type AgentStore struct {
	db *sql.DB
}

func NewAgentStore(db *sql.DB) *AgentStore {
	return &AgentStore{db: db}
}

func (s *AgentStore) CreateAgent(
	agent domain.AgentInstance,
) error {
	if agent.State == nil {
		agent.State = make(map[string]any)
	}

	stateJSON, err := json.Marshal(agent.State)
	if err != nil {
		return fmt.Errorf("encode agent state: %w", err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin agent transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO agents (
			id,
			name,
			status,
			state_json
		)
		VALUES (?, ?, ?, ?)
	`,
		string(agent.ID),
		agent.Name,
		string(agent.Status),
		string(stateJSON),
	)
	if err != nil {
		return fmt.Errorf("insert agent: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit agent transaction: %w", err)
	}

	return nil
}

func (s *AgentStore) GetAgent(
	id domain.ID,
) (domain.AgentInstance, error) {
	var agent domain.AgentInstance
	var status string
	var stateJSON string

	err := s.db.QueryRow(`
	SELECT id, name, status, state_json
	FROM agents
	WHERE id = ?
	`, string(id)).Scan(
		&agent.ID,
		&agent.Name,
		&status,
		&stateJSON,
	)
	if err != nil {
		return domain.AgentInstance{},
			fmt.Errorf("load agent: %w", err)
	}

	if err := json.Unmarshal(
		[]byte(stateJSON),
		&agent.State,
	); err != nil {
		return domain.AgentInstance{},
			fmt.Errorf("decode state json: %w", err)
	}

	agent.Status = domain.AgentStatus(status)

	if agent.State == nil {
		agent.State = make(map[string]any)
	}

	return agent, nil
}
