package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"agent-runtime/codec"
	"agent-runtime/core"
	"agent-runtime/domain"
)

func validateAgent(
	agent domain.AgentInstance,
) error {
	if strings.TrimSpace(string(agent.ID)) == "" {
		return fmt.Errorf("agent id must not be empty")
	}

	if strings.TrimSpace(agent.Name) == "" {
		return fmt.Errorf("agent name must not be empty")
	}

	if err := agent.Definition.Validate(); err != nil {
		return fmt.Errorf("agent definition: %w", err)
	}

	switch agent.Status {
	case domain.AgentStatusCreated,
		domain.AgentStatusActive,
		domain.AgentStatusPaused,
		domain.AgentStatusTerminating,
		domain.AgentStatusTerminated:
		return nil
	default:
		return fmt.Errorf("invalid agent status %q", agent.Status)
	}
}

func encodeAgentState(
	state map[string]any,
) ([]byte, error) {
	if state == nil {
		state = make(map[string]any)
	}

	encoded, err := codec.Encode(state)
	if err != nil {
		return nil, fmt.Errorf(
			"encode agent state: %w",
			err,
		)
	}

	return encoded, nil
}

func (s *Session) CreateAgent(
	ctx context.Context,
	agent domain.AgentInstance,
) error {
	if err := s.guard(ctx, true); err != nil {
		return err
	}

	if err := validateAgent(agent); err != nil {
		return err
	}

	stateJSON, err := encodeAgentState(agent.State)
	if err != nil {
		return err
	}

	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf(
			"begin create agent: %w",
			err,
		)
	}
	defer tx.Rollback()

	var exists int
	err = tx.QueryRowContext(
		ctx,
		`SELECT 1 FROM agents WHERE id = ?`,
		string(agent.ID),
	).Scan(&exists)

	if err == nil {
		return fmt.Errorf(
			"agent %s: %w",
			agent.ID,
			core.ErrStoreConflict,
		)
	}

	if err != sql.ErrNoRows {
		return fmt.Errorf(
			"check agent identity: %w",
			err,
		)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO agents (
			id,
			name,
			definition_id,
			definition_version,
			status,
			state_json,
			state_version
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		string(agent.ID),
		agent.Name,
		agent.Definition.ID,
		agent.Definition.Version,
		string(agent.Status),
		string(stateJSON),
		strconv.FormatUint(agent.StateVersion, 10),
	)
	if err != nil {
		return fmt.Errorf(
			"insert agent: %w",
			err,
		)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf(
			"commit create agent: %w",
			err,
		)
	}

	return nil
}

func (s *Session) LoadAgent(
	ctx context.Context,
	agentID domain.ID,
) (*domain.AgentInstance, error) {
	if err := s.guard(ctx, false); err != nil {
		return nil, err
	}

	var agent domain.AgentInstance
	var status string
	var stateJSON string
	var stateVersion string

	err := s.backend.db.QueryRowContext(ctx, `
		SELECT
			id,
			name,
			definition_id,
			definition_version,
			status,
			state_json,
			state_version
		FROM agents
		WHERE id = ?
	`, string(agentID)).Scan(
		&agent.ID,
		&agent.Name,
		&agent.Definition.ID,
		&agent.Definition.Version,
		&status,
		&stateJSON,
		&stateVersion,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf(
			"agent %s: %w",
			agentID,
			core.ErrStoreNotFound,
		)
	}
	if err != nil {
		return nil, fmt.Errorf(
			"load agent: %w",
			err,
		)
	}

	if err := codec.Decode(
		[]byte(stateJSON),
		&agent.State,
	); err != nil {
		return nil, fmt.Errorf(
			"decode agent state: %w",
			err,
		)
	}

	version, err := strconv.ParseUint(
		stateVersion,
		10,
		64,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"decode agent state version: %w",
			err,
		)
	}

	agent.Status = domain.AgentStatus(status)
	agent.StateVersion = version

	return &agent, nil
}

func (s *Session) ListAgents(
	ctx context.Context,
) ([]domain.AgentInstance, error) {
	if err := s.guard(ctx, false); err != nil {
		return nil, err
	}

	rows, err := s.backend.db.QueryContext(ctx, `
		SELECT
			id,
			name,
			definition_id,
			definition_version,
			status,
			state_json,
			state_version
		FROM agents
		ORDER BY id
	`)
	if err != nil {
		return nil, fmt.Errorf(
			"list agents: %w",
			err,
		)
	}
	defer rows.Close()

	result := make([]domain.AgentInstance, 0)

	for rows.Next() {
		var agent domain.AgentInstance
		var status string
		var stateJSON string
		var stateVersion string

		if err := rows.Scan(
			&agent.ID,
			&agent.Name,
			&agent.Definition.ID,
			&agent.Definition.Version,
			&status,
			&stateJSON,
			&stateVersion,
		); err != nil {
			return nil, fmt.Errorf(
				"scan agent: %w",
				err,
			)
		}

		if err := codec.Decode(
			[]byte(stateJSON),
			&agent.State,
		); err != nil {
			return nil, fmt.Errorf(
				"decode agent state: %w",
				err,
			)
		}

		version, err := strconv.ParseUint(
			stateVersion,
			10,
			64,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"decode agent state version: %w",
				err,
			)
		}

		agent.Status = domain.AgentStatus(status)
		agent.StateVersion = version
		result = append(result, agent)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate agents: %w",
			err,
		)
	}

	return result, nil
}
