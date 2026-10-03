package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"agent-runtime/codec"
	"agent-runtime/core"
	"agent-runtime/domain"
)

func (s *Session) ReceiveEvent(
	ctx context.Context,
	agentID domain.ID,
	event domain.Event,
) (core.ReceivedEvent, error) {
	if err := s.lock(ctx, true); err != nil {
		return core.ReceivedEvent{}, err
	}
	defer s.backend.unlock()

	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return core.ReceivedEvent{}, err
	}
	defer tx.Rollback()

	// agent是否存在
	var exists int
	err = tx.QueryRowContext(
		ctx,
		`SELECT 1 FROM agents WHERE id = ?`,
		string(agentID),
	).Scan(&exists)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.ReceivedEvent{}, core.ErrStoreNotFound //无结果，agent不存在
		}
		return core.ReceivedEvent{}, err
	}

	// event查询
	var (
		storedType    string
		storedPayload string
		storedCreated string
	)
	eventExists := false

	err = tx.QueryRowContext(
		ctx,
		`SELECT type, payload_json, created_at FROM events WHERE id = ?`,
		string(event.ID),
	).Scan(&storedType, &storedPayload, &storedCreated)

	if err == nil {
		// 有event
		eventExists = true
	} else if !errors.Is(err, sql.ErrNoRows) {
		// 数据库错误
		return core.ReceivedEvent{}, err
	}

	if !eventExists {
		var reserved int

		err = tx.QueryRowContext(
			ctx,
			`SELECT 1
			 FROM actions
			 WHERE result_event_id = ?`,
			string(event.ID),
		).Scan(&reserved)

		if err == nil {
			// 已经预留给Action结果
			return core.ReceivedEvent{}, core.ErrStoreConflict
		}

		if !errors.Is(err, sql.ErrNoRows) {
			return core.ReceivedEvent{}, err
		}
	}

	// event比较
	if eventExists {
		// event存在(比较type,payload)
		var storedPayloadValue map[string]any

		var createdAt time.Time
		createdAt, err = time.Parse(time.RFC3339Nano, storedCreated)
		if err = codec.Decode([]byte(storedPayload), &storedPayloadValue); err != nil {
			return core.ReceivedEvent{}, err
		}

		storedEvent := domain.Event{
			ID:        event.ID,
			Type:      storedType,
			Payload:   storedPayloadValue, // payload原为JSON，解析为map
			CreatedAt: createdAt,
		}

		var same bool
		same, err = core.SameEventContent(storedEvent, event)
		if err != nil {
			return core.ReceivedEvent{}, err
		}

		if !same {
			return core.ReceivedEvent{}, core.ErrStoreConflict
		}
	}

	// delivery是否存在
	var (
		storedExecutionID string
		storedStatus      string
	)
	deliveryErr := tx.QueryRowContext(
		ctx,
		`SELECT execution_id, status FROM deliveries WHERE agent_id = ? AND event_id = ?`,
		string(agentID),
		string(event.ID),
	).Scan(&storedExecutionID, &storedStatus)

	if deliveryErr != nil && !errors.Is(deliveryErr, sql.ErrNoRows) {
		return core.ReceivedEvent{}, deliveryErr
	}

	if deliveryErr == nil {
		// 已存在
		if err := tx.Commit(); err != nil {
			return core.ReceivedEvent{}, err
		}
		return core.ReceivedEvent{
			Delivery: domain.Delivery{
				Key:         domain.DeliveryKey{AgentID: agentID, EventID: event.ID},
				ExecutionID: domain.ID(storedExecutionID),
				Status:      domain.DeliveryStatus(storedStatus),
			},
			Duplicate: true,
		}, nil
	}

	// event不存在
	if !eventExists {
		encodedPayload, encErr := codec.Encode(event.Payload)
		if encErr != nil {
			return core.ReceivedEvent{}, encErr
		}

		_, execErr := tx.ExecContext(
			ctx,
			`INSERT INTO events (id, type, payload_json, created_at) VALUES (?, ?, ?, ?)`,
			string(event.ID),
			event.Type,
			string(encodedPayload),
			event.CreatedAt.UTC().Format(time.RFC3339Nano),
		)
		if execErr != nil {
			return core.ReceivedEvent{}, execErr
		}
	}

	// delivery插入
	executionID, genErr := domain.NewID()
	if genErr != nil {
		return core.ReceivedEvent{}, genErr
	}

	// 生成receive_seq
	var maxSeq sql.NullInt64
	seqErr := tx.QueryRowContext(
		ctx,
		`SELECT MAX(receive_seq) FROM deliveries`,
	).Scan(&maxSeq)
	if seqErr != nil {
		return core.ReceivedEvent{}, seqErr
	}

	var receiveSeq int64
	if maxSeq.Valid {
		receiveSeq = maxSeq.Int64 + 1
	} else {
		receiveSeq = 1
	}

	_, execErr := tx.ExecContext(
		ctx,
		`INSERT INTO deliveries (agent_id, event_id, execution_id, status, receive_seq) VALUES(?, ?, ?, ?, ?)`,
		string(agentID),
		string(event.ID),
		string(executionID),
		string(domain.DeliveryStatusPending),
		receiveSeq,
	)
	if execErr != nil {
		return core.ReceivedEvent{}, execErr
	}

	if err := tx.Commit(); err != nil {
		return core.ReceivedEvent{}, err
	}

	return core.ReceivedEvent{
		Delivery: domain.Delivery{
			Key:         domain.DeliveryKey{AgentID: agentID, EventID: event.ID},
			ExecutionID: executionID,
			Status:      domain.DeliveryStatusPending,
		},
		Duplicate: false,
	}, nil
}

// 读delivery
func (s *Session) LoadDelivery(
	ctx context.Context,
	key domain.DeliveryKey,
) (*domain.Delivery, error) {
	if err := s.lock(ctx, false); err != nil {
		return nil, err
	}
	defer s.backend.unlock()

	var (
		executionID string
		status      string
	)
	err := s.backend.db.QueryRowContext(
		ctx,
		`SELECT execution_id, status FROM deliveries WHERE agent_id = ? AND event_id = ?`,
		string(key.AgentID),
		string(key.EventID),
	).Scan(&executionID, &status)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrStoreNotFound
		}
		return nil, err
	}

	return &domain.Delivery{
		Key:         key,
		ExecutionID: domain.ID(executionID),
		Status:      domain.DeliveryStatus(status),
	}, nil
}

// 选deliveries，按 receive_seq排列
func (s *Session) ListDeliveries(
	ctx context.Context,
	statuses ...domain.DeliveryStatus,
) ([]domain.Delivery, error) {
	if err := s.lock(ctx, false); err != nil {
		return nil, err
	}
	defer s.backend.unlock()

	query := `SELECT agent_id, event_id, execution_id, status
	          FROM deliveries`
	args := []any{}

	if len(statuses) > 0 {
		placeholders := make([]string, len(statuses))
		for i, st := range statuses {
			placeholders[i] = "?"
			args = append(args, string(st))
		}
		query += ` WHERE status IN (` + strings.Join(placeholders, ",") + `)`
	}

	query += ` ORDER BY receive_seq`

	rows, err := s.backend.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []domain.Delivery
	for rows.Next() {
		var (
			agentID     string
			eventID     string
			executionID string
			status      string
		)
		if err := rows.Scan(&agentID, &eventID, &executionID, &status); err != nil {
			return nil, err
		}
		result = append(result, domain.Delivery{
			Key: domain.DeliveryKey{
				AgentID: domain.ID(agentID),
				EventID: domain.ID(eventID),
			},
			ExecutionID: domain.ID(executionID),
			Status:      domain.DeliveryStatus(status),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// 读event
func (s *Session) LoadEvent(
	ctx context.Context,
	eventID domain.ID,
) (*domain.Event, error) {
	if err := s.lock(ctx, false); err != nil {
		return nil, err
	}
	defer s.backend.unlock()

	var (
		eventType     string
		payloadJSON   string
		createdAtText string
	)
	err := s.backend.db.QueryRowContext(
		ctx,
		`SELECT type, payload_json, created_at FROM events WHERE id = ?`,
		string(eventID),
	).Scan(&eventType, &payloadJSON, &createdAtText)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrStoreNotFound
		}
		return nil, err
	}

	var payload map[string]any
	if err := codec.Decode([]byte(payloadJSON), &payload); err != nil {
		return nil, err
	}

	createdAt, err := time.Parse(time.RFC3339Nano, createdAtText)
	if err != nil {
		return nil, err
	}

	return &domain.Event{
		ID:        eventID,
		Type:      eventType,
		Payload:   payload,
		CreatedAt: createdAt,
	}, nil
}
