package sqlite

import (
	"context"
	"database/sql"
	"errors"
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
	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return core.ReceivedEvent{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

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
	err = tx.QueryRowContext(
		ctx,
		`SELECT type, payload_json, created_at FROM events WHERE id = ?`,
		string(event.ID),
	).Scan(&storedType, &storedPayload, &storedCreated)

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return core.ReceivedEvent{}, err
	}

	// event比较
	if err == nil {
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
	err = tx.QueryRowContext(
		ctx,
		`SELECT execution_id, status FROM deliveries WHERE agent_id = ? AND event_id = ?`,
		string(agentID),
		string(event.ID),
	).Scan(&storedExecutionID, &storedStatus)

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return core.ReceivedEvent{}, err
	}

	if err == nil {
		// 已存在
		if err == nil {
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
	if errors.Is(err, sql.ErrNoRows) || storedType == "" {
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
