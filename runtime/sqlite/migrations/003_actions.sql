CREATE TABLE IF NOT EXISTS actions (
    sequence         INTEGER PRIMARY KEY AUTOINCREMENT,
    id               TEXT NOT NULL,
    execution_id     TEXT NOT NULL,
    agent_id         TEXT NOT NULL,
    request_json     TEXT NOT NULL,
    handler_version  TEXT NOT NULL,
    recovery_policy  TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    max_attempts     TEXT NOT NULL,
    status           TEXT NOT NULL,
    attempt_count    TEXT NOT NULL,
    result_event_id  TEXT NOT NULL,
    result_json      TEXT,
    last_error_json  TEXT,
    FOREIGN KEY (execution_id) REFERENCES executions(id),
    FOREIGN KEY (agent_id) REFERENCES agents(id)
);

CREATE TABLE IF NOT EXISTS action_attempts (
    id           TEXT PRIMARY KEY,
    action_id    TEXT NOT NULL,
    number       TEXT NOT NULL,
    status       TEXT NOT NULL,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    failure_json TEXT,
    FOREIGN KEY (action_id) REFERENCES actions(id),
    UNIQUE (action_id, number)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_actions_id
    ON actions(id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_actions_result_event_id
    ON actions(result_event_id);
