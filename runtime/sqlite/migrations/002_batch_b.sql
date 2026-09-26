CREATE TABLE IF NOT EXISTS deliveries (
    agent_id     TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    status       TEXT NOT NULL,
    receive_seq  INTEGER NOT NULL,
    PRIMARY KEY (agent_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_status_receive_seq
    ON deliveries(status, receive_seq);

CREATE UNIQUE INDEX IF NOT EXISTS idx_deliveries_one_running_agent
    ON deliveries(agent_id)
    WHERE status = 'running';

CREATE TABLE IF NOT EXISTS execution_attempts (
    id                     TEXT PRIMARY KEY,
    execution_id           TEXT NOT NULL,
    number                 TEXT NOT NULL,
    status                 TEXT NOT NULL,
    started_at             TEXT NOT NULL,
    finished_at            TEXT,
    failure_json           TEXT,
    expected_state_version TEXT NOT NULL,
    UNIQUE (execution_id, number),
    FOREIGN KEY (execution_id) REFERENCES executions(id)
);