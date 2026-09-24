CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agents (
    id                TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    definition_id     TEXT NOT NULL,
    definition_version TEXT NOT NULL,
    status            TEXT NOT NULL,
    state_json        TEXT NOT NULL,
    state_version     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id         TEXT PRIMARY KEY,
    type       TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS executions (
    id            TEXT PRIMARY KEY,
    agent_id      TEXT NOT NULL,
    event_id      TEXT NOT NULL,
    status        TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    started_at    TEXT,
    finished_at   TEXT,
    error         TEXT,
    attempt_count TEXT NOT NULL,
    result_json   TEXT,
    FOREIGN KEY (agent_id) REFERENCES agents(id),
    FOREIGN KEY (event_id) REFERENCES events(id)
);

CREATE INDEX IF NOT EXISTS idx_executions_agent
    ON executions(agent_id);

CREATE INDEX IF NOT EXISTS idx_executions_status
    ON executions(status);
