
-- 迁移版本记录
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Agent 实例及其持久化状态
CREATE TABLE IF NOT EXISTS agents (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL,
    state      TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 输入事件
CREATE TABLE IF NOT EXISTS events (
    id         TEXT PRIMARY KEY,
    type       TEXT NOT NULL,
    payload    TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_created_at
    ON events(created_at);

-- 一次 Agent 执行
CREATE TABLE IF NOT EXISTS executions (
    id          TEXT PRIMARY KEY,
    agent_id    TEXT NOT NULL,
    event_id    TEXT NOT NULL,
    status      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    started_at  TEXT,
    finished_at TEXT,
    error       TEXT,
    FOREIGN KEY (agent_id) REFERENCES agents(id),
    FOREIGN KEY (event_id) REFERENCES events(id)
);

CREATE INDEX IF NOT EXISTS idx_executions_agent_id
    ON executions(agent_id);

CREATE INDEX IF NOT EXISTS idx_executions_status
    ON executions(status);

-- Agent 产生的 Action
CREATE TABLE IF NOT EXISTS actions (
    id           TEXT PRIMARY KEY,
    execution_id TEXT,
    type         TEXT NOT NULL,
    payload      TEXT NOT NULL DEFAULT '{}',
    created_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES executions(id)
);

CREATE INDEX IF NOT EXISTS idx_actions_execution_id
    ON actions(execution_id);

-- Execution 的状态检查点
CREATE TABLE IF NOT EXISTS checkpoints (
    id           TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    state        TEXT NOT NULL DEFAULT '{}',
    created_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES executions(id)
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_execution_id
    ON checkpoints(execution_id);