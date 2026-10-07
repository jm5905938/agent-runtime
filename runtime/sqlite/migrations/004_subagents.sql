CREATE TABLE IF NOT EXISTS subagent_tasks (
    id                      TEXT PRIMARY KEY,
    parent_agent_id         TEXT NOT NULL,
    child_agent_id          TEXT NOT NULL UNIQUE,
    initial_event_id        TEXT NOT NULL,
    completion_execution_id TEXT,
    cancel_requested        INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
    result_json             TEXT,
    FOREIGN KEY (id) REFERENCES actions(id),
    FOREIGN KEY (parent_agent_id) REFERENCES agents(id),
    FOREIGN KEY (child_agent_id) REFERENCES agents(id),
    FOREIGN KEY (initial_event_id) REFERENCES events(id),
    FOREIGN KEY (completion_execution_id) REFERENCES executions(id)
);

CREATE INDEX IF NOT EXISTS idx_subagent_tasks_parent
    ON subagent_tasks(parent_agent_id);
