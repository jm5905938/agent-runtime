#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
python=python3
if [ -x "$project_dir/python/.venv/bin/python" ]; then
    python="$project_dir/python/.venv/bin/python"
elif [ -x "$project_dir/.venv/bin/python" ]; then
    python="$project_dir/.venv/bin/python"
fi

cd "$project_dir/runtime"
exec go run ./cmd/agent-runtime tui \
    --python "$python" \
    --python-source "$project_dir/python/src" \
    --data-dir "$project_dir/.agent-runtime" \
    --env-file "$project_dir/.env" \
    "$@"
