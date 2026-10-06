@echo off
setlocal EnableExtensions DisableDelayedExpansion

set "project_dir=%~dp0"
set "python=python"
if exist "%project_dir%.venv\Scripts\python.exe" set "python=%project_dir%.venv\Scripts\python.exe"
if exist "%project_dir%python\.venv\Scripts\python.exe" set "python=%project_dir%python\.venv\Scripts\python.exe"

pushd "%project_dir%runtime" || exit /b 1
go run ./cmd/agent-runtime tui --python "%python%" --python-source "%project_dir%python\src" --data-dir "%project_dir%.agent-runtime" --env-file "%project_dir%.env" %*
set "launch_exit_code=%errorlevel%"
popd
exit /b %launch_exit_code%
