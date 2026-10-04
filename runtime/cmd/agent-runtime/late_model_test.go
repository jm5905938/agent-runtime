package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"agent-runtime/domain"
	pythonrunner "agent-runtime/python"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 模型工作可以在启动预检后到达，已经运行过的Echo worker也必须采用新的prompt配置。
func TestLateMainInputPreparesModelBeforeClaim(t *testing.T) {
	pythonArgs := chatTestPython(t)
	for _, configured := range []bool{false, true} {
		name := "missing_config_keeps_pending"
		if configured {
			name = "loads_prompt_after_echo_worker_started"
		}
		t.Run(name, func(t *testing.T) {
			var captured mainToolsModelCapture
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				captured.append(mainToolsReadRequest(t, r))
				mainToolsReply(t, w, "late reply")
			}))
			defer server.Close()
			config := chatTestConfig(t, server.URL)
			content, err := os.ReadFile(config)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(config, append(content, []byte("LLM_SYSTEM_PROMPT=late-system\nLLM_MAX_PROMPT_CHARS=50\n")...), 0600); err != nil {
				t.Fatal(err)
			}
			if !configured {
				config = filepath.Join(t.TempDir(), "missing.env")
			}
			runtime := core.NewRuntime()
			worker, err := bindPersistent(context.Background(), runtime, commandOptions{
				request: cli.Request{Command: "run"}, envFile: config,
				python: pythonrunner.Options{Python: pythonArgs[1], SourceDir: pythonArgs[3], Timeout: 10 * time.Second, Stderr: io.Discard},
			})
			if err != nil {
				t.Fatalf("空闲启动不应需要模型配置: %v", err)
			}
			t.Cleanup(func() {
				if err := runtime.Close(context.Background()); err != nil {
					t.Error(err)
				}
				if err := worker.Close(); err != nil {
					t.Error(err)
				}
			})
			echo, err := runtime.CreateAgent("echo", domain.DefinitionRef{ID: "echo", Version: "1"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Submit(echo.ID, domain.NewEvent("echo.request", map[string]any{"message": "start worker"})); err != nil {
				t.Fatal(err)
			}
			if err := runtime.RunUntilIdle(); err != nil {
				t.Fatal(err)
			}
			main, err := runtime.CreateAgent("late-main", domain.DefinitionRef{ID: "main", Version: "1"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Submit(main.ID, domain.NewEvent("main.request", map[string]any{"message": "late input"})); err != nil {
				t.Fatal(err)
			}
			runErr := runtime.RunUntilIdle()
			query, err := runtime.QueryAgent(main.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !configured {
				if runErr == nil || !strings.Contains(runErr.Error(), "缺少模型配置项") {
					t.Fatalf("晚到输入没有预检模型配置: %v", runErr)
				}
				if query.Agent.StateVersion != 0 || len(query.Actions) != 0 || len(query.Deliveries) != 1 ||
					query.Deliveries[0].Delivery.Status != domain.DeliveryStatusPending || query.Deliveries[0].Execution != nil || len(query.Deliveries[0].Attempts) != 0 {
					t.Fatalf("缺配置的输入消耗了执行记录: %+v", query)
				}
				return
			}
			if runErr != nil || query.Agent.State["result"] != "late reply" {
				t.Fatalf("晚到输入没有完成: %+v %v", query, runErr)
			}
			requests := captured.all()
			if len(requests) != 1 {
				t.Fatalf("模型调用次数错误: %d", len(requests))
			}
			mainSQLiteAssertMessages(t, requests[0].Messages, []mainSQLiteMessage{
				{Role: "system", Content: "late-system"}, {Role: "user", Content: "late input"},
			})
		})
	}
}
