package main

import (
	"agent-runtime/domain"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteFileHandlerCreatesFile(t *testing.T) {
	root := t.TempDir()

	result, err := (writeFileHandler{rootDir: root}).Execute(
		domain.NewAction(writeFileActionType, map[string]any{
			"path":    "notes.txt",
			"content": "hello\n世界",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(filepath.Join(root, "notes.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "hello\n世界" {
		t.Fatalf("文件内容错误: %q", content)
	}
	if result["path"] != "notes.txt" {
		t.Fatalf("返回路径错误: %#v", result["path"])
	}
}

func TestWriteFileHandlerRejectsExistingFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.txt")
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := (writeFileHandler{rootDir: root}).Execute(
		domain.NewAction(writeFileActionType, map[string]any{
			"path":    "notes.txt",
			"content": "new",
		}),
	)
	if err == nil || !strings.Contains(err.Error(), "创建失败") {
		t.Fatalf("已有文件未被拒绝: %v", err)
	}
}
