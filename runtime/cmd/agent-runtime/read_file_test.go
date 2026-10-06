package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agent-runtime/domain"
)

func TestReadFileHandlerReturnsUTF8Content(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "README.md")
	if err := os.WriteFile(file, []byte("hello\n世界"), 0600); err != nil {
		t.Fatal(err)
	}

	result, err := (readFileHandler{rootDir: root}).Execute(
		domain.NewAction(readFileActionType, map[string]any{
			"path": "README.md",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	if result["path"] != "README.md" {
		t.Fatalf("path错误: %#v", result["path"])
	}
	if result["content"] != "hello\n世界" {
		t.Fatalf("content错误: %#v", result["content"])
	}
}

func TestReadFileHandlerRejectsUnsafePaths(t *testing.T) {
	root := t.TempDir()
	handler := readFileHandler{rootDir: root}

	for _, path := range []string{
		"",
		"../outside.txt",
		`C:\Windows\win.ini`,
	} {
		_, err := handler.Execute(
			domain.NewAction(readFileActionType, map[string]any{"path": path}),
		)
		if err == nil {
			t.Fatalf("路径应该被拒绝: %q", path)
		}
	}

	_, err := handler.Execute(
		domain.NewAction(readFileActionType, map[string]any{
			"path": "missing.txt",
		}),
	)
	if err == nil || !strings.Contains(err.Error(), "读取失败") {
		t.Fatalf("不存在文件错误不正确: %v", err)
	}
}

func TestReadFileHandlerRejectsOversizedAndInvalidUTF8(t *testing.T) {
	root := t.TempDir()
	handler := readFileHandler{rootDir: root}

	if err := os.WriteFile(
		filepath.Join(root, "large.txt"),
		[]byte(strings.Repeat("x", maxReadFileBytes+1)),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := handler.Execute(domain.NewAction(
		readFileActionType,
		map[string]any{"path": "large.txt"},
	)); err == nil || !strings.Contains(err.Error(), "超过64KiB") {
		t.Fatalf("超大文件未正确拒绝: %v", err)
	}

	if err := os.WriteFile(
		filepath.Join(root, "invalid.txt"),
		[]byte{0xff, 0xfe},
		0600,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := handler.Execute(domain.NewAction(
		readFileActionType,
		map[string]any{"path": "invalid.txt"},
	)); err == nil || !strings.Contains(err.Error(), "有效UTF-8") {
		t.Fatalf("非法UTF-8文件未正确拒绝: %v", err)
	}
}
