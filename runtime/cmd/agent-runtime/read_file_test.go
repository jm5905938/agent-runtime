package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agent-runtime/domain"
)

func TestDefaultReadFileRoot(t *testing.T) {
	project := t.TempDir()
	source := filepath.Join(project, "python", "src", "agent_runtime")
	if err := os.MkdirAll(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "worker.py"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{project, filepath.Join(project, "runtime")} {
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		t.Run(filepath.Base(directory), func(t *testing.T) {
			t.Chdir(directory)
			if got := defaultReadFileRoot(); got != project {
				t.Fatalf("项目读取根目录错误: got=%q want=%q", got, project)
			}
		})
	}
	t.Run("installed_binary", func(t *testing.T) {
		t.Chdir(t.TempDir())
		if got := defaultReadFileRoot(); got != "." {
			t.Fatalf("独立目录读取根目录错误: %q", got)
		}
	})
}

func TestReadFileHandlerReadsWholeProject(t *testing.T) {
	root := t.TempDir()
	handler := readFileHandler{rootDir: root}
	for _, path := range []string{"README.md", "runtime/main.go", "docs/guide.md", "python/src/agent_runtime/main.py"} {
		file := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(path), 0600); err != nil {
			t.Fatal(err)
		}
		result, err := handler.Execute(domain.NewAction(readFileActionType, map[string]any{"path": path}))
		if err != nil || result["path"] != path || result["content"] != path {
			t.Fatalf("项目文件读取错误: path=%q result=%v err=%v", path, result, err)
		}
	}
}

func TestReadFileHandlerRejectsProjectEscapes(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "project")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(parent, "outside.txt")
	if err := os.WriteFile(outside, []byte("项目外文件"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "outside-link.txt")); err != nil {
		t.Fatal(err)
	}
	handler := readFileHandler{rootDir: root}
	for _, path := range []string{"../outside.txt", "outside-link.txt", outside} {
		if _, err := handler.Execute(domain.NewAction(readFileActionType, map[string]any{"path": path})); err == nil {
			t.Fatalf("项目外路径未被拒绝: %q", path)
		}
	}
}

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
	if err == nil || !strings.Contains(err.Error(), "读取文件失败") {
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
	)); err == nil || !strings.Contains(err.Error(), "超过6KiB") {
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

func TestReadFileHandlerAcceptsFullSizePlainText(t *testing.T) {
	root := t.TempDir()

	if err := os.WriteFile(
		filepath.Join(root, "plain.txt"),
		[]byte(strings.Repeat("x", maxReadFileBytes)),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	result, err := (readFileHandler{rootDir: root}).Execute(domain.NewAction(
		readFileActionType,
		map[string]any{"path": "plain.txt"},
	))
	if err != nil {
		t.Fatalf("上限内的纯文本被误拒: %v", err)
	}
	content, _ := result["content"].(string)
	if len(content) != maxReadFileBytes {
		t.Fatalf("内容长度错误: %d", len(content))
	}
}

func TestReadFileHandlerRejectsEscapedResultOverBudget(t *testing.T) {
	root := t.TempDir()

	if err := os.WriteFile(
		filepath.Join(root, "escaped.txt"),
		[]byte(strings.Repeat("<", maxReadFileBytes)),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	_, err := (readFileHandler{rootDir: root}).Execute(domain.NewAction(
		readFileActionType,
		map[string]any{"path": "escaped.txt"},
	))
	if err == nil || !strings.Contains(err.Error(), "8KiB") {
		t.Fatalf("编码后超预算未被拒绝: %v", err)
	}
}
