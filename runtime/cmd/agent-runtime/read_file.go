package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"agent-runtime/domain"
)

const (
	readFileActionType     = "tool.read_file"
	maxReadFileBytes       = 6 * 1024
	maxReadFileResultBytes = 8 * 1024
)

type readFileHandler struct {
	rootDir string
}

func defaultReadFileRoot() string {
	if source := defaultPythonSource(); source != "" {
		return filepath.Dir(filepath.Dir(source))
	}
	return "."
}

func (handler readFileHandler) Execute(action domain.Action) (map[string]any, error) {
	if len(action.Payload) != 1 {
		return nil, errors.New("请提供path参数")
	}

	path, ok := action.Payload["path"].(string)
	if !ok || strings.TrimSpace(path) == "" {
		return nil, errors.New("path须为非空文本")
	}

	if filepath.IsAbs(path) {
		return nil, errors.New("请使用项目相对路径")
	}

	root, err := filepath.Abs(handler.rootDir)
	if err != nil {
		return nil, errors.New("项目目录不可用")
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, errors.New("项目目录不可用")
	}

	candidate := filepath.Join(root, filepath.Clean(path))
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("文件路径超出项目目录")
	}

	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return nil, fmt.Errorf("读取文件失败: %w", err)
	}

	resolvedRelative, err := filepath.Rel(root, resolved)
	if err != nil || resolvedRelative == ".." ||
		strings.HasPrefix(resolvedRelative, ".."+string(filepath.Separator)) {
		return nil, errors.New("文件路径超出项目目录")

	}

	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("读取文件失败: %w", err)
	}
	if info.IsDir() {
		return nil, errors.New("请选择文件，不能读取目录")
	}
	if info.Size() > maxReadFileBytes {
		return nil, errors.New("文件超过6KiB")
	}

	file, err := os.Open(resolved)
	if err != nil {
		return nil, fmt.Errorf("读取文件失败: %w", err)
	}
	defer file.Close()

	content, err := io.ReadAll(io.LimitReader(file, maxReadFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("读取文件失败: %w", err)
	}
	if len(content) > maxReadFileBytes {
		return nil, errors.New("文件超过6KiB")
	}

	if !utf8.Valid(content) {
		return nil, errors.New("文件须为有效UTF-8文本")
	}

	result := map[string]any{
		"path":    filepath.ToSlash(relative),
		"content": string(content),
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > maxReadFileResultBytes {
		return nil, errors.New("读取结果编码后超过8KiB")
	}
	return result, nil
}
