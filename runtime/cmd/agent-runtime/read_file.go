package main

import (
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
	readFileActionType = "tool.read_file"
	maxReadFileBytes   = 64 * 1024
)

type readFileHandler struct {
	rootDir string
}

func (handler readFileHandler) Execute(action domain.Action) (map[string]any, error) {
	if len(action.Payload) != 1 {
		return nil, errors.New("read_file需要path参数")
	}

	path, ok := action.Payload["path"].(string)
	if !ok || strings.TrimSpace(path) == "" {
		return nil, errors.New("read_file的path需要非空字符串")
	}

	if filepath.IsAbs(path) {
		return nil, errors.New("read_file不允许绝对路径")
	}

	root, err := filepath.Abs(handler.rootDir)
	if err != nil {
		return nil, errors.New("read_file根目录无效")
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, errors.New("read_file根目录无效")
	}

	candidate := filepath.Join(root, filepath.Clean(path))
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("read_file路径超出允许目录")
	}

	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return nil, fmt.Errorf("read_file读取失败: %w", err)
	}

	resolvedRelative, err := filepath.Rel(root, resolved)
	if err != nil || resolvedRelative == ".." ||
		strings.HasPrefix(resolvedRelative, ".."+string(filepath.Separator)) {
		return nil, errors.New("read_file路径超出允许目录")

	}

	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("read_file读取失败: %w", err)
	}
	if info.IsDir() {
		return nil, errors.New("read_file不能读取目录")
	}
	if info.Size() > maxReadFileBytes {
		return nil, errors.New("read_file文件超过64KiB")
	}

	file, err := os.Open(resolved)
	if err != nil {
		return nil, fmt.Errorf("read_file读取失败: %w", err)
	}
	defer file.Close()

	content, err := io.ReadAll(io.LimitReader(file, maxReadFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read_file读取失败: %w", err)
	}
	if len(content) > maxReadFileBytes {
		return nil, errors.New("read_file文件超过64KiB")
	}

	if !utf8.Valid(content) {
		return nil, errors.New("read_file只支持有效UTF-8文本")
	}

	return map[string]any{
		"path":    filepath.ToSlash(relative),
		"content": string(content),
	}, nil
}
