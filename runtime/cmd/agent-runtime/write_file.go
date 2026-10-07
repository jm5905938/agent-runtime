package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"agent-runtime/domain"
)

const (
	writeFileActionType = "tool.write_file"
	maxWriteFileBytes   = 4 * 1024
)

type writeFileHandler struct {
	rootDir string
}

func (handler writeFileHandler) Execute(action domain.Action) (map[string]any, error) {
	if len(action.Payload) != 2 {
		return nil, errors.New("请提供path和content参数")
	}

	path, pathOK := action.Payload["path"].(string)
	content, contentOK := action.Payload["content"].(string)
	if !pathOK || strings.TrimSpace(path) == "" {
		return nil, errors.New("path须为非空文本")
	}
	if !contentOK {
		return nil, errors.New("content须为文本")
	}

	contentBytes := []byte(content)
	if len(contentBytes) > maxWriteFileBytes {
		return nil, errors.New("文件内容超过4KiB")
	}
	if !utf8.Valid(contentBytes) {
		return nil, errors.New("文件内容须为有效UTF-8文本")
	}
	if filepath.IsAbs(path) {
		return nil, errors.New("请使用源码目录的相对路径")
	}

	root, err := filepath.Abs(handler.rootDir)
	if err != nil {
		return nil, errors.New("源码目录不可用")
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, errors.New("源码目录不可用")
	}

	candidate := filepath.Join(root, filepath.Clean(path))
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("文件路径超出源码目录")
	}

	parent := filepath.Dir(candidate)
	parentResolved, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return nil, fmt.Errorf("文件父目录不可用: %w", err)
	}
	parentRelative, err := filepath.Rel(root, parentResolved)
	if err != nil || parentRelative == ".." ||
		strings.HasPrefix(parentRelative, ".."+string(filepath.Separator)) {
		return nil, errors.New("文件路径超出源码目录")
	}

	file, err := os.OpenFile(candidate, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, fmt.Errorf("创建文件失败: %w", err)
	}
	written, writeErr := file.Write(contentBytes)
	closeErr := file.Close()
	if writeErr != nil {
		return nil, fmt.Errorf("写入文件失败: %w", writeErr)
	}
	if written != len(contentBytes) {
		return nil, errors.New("文件未完整写入")
	}
	if closeErr != nil {
		return nil, fmt.Errorf("关闭文件失败: %w", closeErr)
	}

	return map[string]any{
		"path":  filepath.ToSlash(relative),
		"bytes": len(contentBytes),
	}, nil
}
