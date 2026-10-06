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
	maxWriteFileBytes   = 64 * 1024
)

type writeFileHandler struct {
	rootDir string
}

func (handler writeFileHandler) Execute(action domain.Action) (map[string]any, error) {
	if len(action.Payload) != 2 {
		return nil, errors.New("write_file需要path和content参数")
	}

	path, pathOK := action.Payload["path"].(string)
	content, contentOK := action.Payload["content"].(string)
	if !pathOK || strings.TrimSpace(path) == "" {
		return nil, errors.New("write_file的path需要非空字符串")
	}
	if !contentOK {
		return nil, errors.New("write_file的content需要字符串")
	}

	contentBytes := []byte(content)
	if len(contentBytes) > maxWriteFileBytes {
		return nil, errors.New("write_file内容超过64KiB")
	}
	if !utf8.Valid(contentBytes) {
		return nil, errors.New("write_file内容不是有效UTF-8")
	}
	if filepath.IsAbs(path) {
		return nil, errors.New("write_file不允许绝对路径")
	}

	root, err := filepath.Abs(handler.rootDir)
	if err != nil {
		return nil, errors.New("write_file根目录无效")
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, errors.New("write_file根目录无效")
	}

	candidate := filepath.Join(root, filepath.Clean(path))
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("write_file路径超出允许目录")
	}

	parent := filepath.Dir(candidate)
	parentResolved, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return nil, fmt.Errorf("write_file父目录不存在: %w", err)
	}
	parentRelative, err := filepath.Rel(root, parentResolved)
	if err != nil || parentRelative == ".." ||
		strings.HasPrefix(parentRelative, ".."+string(filepath.Separator)) {
		return nil, errors.New("write_file路径超出允许目录")
	}

	file, err := os.OpenFile(candidate, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, fmt.Errorf("write_file创建失败: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(contentBytes); err != nil {
		return nil, fmt.Errorf("write_file写入失败: %w", err)
	}

	return map[string]any{
		"path":  filepath.ToSlash(relative),
		"bytes": len(contentBytes),
	}, nil
}
