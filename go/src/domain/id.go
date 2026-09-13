package domain

import (
	"crypto/rand"
	"fmt"
)

// ID 是每个对象的唯一编号。
// 用字符串保存，写入 JSON、日志或数据库都很方便。
type ID string

// NewID 创建一个随机的 UUID 编号。
// UUID 很难重复，可以用来区分不同的事件、执行记录和动作。
func NewID() (ID, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate domain id: %w", err)
	}

	// 下面两行把编号标记成标准的 UUIDv4 格式。
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	return ID(fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])), nil
}

func mustNewID() ID {
	id, err := NewID()
	if err != nil {
		// 如果系统无法提供随机数，就不能安全地生成编号。
		// 这时直接报错，避免产生可能重复的编号。
		panic(err)
	}
	return id
}
