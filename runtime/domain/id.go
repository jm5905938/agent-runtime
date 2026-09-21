package domain

import (
	"crypto/rand"
	"fmt"
)

//字符串形式的唯一id
type ID string

func NewID() (ID, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate domain id: %w", err)
	}

	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	return ID(fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])), nil
}

func mustNewID() ID {
	id, err := NewID()
	if err != nil {
		panic(err)
	}
	return id
}
