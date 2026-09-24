//go:build !linux && !darwin && !freebsd

package sqlite

import (
	"fmt"
	"os"
)

func acquireOwnership(path string) (*os.File, error) {
	return nil, fmt.Errorf("当前平台尚未实现sqlite独占文件锁")
}
