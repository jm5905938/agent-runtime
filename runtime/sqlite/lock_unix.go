//go:build linux || darwin || freebsd

package sqlite

import (
	"errors"
	"fmt"
	"os"

	"agent-runtime/core"
	"golang.org/x/sys/unix"
)

// 锁文件保持原inode，不在释放时删除，避免两个进程锁住不同文件
func acquireOwnership(path string) (*os.File, error) {
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err == nil {
		if stat.Nlink > 1 {
			return nil, fmt.Errorf("sqlite不支持硬链接数据库路径")
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return nil, fmt.Errorf("检查sqlite文件: %w", err)
	}
	file, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("打开sqlite锁文件: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.Join(core.ErrStoreOwned, closeErr)
		}
		return nil, errors.Join(fmt.Errorf("锁定sqlite文件: %w", err), closeErr)
	}
	return file, nil
}
