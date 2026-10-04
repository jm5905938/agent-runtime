//go:build windows

package sqlite

import (
	"errors"
	"fmt"
	"os"

	"agent-runtime/core"

	"golang.org/x/sys/windows"
)

func acquireOwnership(path string) (*os.File, error) {
	if err := checkDatabasePath(path); err != nil {
		return nil, err
	}
	// 打开或创建独占锁文件：database.db.lock
	file, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("打开sqlite锁文件: %w", err)
	}

	var overlapped windows.Overlapped

	// 锁定锁文件的第一个字节：
	// EXCLUSIVE：独占
	// FAIL_IMMEDIATELY：如果被其他进程占用，立即失败，不等待
	err = windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&overlapped,
	)
	if err != nil {
		closeErr := file.Close()

		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, errors.Join(core.ErrStoreOwned, closeErr)
		}

		return nil, errors.Join(
			fmt.Errorf("锁定sqlite文件: %w", err),
			closeErr,
		)
	}

	// Session.Close会调用file.Close，Windows会自动释放锁
	return file, nil
}

func checkDatabasePath(path string) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("检查sqlite文件: %w", err)
	}
	defer file.Close()
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info); err != nil {
		return fmt.Errorf("检查sqlite硬链接: %w", err)
	}
	if info.NumberOfLinks > 1 {
		return fmt.Errorf("sqlite不支持硬链接数据库路径")
	}
	return nil
}
