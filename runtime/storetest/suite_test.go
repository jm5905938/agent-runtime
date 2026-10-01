package storetest_test

import (
	"testing"

	"agent-runtime/core"
	"agent-runtime/storetest"
)

func TestMemoryRecoveryStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) core.RecoveryStore {
		return core.NewMemoryRecoveryStore()
	})
}
