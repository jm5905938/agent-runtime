package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"agent-runtime/core"
	"agent-runtime/storetest"
)

type testRecoveryStore struct {
	*Backend
}

func (s testRecoveryStore) OpenSession(ctx context.Context) (core.RecoverySession, error) {
	session, err := s.Backend.OpenSession(ctx)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func TestSQLiteContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) core.RecoveryStore {
		backend, err := Open(filepath.Join(t.TempDir(), "store.db"))
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			_ = backend.Close()
		})

		return testRecoveryStore{backend}
	})
}
