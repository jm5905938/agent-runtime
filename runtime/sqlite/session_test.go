package sqlite

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func openTestBackend(t *testing.T) *Backend {
	t.Helper()
	b, e := Open(filepath.Join(t.TempDir(), "test.db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := b.Close(); e != nil {
			t.Error(e)
		}
	})
	return b
}
func openTestSession(t *testing.T, b *Backend) *Session {
	t.Helper()
	s, e := b.OpenSession(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := s.Close(context.Background()); e != nil {
			t.Error(e)
		}
	})
	return s
}
func testAgent() domain.AgentInstance {
	a := domain.NewAgentInstance("echo")
	a.Definition = domain.DefinitionRef{ID: "echo", Version: "1"}
	return a
}
func TestSessionGatesAndClosedBackend(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)
	s := openTestSession(t, b)
	if e := s.CreateAgent(ctx, testAgent()); !errors.Is(e, core.ErrRecoveryRequired) {
		t.Fatalf("恢复前写入: %v", e)
	}
	if _, e := s.ListAgents(ctx); e != nil {
		t.Fatal(e)
	}
	if e := b.Close(); !errors.Is(e, core.ErrStoreOwned) {
		t.Fatalf("活动会话关闭后端: %v", e)
	}
	if _, e := s.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	if e := s.Close(ctx); e != nil {
		t.Fatal(e)
	}
	second := openTestSession(t, b)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if e := s.Close(canceled); e != nil {
		t.Fatal(e)
	}
	if _, e := b.OpenSession(ctx); !errors.Is(e, core.ErrStoreOwned) {
		t.Fatalf("旧会话释放了新所有者: %v", e)
	}
	if _, e := s.Recover(canceled); !errors.Is(e, core.ErrStoreClosed) {
		t.Fatalf("旧会话恢复: %v", e)
	}
	if _, e := s.ListAgents(ctx); !errors.Is(e, core.ErrStoreClosed) {
		t.Fatalf("旧会话读取: %v", e)
	}
	if e := s.CreateAgent(ctx, testAgent()); !errors.Is(e, core.ErrStoreClosed) {
		t.Fatalf("旧会话写入: %v", e)
	}
	if e := second.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if e := b.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := b.OpenSession(ctx); !errors.Is(e, core.ErrStoreClosed) {
		t.Fatalf("重开已关闭后端: %v", e)
	}
}
func TestConcurrentSessionOperations(t *testing.T) {
	b := openTestBackend(t)
	s := openTestSession(t, b)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < 20; j++ {
				var e error
				switch i % 4 {
				case 0:
					_, e = s.Recover(context.Background())
				case 1:
					_, e = s.ListAgents(context.Background())
				case 2:
					e = s.CreateAgent(context.Background(), testAgent())
				case 3:
					e = s.Close(context.Background())
				}
				if e != nil && !errors.Is(e, core.ErrStoreClosed) && !errors.Is(e, core.ErrRecoveryRequired) {
					t.Error(e)
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
}
func TestCloseTimeoutKeepsOwnershipDuringWrite(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)
	s := openTestSession(t, b)
	if _, e := s.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	conn, e := b.db.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	baseline := b.db.Stats().WaitCount
	writeCtx, cancelWrite := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWrite()
	written := make(chan error, 1)
	go func() { written <- s.CreateAgent(writeCtx, testAgent()) }()
	//等待真实SQL操作占用门禁并开始等待连接
	for b.db.Stats().WaitCount == baseline {
		if writeCtx.Err() != nil {
			t.Fatal("写入没有进入等待")
		}
		runtime.Gosched()
	}
	closeCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if e := s.Close(closeCtx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("关闭超时: %v", e)
	}
	if other, e := Open(b.Path()); !errors.Is(e, core.ErrStoreOwned) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("关闭超时释放了所有权: %v", e)
	}
	conn.Close()
	if e := <-written; e != nil {
		t.Fatal(e)
	}
	if e := s.Close(ctx); e != nil {
		t.Fatal(e)
	}
	other, e := Open(b.Path())
	if e != nil {
		t.Fatal(e)
	}
	other.Close()
}
func TestRecoverRejectsUnsupportedWorkAndInvalidAgent(t *testing.T) {
	for _, kind := range []string{"event", "agent"} {
		t.Run(kind, func(t *testing.T) {
			b := openTestBackend(t)
			var e error
			if kind == "event" {
				_, e = b.db.Exec(`INSERT INTO events VALUES('event','echo.request','{}','2026-09-24T00:00:00Z')`)
			} else {
				_, e = b.db.Exec(`INSERT INTO agents VALUES('agent','echo','echo','1','invalid','{}','0')`)
			}
			if e != nil {
				t.Fatal(e)
			}
			s := openTestSession(t, b)
			_, e = s.Recover(context.Background())
			if e == nil {
				t.Fatal("不支持或损坏的记录被接受")
			}
			if kind == "event" && !errors.Is(e, ErrRecoveryUnsupported) {
				t.Fatal(e)
			}
			if e := s.CreateAgent(context.Background(), testAgent()); !errors.Is(e, core.ErrRecoveryRequired) {
				t.Fatalf("失败后写入门禁失效: %v", e)
			}
		})
	}
}
