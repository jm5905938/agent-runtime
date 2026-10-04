package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func ingressFixture(t *testing.T, definition string) (*Backend, *Session, domain.AgentInstance) {
	t.Helper()
	b := openTestBackend(t)
	s := openTestSession(t, b)
	if _, err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	agent := testAgent()
	agent.Definition.ID = definition
	agent.Status = domain.AgentStatusActive
	if err := s.CreateAgent(context.Background(), agent); err != nil {
		t.Fatal(err)
	}
	return b, s, agent
}

func openTestIngress(t *testing.T, path string) *Ingress {
	t.Helper()
	ingress, err := OpenIngress(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ingress.Close(); err != nil {
			t.Error(err)
		}
	})
	return ingress
}

func TestIngressSubmitPreservesRunningExecutionAndAction(t *testing.T) {
	ctx := context.Background()
	b, s, agent := ingressFixture(t, "main")
	sourceEvent := domain.NewEvent("main.request", map[string]any{"message": "first"})
	received, err := s.ReceiveEvent(ctx, agent.ID, sourceEvent)
	if err != nil {
		t.Fatal(err)
	}
	source, err := s.ClaimExecution(ctx, received.Delivery.Key)
	if err != nil {
		t.Fatal(err)
	}
	request := domain.NewAction("model.generate", nil)
	request.BindExecution(source.Token.ExecutionID)
	action := domain.ActionRecord{Request: request, AgentID: agent.ID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicyManual,
		IdempotencyKey: string(request.ID), MaxAttempts: 1, Status: domain.ActionStatusPending, ResultEventID: "reserved-result"}
	if _, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: source.Token, Actions: []domain.ActionRecord{action}}); err != nil {
		t.Fatal(err)
	}
	actionClaim, err := s.ClaimAction(ctx, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	runningEvent := domain.NewEvent("main.request", map[string]any{"message": "running"})
	runningDelivery, err := s.ReceiveEvent(ctx, agent.ID, runningEvent)
	if err != nil {
		t.Fatal(err)
	}
	running, err := s.ClaimExecution(ctx, runningDelivery.Delivery.Key)
	if err != nil {
		t.Fatal(err)
	}
	before := recoveryDatabaseSnapshot(t, b)
	ingress := openTestIngress(t, b.Path())
	queued, err := ingress.SubmitMessage(ctx, agent.ID, "queued-input", "later")
	if err != nil {
		t.Fatal(err)
	}
	if queued.Duplicate || queued.Delivery.Status != domain.DeliveryStatusPending || queued.Delivery.ExecutionID == "" {
		t.Fatalf("新输入未正确入队: %+v", queued)
	}
	after := recoveryDatabaseSnapshot(t, b)
	for _, table := range []string{"agents", "executions", "execution_attempts", "actions", "action_attempts"} {
		if !reflect.DeepEqual(before[table], after[table]) {
			t.Fatalf("接收输入改变了%s，可能触发了恢复", table)
		}
	}
	if delivery, err := s.LoadDelivery(ctx, running.Token.Delivery); err != nil || delivery.Status != domain.DeliveryStatusRunning {
		t.Fatalf("运行中的投递被恢复或修改: %+v %v", delivery, err)
	}
	if _, err := b.OpenSession(ctx); !errors.Is(err, core.ErrStoreOwned) {
		t.Fatalf("输入通道释放了执行所有权: %v", err)
	}
	if other, err := Open(b.Path()); !errors.Is(err, core.ErrStoreOwned) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("输入通道绕过了执行会话独占规则: %v", err)
	}
	if _, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: running.Token}); err != nil {
		t.Fatalf("入队后原execution token无法提交: %v", err)
	}
	if _, err := s.CompleteAction(ctx, actionRegressionCompletion(actionClaim)); err != nil {
		t.Fatalf("入队后原action token无法提交: %v", err)
	}
}

func TestIngressDuplicateConflictAndReservedResult(t *testing.T) {
	ctx := context.Background()
	for _, definition := range []string{"echo", "main"} {
		t.Run(definition, func(t *testing.T) {
			b, s, agent := ingressFixture(t, definition)
			ingress := openTestIngress(t, b.Path())
			first, err := ingress.SubmitMessage(ctx, agent.ID, "input", "你好")
			if err != nil {
				t.Fatal(err)
			}
			stored, err := s.LoadEvent(ctx, "input")
			if err != nil {
				t.Fatal(err)
			}
			if stored.Type != definition+".request" || stored.Payload["message"] != "你好" {
				t.Fatalf("消息事件错误: %+v", stored)
			}
			before := recoveryDatabaseSnapshot(t, b)
			duplicate, err := ingress.SubmitMessage(ctx, agent.ID, "input", "你好")
			if err != nil || !duplicate.Duplicate || duplicate.Delivery != first.Delivery {
				t.Fatalf("重复输入改变了投递身份: %+v %v", duplicate, err)
			}
			if _, err := ingress.SubmitMessage(ctx, agent.ID, "input", "不同内容"); !errors.Is(err, core.ErrStoreConflict) {
				t.Fatalf("同ID不同内容未拒绝: %v", err)
			}
			if after := recoveryDatabaseSnapshot(t, b); !reflect.DeepEqual(before, after) {
				t.Fatal("重复或冲突输入改变了数据库")
			}
			execution, err := s.ClaimExecution(ctx, first.Delivery.Key)
			if err != nil {
				t.Fatal(err)
			}
			request := domain.NewAction("echo", nil)
			request.BindExecution(execution.Token.ExecutionID)
			action := domain.ActionRecord{Request: request, AgentID: agent.ID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry,
				IdempotencyKey: string(request.ID), MaxAttempts: 2, Status: domain.ActionStatusPending, ResultEventID: "reserved-result"}
			if _, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: execution.Token, Actions: []domain.ActionRecord{action}}); err != nil {
				t.Fatal(err)
			}
			before = recoveryDatabaseSnapshot(t, b)
			if _, err := ingress.SubmitMessage(ctx, agent.ID, action.ResultEventID, "抢占结果ID"); !errors.Is(err, core.ErrStoreConflict) {
				t.Fatalf("接受了Action预留结果ID: %v", err)
			}
			if after := recoveryDatabaseSnapshot(t, b); !reflect.DeepEqual(before, after) {
				t.Fatal("冲突结果ID留下了写入")
			}
		})
	}
}

func TestIngressConcurrentSubmissionsPreserveReceiveOrder(t *testing.T) {
	ctx := context.Background()
	b, s, agent := ingressFixture(t, "echo")
	const senders, inputs = 6, 12
	ingresses := make([]*Ingress, senders)
	for index := range ingresses {
		ingresses[index] = openTestIngress(t, b.Path())
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for sender := 0; sender <= senders; sender++ {
		workers.Add(1)
		go func(sender int) {
			defer workers.Done()
			<-start
			for input := 0; input < inputs; input++ {
				id := domain.ID(fmt.Sprintf("input-%d-%d", sender, input))
				message := string(id)
				var received core.ReceivedEvent
				var err error
				if sender == senders {
					event := domain.NewEvent("echo.request", map[string]any{"message": message})
					event.ID = id
					received, err = s.ReceiveEvent(ctx, agent.ID, event)
				} else {
					received, err = ingresses[sender].SubmitMessage(ctx, agent.ID, id, message)
				}
				if err != nil {
					t.Errorf("提交%s失败: %v", id, err)
					return
				}
				if received.Duplicate || received.Delivery.Status != domain.DeliveryStatusPending {
					t.Errorf("提交%s返回错误状态: %+v", id, received)
				}
			}
		}(sender)
	}
	close(start)
	workers.Wait()
	deliveries, err := s.ListDeliveries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != (senders+1)*inputs {
		t.Fatalf("并发接收丢失输入: %d", len(deliveries))
	}
	rows, err := b.db.QueryContext(ctx, `SELECT receive_seq, event_id FROM deliveries ORDER BY receive_seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	next := make([]int, senders+1)
	count := 0
	for rows.Next() {
		var sequence int64
		var eventID string
		if err := rows.Scan(&sequence, &eventID); err != nil {
			t.Fatal(err)
		}
		if sequence != int64(count+1) || deliveries[count].Key.EventID != domain.ID(eventID) {
			t.Fatalf("receive_seq不唯一或查询顺序错误: 第%d项=%d/%s", count, sequence, eventID)
		}
		var sender, input int
		if _, err := fmt.Sscanf(eventID, "input-%d-%d", &sender, &input); err != nil || sender < 0 || sender > senders || input != next[sender] {
			t.Fatalf("同发送者的输入未按FIFO排列: %s %v", eventID, err)
		}
		next[sender]++
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != len(deliveries) {
		t.Fatalf("查询记录数不一致: %d/%d", count, len(deliveries))
	}
}

func TestIngressConcurrentDuplicateCreatesOneDelivery(t *testing.T) {
	b, s, agent := ingressFixture(t, "echo")
	const submitters = 8
	ingresses := make([]*Ingress, submitters)
	for index := range ingresses {
		ingresses[index] = openTestIngress(t, b.Path())
	}
	results := make(chan core.ReceivedEvent, submitters)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for _, ingress := range ingresses {
		workers.Add(1)
		go func(ingress *Ingress) {
			defer workers.Done()
			<-start
			received, err := ingress.SubmitMessage(context.Background(), agent.ID, "same-id", "same-content")
			if err != nil {
				t.Errorf("并发重复提交失败: %v", err)
				return
			}
			results <- received
		}(ingress)
	}
	close(start)
	workers.Wait()
	close(results)
	fresh := 0
	var first *domain.Delivery
	for received := range results {
		if !received.Duplicate {
			fresh++
		}
		if first == nil {
			copy := received.Delivery
			first = &copy
		} else if *first != received.Delivery {
			t.Fatalf("重复输入得到不同投递身份: %+v %+v", first, received.Delivery)
		}
	}
	deliveries, err := s.ListDeliveries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fresh != 1 || len(deliveries) != 1 {
		t.Fatalf("并发重复输入未去重: fresh=%d deliveries=%d", fresh, len(deliveries))
	}
}

func TestIngressCancellationCloseAndInvalidInputLeaveNoWrites(t *testing.T) {
	ctx := context.Background()
	b, s, agent := ingressFixture(t, "main")
	ingress := openTestIngress(t, b.Path())
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	before := recoveryDatabaseSnapshot(t, b)
	if _, err := ingress.SubmitMessage(canceled, agent.ID, "canceled", "cancel"); !errors.Is(err, context.Canceled) {
		t.Fatalf("忽略已取消context: %v", err)
	}
	if _, err := ingress.SubmitMessage(ctx, "missing-agent", "missing", "hello"); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("缺失Agent返回错误: %v", err)
	}
	if _, err := ingress.SubmitMessage(ctx, agent.ID, " ", "hello"); err == nil {
		t.Fatal("接受了空白事件ID")
	}
	if after := recoveryDatabaseSnapshot(t, b); !reflect.DeepEqual(before, after) {
		t.Fatal("被取消或无效输入留下了写入")
	}
	unknown := testAgent()
	unknown.Definition.ID = "unsupported"
	if err := s.CreateAgent(ctx, unknown); err != nil {
		t.Fatal(err)
	}
	before = recoveryDatabaseSnapshot(t, b)
	if _, err := ingress.SubmitMessage(ctx, unknown.ID, "unsupported", "hello"); err == nil {
		t.Fatal("接受了不支持的Definition")
	}
	if after := recoveryDatabaseSnapshot(t, b); !reflect.DeepEqual(before, after) {
		t.Fatal("不支持的Definition留下了写入")
	}
	if err := ingress.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ingress.Close(); err != nil {
		t.Fatalf("重复关闭失败: %v", err)
	}
	if _, err := ingress.SubmitMessage(ctx, agent.ID, "closed", "hello"); !errors.Is(err, core.ErrStoreClosed) {
		t.Fatalf("关闭后仍能提交: %v", err)
	}
}

func TestIngressCanceledWhileWaitingForSQLiteWriter(t *testing.T) {
	b, _, agent := ingressFixture(t, "main")
	ingress := openTestIngress(t, b.Path())
	writer, err := b.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		_, err := ingress.SubmitMessage(ctx, agent.ID, "blocked-input", "hello")
		completed <- err
	}()
	select {
	case err = <-completed:
	case <-time.After(2 * time.Second):
		writer.Rollback()
		err = <-completed
		t.Errorf("等待SQLite写锁没有及时响应取消")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("写锁等待取消未返回context错误: %v", err)
	}
	if err := writer.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Fatal(err)
	}
	var events, deliveries int
	if err := b.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := b.db.QueryRow(`SELECT COUNT(*) FROM deliveries`).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if events != 0 || deliveries != 0 {
		t.Fatalf("取消写锁等待留下输入: events=%d deliveries=%d", events, deliveries)
	}
	var timeout int
	if err := ingress.db.QueryRow(`PRAGMA busy_timeout`).Scan(&timeout); err != nil {
		t.Fatal(err)
	}
	if timeout != 5000 {
		t.Fatalf("取消输入后连接配置未恢复: busy_timeout=%d", timeout)
	}
	if received, err := ingress.SubmitMessage(context.Background(), agent.ID, "after-cancellation", "later"); err != nil || received.Duplicate {
		t.Fatalf("取消等待后输入通道无法继续接收: %+v %v", received, err)
	}
}

func TestIngressCommitBusyRetriesAndCancelsWithoutResidualWrites(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "reader_released"
		if canceled {
			name = "canceled_with_reader"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			b, _, agent := ingressFixture(t, "main")
			ingress := openTestIngress(t, b.Path())
			readerDB := rawTestDB(t, b.Path())
			reader, err := readerDB.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if _, err := reader.ExecContext(ctx, `BEGIN`); err != nil {
				t.Fatal(err)
			}
			defer reader.ExecContext(ctx, `ROLLBACK`)
			var count int
			if err := reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			// 普通reader持SHARED锁：IMMEDIATE事务可以开始，COMMIT才会被阻塞。
			probe, err := b.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("reader阻塞了BEGIN，未建立COMMIT竞争条件: %v", err)
			}
			if err := probe.Rollback(); err != nil {
				t.Fatal(err)
			}
			duration := 3 * time.Second
			if canceled {
				duration = 80 * time.Millisecond
			}
			submitCtx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			type result struct {
				received core.ReceivedEvent
				err      error
			}
			completed := make(chan result, 1)
			go func() {
				received, err := ingress.SubmitMessage(submitCtx, agent.ID, "commit-input", "hello")
				completed <- result{received: received, err: err}
			}()
			var submitted result
			if canceled {
				readerReleased := false
				select {
				case submitted = <-completed:
				case <-time.After(2 * time.Second):
					if _, err := reader.ExecContext(ctx, `ROLLBACK`); err != nil {
						t.Fatal(err)
					}
					readerReleased = true
					submitted = <-completed
					t.Error("COMMIT等待reader时没有及时响应取消")
				}
				if !errors.Is(submitted.err, context.DeadlineExceeded) {
					t.Errorf("COMMIT竞争取消返回错误: %v", submitted.err)
				}
				if !readerReleased {
					if _, err := reader.ExecContext(ctx, `ROLLBACK`); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				returned := false
				select {
				case submitted = <-completed:
					returned = true
					t.Errorf("reader释放前提交已结束，COMMIT竞争未重试: %v", submitted.err)
				case <-time.After(150 * time.Millisecond):
				}
				if _, err := reader.ExecContext(ctx, `ROLLBACK`); err != nil {
					t.Fatal(err)
				}
				if !returned {
					select {
					case submitted = <-completed:
					case <-time.After(2 * time.Second):
						cancel()
						submitted = <-completed
						t.Error("reader释放后COMMIT仍未完成")
					}
				}
				if submitted.err != nil || submitted.received.Duplicate || submitted.received.Delivery.Status != domain.DeliveryStatusPending {
					t.Errorf("COMMIT竞争重试未正确保存输入: %+v %v", submitted.received, submitted.err)
				}
			}
			var events, deliveries, orphans int
			if err := b.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&events); err != nil {
				t.Fatal(err)
			}
			if err := b.db.QueryRow(`SELECT COUNT(*) FROM deliveries`).Scan(&deliveries); err != nil {
				t.Fatal(err)
			}
			if err := b.db.QueryRow(`SELECT COUNT(*) FROM events e LEFT JOIN deliveries d ON d.event_id=e.id WHERE d.event_id IS NULL`).Scan(&orphans); err != nil {
				t.Fatal(err)
			}
			want := 1
			if canceled {
				want = 0
			}
			if events != want || deliveries != want || orphans != 0 {
				t.Errorf("COMMIT竞争留下重复或部分写入: events=%d deliveries=%d orphans=%d", events, deliveries, orphans)
			}
			if received, err := ingress.SubmitMessage(ctx, agent.ID, "after-commit-busy", "later"); err != nil || received.Duplicate {
				t.Fatalf("COMMIT竞争后输入通道无法继续接收: %+v %v", received, err)
			}
		})
	}
}

func TestIngressRejectsMissingAndIncompatibleDatabaseWithoutMigration(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.db")
		if ingress, err := OpenIngress(path); err == nil {
			ingress.Close()
			t.Fatal("输入通道创建了新数据库")
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("输入通道留下新数据库: %v", err)
		}
	})
	for _, version := range []string{"empty", "legacy", "unknown", "incompatible"} {
		t.Run(version, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "old.db")
			db := rawTestDB(t, path)
			if _, err := db.Exec(`CREATE TABLE sentinel(value TEXT); INSERT INTO sentinel VALUES('preserved')`); err != nil {
				t.Fatal(err)
			}
			if version != "empty" {
				if _, err := db.Exec(`CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY)`); err != nil {
					t.Fatal(err)
				}
				number := 1
				if version == "unknown" {
					number = 999
				}
				if _, err := db.Exec(`INSERT INTO schema_migrations VALUES(?)`, number); err != nil {
					t.Fatal(err)
				}
				if version == "legacy" {
					content, err := migrationFiles.ReadFile("migrations/001_initial.sql")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := db.Exec(string(content)); err != nil {
						t.Fatal(err)
					}
				}
				if version == "incompatible" {
					if _, err := db.Exec(`CREATE TABLE agents(id TEXT PRIMARY KEY)`); err != nil {
						t.Fatal(err)
					}
				}
			}
			var before string
			if err := db.QueryRow(`SELECT group_concat(name || ':' || coalesce(sql,''), char(10)) FROM (SELECT name, sql FROM sqlite_master ORDER BY name)`).Scan(&before); err != nil {
				t.Fatal(err)
			}
			ingress, err := OpenIngress(path)
			if err == nil {
				defer ingress.Close()
				if _, err := ingress.SubmitMessage(context.Background(), "agent", "event", "hello"); err == nil {
					t.Fatal("接受了不兼容的数据库")
				}
			}
			var after, value string
			if err := db.QueryRow(`SELECT group_concat(name || ':' || coalesce(sql,''), char(10)) FROM (SELECT name, sql FROM sqlite_master ORDER BY name)`).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT value FROM sentinel`).Scan(&value); err != nil {
				t.Fatal(err)
			}
			if before != after || value != "preserved" {
				t.Fatal("输入通道迁移或修改了不兼容的数据库")
			}
		})
	}
	for _, column := range []struct{ table, name string }{{"deliveries", "receive_seq"}, {"actions", "result_event_id"}} {
		t.Run("missing_"+column.name, func(t *testing.T) {
			b, _, agent := ingressFixture(t, "echo")
			if _, err := b.db.Exec("ALTER TABLE " + column.table + " RENAME COLUMN " + column.name + " TO legacy_" + column.name); err != nil {
				t.Fatal(err)
			}
			before := recoveryDatabaseSnapshot(t, b)
			ingress := openTestIngress(t, b.Path())
			if _, err := ingress.SubmitMessage(context.Background(), agent.ID, "incompatible-input", "hello"); err == nil {
				t.Fatalf("输入通道接受了缺失%s的schema", column.name)
			}
			if after := recoveryDatabaseSnapshot(t, b); !reflect.DeepEqual(before, after) {
				t.Fatal("输入通道修改了缺失列的数据库")
			}
		})
	}
}

func TestIngressDatabasePathAliases(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("符号链接和硬链接行为由Unix环境验证")
	}
	ctx := context.Background()
	b, _, agent := ingressFixture(t, "echo")
	alias := filepath.Join(filepath.Dir(b.Path()), "别名 ?#%.db")
	if err := os.Symlink(b.Path(), alias); err != nil {
		t.Fatal(err)
	}
	ingress := openTestIngress(t, alias)
	if _, err := ingress.SubmitMessage(ctx, agent.ID, "via-alias", "hello"); err != nil {
		t.Fatalf("符号链接输入通道失败: %v", err)
	}
	hardlink := filepath.Join(filepath.Dir(b.Path()), "hard.db")
	if err := os.Link(b.Path(), hardlink); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{b.Path(), hardlink} {
		if opened, err := OpenIngress(path); err == nil {
			opened.Close()
			t.Fatalf("输入通道接受了硬链接数据库: %s", path)
		}
	}
}
