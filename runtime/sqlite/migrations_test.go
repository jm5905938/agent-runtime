package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func rawTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, e := sql.Open(Name, path)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
func TestRejectsUnknownSchemaWithoutChanges(t *testing.T) {
	for _, version := range []int{-1, 0, 2, 999} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "test.db")
			db := rawTestDB(t, path)
			if _, e := db.Exec(`CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY)`); e != nil {
				t.Fatal(e)
			}
			if _, e := db.Exec(`INSERT INTO schema_migrations VALUES(?)`, version); e != nil {
				t.Fatal(e)
			}
			if b, e := Open(path); e == nil {
				b.Close()
				t.Fatal("接受了未知版本")
			}
			var tables int
			if e := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table'`).Scan(&tables); e != nil {
				t.Fatal(e)
			}
			if tables != 1 {
				t.Fatalf("失败时修改了schema: %d", tables)
			}
			if _, e := db.Exec(`DROP TABLE schema_migrations`); e != nil {
				t.Fatal(e)
			}
			b, e := Open(path)
			if e != nil {
				t.Fatalf("打开失败没有释放资源: %v", e)
			}
			b.Close()
		})
	}
}
func TestMigrationFailureRollsBackAndRejectsOldSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := rawTestDB(t, path)
	if _, e := db.Exec(`CREATE TABLE agents(id TEXT PRIMARY KEY)`); e != nil {
		t.Fatal(e)
	}
	if b, e := Open(path); e == nil {
		b.Close()
		t.Fatal("接受了旧表结构")
	}
	var count int
	if e := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('events','executions','schema_migrations')`).Scan(&count); e != nil {
		t.Fatal(e)
	}
	if count != 0 {
		t.Fatal("迁移失败留下部分表")
	}
}
func TestSessionRechecksSchemaAfterAnotherOwner(t *testing.T) {
	b := openTestBackend(t)
	other, e := Open(b.Path())
	if e != nil {
		t.Fatal(e)
	}
	if _, e := other.db.Exec(`INSERT INTO schema_migrations(version) VALUES(999)`); e != nil {
		t.Fatal(e)
	}
	other.Close()
	if s, e := b.OpenSession(context.Background()); e == nil {
		s.Close(context.Background())
		t.Fatal("旧后端未检查新版本")
	}
}
func TestConnectionConfigurationSurvivesPoolReplacement(t *testing.T) {
	b := openTestBackend(t)
	b.db.SetMaxOpenConns(2)
	b.db.SetMaxIdleConns(0)
	for i := 0; i < 2; i++ {
		first, e := b.db.Conn(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		second, e := b.db.Conn(context.Background())
		if e != nil {
			first.Close()
			t.Fatal(e)
		}
		for _, c := range []*sql.Conn{first, second} {
			var fk, timeout int
			if e := c.QueryRowContext(context.Background(), `PRAGMA foreign_keys`).Scan(&fk); e != nil {
				t.Fatal(e)
			}
			if e := c.QueryRowContext(context.Background(), `PRAGMA busy_timeout`).Scan(&timeout); e != nil {
				t.Fatal(e)
			}
			if fk != 1 || timeout != 5000 {
				t.Fatalf("连接配置丢失: %d %d", fk, timeout)
			}
			if _, e := c.ExecContext(context.Background(), `INSERT INTO executions(id,agent_id,event_id,status,created_at,attempt_count) VALUES('orphan','missing','missing','pending','now','0')`); e == nil {
				t.Fatal("外键没有生效")
			}
		}
		first.Close()
		second.Close()
	}
}
