package runtime

import (
	"path/filepath"
	"testing"
)

func TestOpenSQLite_Basic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL
    )`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO kv(k, v) VALUES(?, ?)`, "hello", "world"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var k, v string
	if err := db.QueryRow(`SELECT k, v FROM kv WHERE k = ?`, "hello").Scan(&k, &v); err != nil {
		t.Fatalf("select: %v", err)
	}
	if k != "hello" || v != "world" {
		t.Fatalf("unexpected row: k=%q v=%q", k, v)
	}
}
