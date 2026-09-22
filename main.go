package main

import (
	"agent-runtime/runtime"
	"agent-runtime/storage"
	"log"
)

func main() {
	db, err := runtime.OpenSQLite("app.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := storage.RunMigrations(db); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL
    )`); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`INSERT INTO kv(k, v) VALUES(?, ?)
        ON CONFLICT(k) DO UPDATE SET v = excluded.v`, "hello", "world"); err != nil {
		log.Fatal(err)
	}

	var k, v string
	if err := db.QueryRow(`SELECT k, v FROM kv WHERE k = ?`, "hello").Scan(&k, &v); err != nil {
		log.Fatal(err)
	}
	log.Printf("read back: k=%q v=%q", k, v)
}
