//go:build linux || darwin || freebsd

package sqlite

import (
	"agent-runtime/core"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestOwnershipAcrossBackendsAndPathAliases(t *testing.T) {
	b := openTestBackend(t)
	other, e := Open(b.Path())
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	s := openTestSession(t, b)
	if _, e := other.OpenSession(context.Background()); !errors.Is(e, core.ErrStoreOwned) {
		t.Fatalf("同库第二个会话: %v", e)
	}
	alias := filepath.Join(filepath.Dir(b.Path()), "alias.db")
	if e := os.Symlink(b.Path(), alias); e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{b.Path(), alias} {
		opened, e := Open(path)
		if opened != nil {
			opened.Close()
		}
		if !errors.Is(e, core.ErrStoreOwned) {
			t.Fatalf("路径%s绕过所有权: %v", path, e)
		}
	}
	if e := s.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	second, e := other.OpenSession(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close(context.Background())
	if _, e := b.OpenSession(context.Background()); !errors.Is(e, core.ErrStoreOwned) {
		t.Fatalf("反向抢占: %v", e)
	}
}
func TestHardlinkedDatabaseRejected(t *testing.T) {
	b := openTestBackend(t)
	alias := filepath.Join(filepath.Dir(b.Path()), "hard.db")
	if e := os.Link(b.Path(), alias); e != nil {
		t.Fatal(e)
	}
	if _, e := b.OpenSession(context.Background()); e == nil {
		t.Fatal("接受了硬链接数据库")
	}
	if other, e := Open(alias); e == nil {
		other.Close()
		t.Fatal("接受了硬链接别名")
	}
}
func TestSQLiteProcessHelper(t *testing.T) {
	path := os.Getenv("SQLITE_TEST_CHILD_DB")
	if path == "" {
		return
	}
	b, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	s, e := b.OpenSession(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close(context.Background())
	if _, e := s.Recover(context.Background()); e != nil {
		t.Fatal(e)
	}
	a := testAgent()
	a.ID = "persisted-agent"
	a.StateVersion = math.MaxUint64
	a.State = map[string]any{"integer": json.Number("9007199254740993"), "decimal": json.Number("0.12345678901234567890123456789")}
	if e := s.CreateAgent(context.Background(), a); e != nil {
		t.Fatal(e)
	}
	fmt.Println("ready")
	bufio.NewReader(os.Stdin).ReadByte()
}
func TestProcessExitReleasesOwnershipAndPreservesAgent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "进程 ?#%.db")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSQLiteProcessHelper$")
	cmd.Env = append(os.Environ(), "SQLITE_TEST_CHILD_DB="+path)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	stdin, e := cmd.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	defer stdin.Close()
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	line, e := bufio.NewReader(stdout).ReadString('\n')
	if e != nil || line != "ready\n" {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatalf("子进程未就绪: %q %v %s", line, e, stderr.String())
	}
	if b, e := Open(path); !errors.Is(e, core.ErrStoreOwned) {
		if b != nil {
			b.Close()
		}
		t.Fatalf("跨进程抢占成功: %v", e)
	}
	if e := cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	cmd.Wait()
	b, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	s, e := b.OpenSession(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close(context.Background())
	if _, e := s.Recover(context.Background()); e != nil {
		t.Fatal(e)
	}
	a, e := s.LoadAgent(context.Background(), "persisted-agent")
	if e != nil {
		t.Fatal(e)
	}
	if a.StateVersion != math.MaxUint64 || a.State["integer"] != json.Number("9007199254740993") || a.State["decimal"] != json.Number("0.12345678901234567890123456789") {
		t.Fatalf("重开数据变化: %#v", a)
	}
}

func TestDanglingSymlinkCannotCreateAlternateLock(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.db")
	alias := filepath.Join(directory, "alias.db")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	if b, err := Open(alias); err == nil {
		b.Close()
		t.Fatal("失效符号链接绕过路径归一")
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("目标数据库被创建: %v", err)
	}
}
