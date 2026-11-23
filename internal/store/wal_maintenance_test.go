package store

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestWALMaintenancePreservesCommitAndPinnedReader(t *testing.T) {
	s := testStore(t)
	for pragma, want := range map[string]int{"synchronous": 2, "wal_autocheckpoint": 1000} {
		var got int
		if err := s.db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil || got != want {
			t.Fatalf("%s=%d err=%v", pragma, got, err)
		}
	}
	if _, err := s.db.Exec("CREATE TABLE maintenance_probe(v INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("INSERT INTO maintenance_probe VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	tx, err := s.readers.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var n int
	if err = tx.QueryRow("SELECT count(*) FROM maintenance_probe").Scan(&n); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err = s.db.ExecContext(ctx, "INSERT INTO maintenance_probe VALUES(2)"); err != nil {
		t.Fatal(err)
	}
	_, frames, copied, err := s.wal.checkpoint(ctx)
	if err != nil || frames <= copied {
		t.Fatalf("reader not respected frames=%d copied=%d err=%v", frames, copied, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	_, frames, copied, err = s.wal.checkpoint(ctx)
	if err != nil || frames != copied {
		t.Fatalf("checkpoint did not drain %d %d %v", frames, copied, err)
	}
	if err = s.db.QueryRow("SELECT count(*) FROM maintenance_probe").Scan(&n); err != nil || n != 2 {
		t.Fatal("committed data lost", err, n)
	}
}

func TestWALCommitSurvivesProcessExit(t *testing.T) {
	if path := os.Getenv("KUASAR_TEST_WAL_CRASH_PATH"); path != "" {
		s, err := Open(path, nil)
		if err != nil {
			panic(err)
		}
		if _, err = s.db.Exec("CREATE TABLE crash_probe(v TEXT)"); err != nil {
			panic(err)
		}
		tx, err := s.readers.Begin()
		if err != nil {
			panic(err)
		}
		var n int
		if err = tx.QueryRow("SELECT count(*) FROM crash_probe").Scan(&n); err != nil {
			panic(err)
		}
		if _, err = s.db.Exec("INSERT INTO crash_probe VALUES('durable-before-ack')"); err != nil {
			panic(err)
		}
		// Keep the reader pinned and exit without closing/checkpointing the store.
		os.Exit(0)
	}
	path := filepath.Join(t.TempDir(), "crash.db")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestWALCommitSurvivesProcessExit$")
	cmd.Env = append(os.Environ(), "KUASAR_TEST_WAL_CRASH_PATH="+path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v %s", err, out)
	}
	s, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var got string
	if err = s.db.QueryRow("SELECT v FROM crash_probe").Scan(&got); err != nil || got != "durable-before-ack" {
		t.Fatalf("WAL recovery failed: %q %v", got, err)
	}
}
