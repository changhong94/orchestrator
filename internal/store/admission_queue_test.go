package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAdmissionConnectionQueueCancellationAndDurability(t *testing.T) {
	s := testStore(t)
	if s.db.Stats().MaxOpenConnections != 1 {
		t.Fatal("store must serialize local connections")
	}
	var sync int
	if err := s.db.QueryRow("PRAGMA synchronous").Scan(&sync); err != nil || sync != 2 {
		t.Fatalf("FULL durability lost: %d %v", sync, err)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = s.db.Conn(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued admission ignored cancellation: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	conn, err := s.db.Conn(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if s.db.Stats().WaitCount == 0 {
		t.Fatal("wait was not accounted in Go connection queue")
	}
}
