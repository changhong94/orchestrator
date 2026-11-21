package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Concurrent pool bookkeeping must queue without losing writes or cancellation.
func TestAdmissionConcurrentBookkeeping(t *testing.T) {
	for _, limit := range []int{0, 1} {
		s := testStore(t)
		s.db.SetMaxOpenConns(limit)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := s.db.ExecContext(ctx, "CREATE TABLE admission_queue_test (id INTEGER PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		errors := make(chan error, 8)
		elapsed := make(chan time.Duration, 8)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				begin := time.Now()
				_, err := s.db.ExecContext(ctx, "INSERT INTO admission_queue_test VALUES (?)", i)
				elapsed <- time.Since(begin)
				errors <- err
			}(i)
		}
		close(start)
		wg.Wait()
		close(errors)
		close(elapsed)
		for err := range errors {
			if err != nil {
				t.Fatal(err)
			}
		}
		var count int
		if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM admission_queue_test").Scan(&count); err != nil || count != 8 {
			t.Fatalf("lost commit: %d %v", count, err)
		}
		var max time.Duration
		for d := range elapsed {
			if d > max {
				max = d
			}
		}
		t.Logf("connection limit=%d, eight durable concurrent writes maximum=%s queue_wait=%s", limit, max, s.db.Stats().WaitDuration)
		cancel()
	}
}
