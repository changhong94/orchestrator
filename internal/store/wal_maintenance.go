package store

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Commit still synchronizes the WAL with FULL before publishing ownership.
// Only copying already committed frames to the main DB is background work.
// Keep SQLite's default 1000-frame automatic checkpoint as a fallback: a
// stalled/failed worker must not silently disable the original maintenance.
type walMaintenance struct {
	db   *sql.DB
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func startWALMaintenance(path string) (*walMaintenance, error) {
	q := url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(0)", "synchronous(FULL)"}}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: filepath.ToSlash(path), RawQuery: q.Encode()}).String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	w := &walMaintenance{db: db, stop: make(chan struct{}), done: make(chan struct{})}
	go w.run()
	return w, nil
}

func (w *walMaintenance) checkpoint(ctx context.Context) (busy, frames, copied int, err error) {
	err = w.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &frames, &copied)
	return
}
func (w *walMaintenance) run() {
	defer close(w.done)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
		}
		started := time.Now()
		busy, frames, copied, err := w.checkpoint(context.Background())
		if err != nil {
			log.Printf("STORE_WAL checkpoint failed; automatic checkpoint retained: %v", err)
		} else if os.Getenv("KUASAR_STORE_TIMING") != "" && frames > 0 {
			log.Printf("STORE_WAL start_ns=%d elapsed_ns=%d busy=%d frames=%d copied=%d", started.UnixNano(), time.Since(started).Nanoseconds(), busy, frames, copied)
		}
	}
}
func (w *walMaintenance) close() error {
	if w == nil {
		return nil
	}
	w.once.Do(func() { close(w.stop) })
	<-w.done
	if err := w.db.Close(); err != nil {
		return fmt.Errorf("close WAL maintenance: %w", err)
	}
	return nil
}
