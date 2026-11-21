package store

import (
	"fmt"
	"log"
	"os"
	"syscall"
	"time"
)

// Admission timing contains operation IDs and durations, never credentials or SQL values.
func admissionTiming(op, id string) (func(string), func()) {
	if os.Getenv("KUASAR_STORE_TIMING") == "" {
		return func(string) {}, func() {}
	}
	start := time.Now()
	last := start
	lastTID := syscall.Gettid()
	var threads []string
	type phase struct {
		Name        string
		Nanoseconds int64
	}
	phases := make([]phase, 0, 6)
	return func(name string) {
			now := time.Now()
			tid := syscall.Gettid()
			threads = append(threads, fmt.Sprintf("%s:%d:%d", name, lastTID, tid))
			lastTID = tid
			phases = append(phases, phase{name, now.Sub(last).Nanoseconds()})
			last = now
		}, func() {
			log.Printf("STORE_ADMISSION op=%s id=%s start_ns=%d total_ns=%d phases=%v pid=%d threads=%v", op, id, start.UnixNano(), time.Since(start).Nanoseconds(), phases, os.Getpid(), threads)
		}
}
