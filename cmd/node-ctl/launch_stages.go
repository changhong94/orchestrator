package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"
	"time"
)

// Diagnostic phase names and timestamps only; never arguments or credentials.
func traceLaunchStages(op string) (func(string), func()) {
	if os.Getenv("CH_FORK_TIMING") == "" {
		return func(string) {}, func() {}
	}
	last := time.Now()
	lastTID := syscall.Gettid()
	var threads []string
	var lines []string
	return func(name string) {
			now := time.Now()
			tid := syscall.Gettid()
			threads = append(threads, fmt.Sprintf("%s:%d:%d", name, lastTID, tid))
			lastTID = tid
			lines = append(lines, fmt.Sprintf("%s:%d:%d", name, last.UnixNano(), now.UnixNano()))
			last = now
		}, func() {
			if len(lines) == 0 {
				return
			}
			log.Printf("LAUNCH_STAGES op=%s spans=%s pid=%d threads=%s", op, strings.Join(lines, ","), os.Getpid(), strings.Join(threads, ","))
			lines = nil
		}
}
