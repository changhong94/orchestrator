package orch

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestParallelStartJobsKeepSameRunStopFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lc := newRunPoolTestLauncher()
	entered := make(chan string, 2)
	release := make(chan struct{})
	lc.startFn = func(ctx context.Context, u string) error {
		entered <- u
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p := newRunPool(runKindSandbox, 0, time.Second, t.TempDir(), lc, testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.prepareConcurrency = 2
	done := make(chan struct{})
	go func() { p.controlLoop(ctx); close(done) }()
	send := func(r runControlReq) {
		t.Helper()
		select {
		case p.controlCh <- r:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	send(runControlReq{op: "start", runID: "a"})
	send(runControlReq{op: "start", runID: "b"})
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("independent start was serialized")
		}
	}
	send(runControlReq{op: "stop", runID: "a"})
	select {
	case <-lc.stopped:
		t.Fatal("stop passed an unsettled start")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	for i := 0; i < 2; i++ {
		select {
		case d := <-p.startDoneCh:
			if d.err != nil {
				t.Fatal(d.err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case u := <-lc.stopped:
		if u != testRunUnit("a") {
			t.Fatal(u)
		}
	case <-ctx.Done():
		t.Fatal("stop never ran")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("start worker leaked")
	}
}

func TestParallelStartCancellationJoinsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	lc := newRunPoolTestLauncher()
	entered := make(chan struct{})
	lc.startFn = func(ctx context.Context, u string) error { close(entered); <-ctx.Done(); return ctx.Err() }
	p := newRunPool(runKindSandbox, 0, time.Second, t.TempDir(), lc, testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.prepareConcurrency = 2
	done := make(chan struct{})
	go func() { p.controlLoop(ctx); close(done) }()
	p.controlCh <- runControlReq{op: "start", runID: "cancelled"}
	<-entered
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled start worker not joined")
	}
}
