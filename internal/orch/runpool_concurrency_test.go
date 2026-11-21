package orch

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLimitedPreparationServesRequestsAcrossSuccessfulWaves(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lc := newRunPoolTestLauncher()
	p := newRunPool(runKindSandbox, 0, 4*time.Second, t.TempDir(), lc, testRunUnit, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.prepareConcurrency = 2
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() { _, err := p.Assign(ctx, "task", func(string) error { return nil }); done <- err }()
	}
	first := []string{}
	for len(first) < 2 {
		select {
		case u := <-lc.started:
			first = append(first, u)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case u := <-lc.started:
		t.Fatalf("third start before any readiness: %s", u)
	case <-time.After(50 * time.Millisecond):
	}
	ready := func(u string) {
		id := strings.TrimSuffix(strings.TrimPrefix(u, "sandbox-runner@"), ".service")
		go func() { _, _, _ = p.WaitAssignment(ctx, id) }()
	}
	for _, u := range first {
		ready(u)
	}
	for n := 2; n < 10; n++ {
		select {
		case u := <-lc.started:
			ready(u)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for i := 0; i < 10; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
