package orch

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Reproduce the assignment-fence -> database / cursor -> fence inversion.
// Holding the fence is intentional: admission must be able to commit while a
// maintenance pass waits for it, without relying on its request timing out.
func TestRunSessionMaintenanceReleasesCursorBeforeAssignmentFence(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "maintenance-cursor-fence")
	session, ok, err := f.o.RegisterRunSession(context.Background(), runKindSandbox, f.sb.RunID)
	if err != nil || !ok {
		t.Fatalf("register: %v %v", ok, err)
	}
	defer session.Close(true)
	f.o.runSessionsMu.Lock()
	locked := true
	defer func() {
		if locked {
			f.o.runSessionsMu.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.o.queueMissingRunSessionChecks(ctx) }()
	// Synchronize on the actual blocked call, rather than an arbitrary sleep.
	deadline := time.Now().Add(2 * time.Second)
	waiting := false
	for time.Now().Before(deadline) {
		b := make([]byte, 1<<20)
		n := runtime.Stack(b, true)
		for _, stack := range strings.Split(string(b[:n]), "\n\n") {
			if strings.Contains(stack, "queueMissingRunSessionChecks") && strings.Contains(stack, "runSessionActive") {
				waiting = true
				break
			}
		}
		if waiting {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !waiting {
		t.Fatal("maintenance did not reach assignment fence")
	}
	writeCtx, writeCancel := context.WithTimeout(context.Background(), time.Second)
	err = f.o.st.SetState(writeCtx, f.sb.ID, types.StateRunning)
	writeCancel()
	f.o.runSessionsMu.Unlock()
	locked = false
	if err != nil {
		t.Errorf("maintenance retained writer connection while waiting for fence: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("maintenance did not complete")
	}
}
