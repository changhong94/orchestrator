package orch

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"testing"
	"time"
)

func TestPreparedRunCreatesTaskBeforeAssignmentAndRefills(t *testing.T) {
	p, lc, ctx, cancel := startRunPoolTest(t, 1)
	defer cancel()
	runID, waiter := addIdleRunForTest(t, p, lc, ctx)
	entered, commit := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		task, run, err := p.AssignNew(ctx, nil, func(run string) (string, error) {
			if run != runID {
				return "", errors.New("prepared VM identity changed")
			}
			close(entered)
			<-commit
			return run, nil
		})
		if err == nil && (task != runID || run != runID) {
			err = errors.New("assignment identity mismatch")
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("creator not entered")
	}
	select {
	case <-waiter.resp:
		t.Fatal("runner received task before durable creation")
	default:
	}
	close(commit)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if response := <-waiter.resp; response.err != nil || response.taskID != runID {
		t.Fatal(response)
	}
	select {
	case replacement := <-lc.started:
		if runIDFromTestUnit(replacement) == runID {
			t.Fatal("reused running VM for reserve")
		}
	case <-time.After(time.Second):
		t.Fatal("claim did not replenish reserve")
	}
}

func TestPreparedRunFailedCreationRetiresVM(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit-error", true: "empty-identity"}[empty], func(t *testing.T) {
			p, lc, ctx, cancel := startRunPoolTest(t, 1)
			defer cancel()
			runID, waiter := addIdleRunForTest(t, p, lc, ctx)
			_, _, err := p.AssignNew(ctx, nil, func(string) (string, error) {
				if empty {
					return "", nil
				}
				return "", errors.New("database commit failed")
			})
			if err == nil {
				t.Fatal("accepted failed creation")
			}
			if response := <-waiter.resp; response.err == nil {
				t.Fatal("failed VM assigned")
			}
			select {
			case unit := <-lc.stopped:
				if runIDFromTestUnit(unit) != runID {
					t.Fatal("stopped unrelated VM")
				}
			case <-time.After(time.Second):
				t.Fatal("failed VM not retired")
			}
		})
	}
}

func TestPreparedRunCanceledBeforeCommitDoesNotCreate(t *testing.T) {
	p, _, ctx, cancel := startRunPoolTest(t, 0)
	defer cancel()
	request, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	_, _, err := p.AssignNew(request, nil, func(string) (string, error) { t.Error("created canceled task"); return "task", nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestPreparedRunPoolCapacityTracksClaimedOwner(t *testing.T) {
	runs := &runIndex{}
	lc := callbackLauncher(t, runs)
	capacity := 1
	pools, ctx, _ := startPoolSet(t, runKindSandbox, []config.RunPoolConfig{{Unit: "prepared@.service", Capacity: &capacity}}, lc, runs)
	task, run, err := pools.AssignNew(ctx, "", nil, func(id string) (string, error) { return id, nil })
	if err != nil || task != run || run == "" {
		t.Fatalf("claim %q %q %v", task, run, err)
	}
	called := false
	if _, _, err = pools.AssignNew(ctx, "", nil, func(id string) (string, error) { called = true; return id, nil }); err == nil || called {
		t.Fatal("prepared claim bypassed running capacity")
	}
	runs.forget(run)
	task, run, err = pools.AssignNew(ctx, "", nil, func(id string) (string, error) { return id, nil })
	if err != nil || task != run {
		t.Fatalf("capacity not released: %v", err)
	}
	runs.forget(run)
	runs.mu.Lock()
	defer runs.mu.Unlock()
	if pools.pools[0].reserved != 0 {
		t.Fatal("claim leaked capacity reservation")
	}
}
