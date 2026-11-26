package orch

import (
	"context"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Use the real recovery path with a launcher reporting a live process, but no
// guest-ready witness. It demonstrates why neither moving Running earlier nor
// leaving Starting in storage can replace a recovery protocol.
func TestLaunchAuthorizationNeedsRecoveryWitness(t *testing.T) {
	for _, state := range []types.State{types.StateStarting, types.StateRunning} {
		t.Run(string(state), func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "authorization-"+string(state))
			f.sb.State = state
			if state == types.StateStarting {
				f.sb.LaunchMode = types.LaunchImage
			}
			if err := f.o.st.Put(context.Background(), f.sb); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			f.o.SetLifecycleContext(ctx)
			defer func() {
				cancel()
				drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				if err := f.o.DrainPauses(drain); err != nil {
					t.Error(err)
				}
			}()
			if err := f.o.ReconcileSandboxes(ctx); err != nil {
				t.Fatal(err)
			}
			row, err := f.o.st.Get(ctx, f.sb.ID)
			if err != nil || row == nil {
				t.Fatal(err)
			}
			_, stops := f.lc.stopSnapshot()
			if state == types.StateStarting {
				if stops == 0 || row.State != types.StateDead {
					t.Fatalf("starting recovery changed: state=%s stops=%d", row.State, stops)
				}
				t.Log("live runner with only durable Starting is torn down; a volatile ready confirmation cannot survive controller recovery")
			} else {
				if stops != 0 || row.State != types.StateRunning {
					t.Fatalf("running recovery changed: state=%s stops=%d", row.State, stops)
				}
				t.Log("durable Running is adopted from live runner ownership without a new guest-ready witness; premature commit is unsafe")
			}
		})
	}
}
