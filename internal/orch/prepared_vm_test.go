package orch

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

type preparedReleaseSwitch struct {
	capturingNetworkVS
	calls int
	fail  bool
}

func (v *preparedReleaseSwitch) Detach(context.Context, string) error {
	v.calls++
	if v.fail {
		return errors.New("injected detach failure")
	}
	return nil
}

func TestPreparedRetirementRetainsLeaseUntilDetachSucceeds(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	v := &preparedReleaseSwitch{fail: true}
	o.vs = v
	ctx := context.Background()
	id := "prepared-retirement"
	if err := o.st.SaveRunPoolOwnership(ctx, store.RunPoolOwnership{RunID: id, Kind: "sandbox", Pool: "ready", ResourcePool: "default", Unit: "prepared@" + id + ".service"}); err != nil {
		t.Fatal(err)
	}
	lease := store.PreparedVMLease{RunID: id, Port: "7", InnerIP: "10.0.0.2/24", MAC: "02:00:00:00:00:07", Spec: json.RawMessage(`{}`)}
	if err := o.st.SavePreparedVM(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if err := o.releasePreparedVM(ctx, id); err == nil {
		t.Fatal("lost detach error")
	}
	if saved, err := o.st.GetPreparedVM(ctx, id); err != nil || saved == nil {
		t.Fatal("lost retry ownership", err)
	}
	v.fail = false
	if err := o.releasePreparedVM(ctx, id); err != nil {
		t.Fatal(err)
	}
	if saved, err := o.st.GetPreparedVM(ctx, id); err != nil || saved != nil {
		t.Fatal("lease survived successful release", err)
	}
	if err := o.releasePreparedVM(ctx, id); err != nil {
		t.Fatal(err)
	}
	if v.calls != 2 {
		t.Fatalf("released the same port after ownership ended: %d", v.calls)
	}
}

func TestPreparedRecoveryReleasesStoppedLeaseBeforeOwnership(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	o.lc = newRunPoolTestLauncher()
	v := &preparedReleaseSwitch{fail: true}
	o.vs = v
	ctx := context.Background()
	id := "stopped-prepared-recovery"
	owner := store.RunPoolOwnership{RunID: id, Kind: "sandbox", Pool: "ready", ResourcePool: "default", Unit: "prepared@" + id + ".service"}
	if err := o.st.SaveRunPoolOwnership(ctx, owner); err != nil {
		t.Fatal(err)
	}
	if err := o.st.SavePreparedVM(ctx, store.PreparedVMLease{RunID: id, Port: "7", InnerIP: "10.0.0.2/24", MAC: "02:00:00:00:00:07", Spec: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := o.RestoreRunPoolIdentities(ctx); err == nil {
		t.Fatal("ignored detach failure")
	}
	rows, err := o.st.RunPoolOwnerships(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("lost retry ownership: %v %v", rows, err)
	}
	lease, err := o.st.GetPreparedVM(ctx, id)
	if err != nil || lease == nil {
		t.Fatal("lost retry lease", err)
	}
	v.fail = false
	if err := o.RestoreRunPoolIdentities(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err = o.st.RunPoolOwnerships(ctx)
	if err != nil || len(rows) != 0 {
		t.Fatalf("ownership survived: %v %v", rows, err)
	}
	lease, err = o.st.GetPreparedVM(ctx, id)
	if err != nil || lease != nil {
		t.Fatal("lease survived", err)
	}
	if v.calls != 2 {
		t.Fatalf("detach calls=%d", v.calls)
	}
}
