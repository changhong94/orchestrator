package orch

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPreparedAtomicAdmissionStillWaitsForProxyBarrier(t *testing.T) {
	cfg := &config.Config{}
	cfg.Proxy.ParkTimeout = "1s"
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	sb, tmpl := launchTestSandbox(t, o.cfg, types.ProfileBare, "prepared-atomic-barrier")
	tmpl.Kind = types.KindSnp
	sb.TemplateID = tmpl.String()
	sb.LaunchMode = types.LaunchMemory
	lease := store.PreparedVMLease{RunID: sb.ID, Port: "7", InnerIP: "10.0.0.2/24", MAC: "02:00:00:00:00:07", Spec: json.RawMessage(`{}`)}
	if err := o.st.SaveRunPoolOwnership(ctx, store.RunPoolOwnership{RunID: sb.ID, Kind: "sandbox", Pool: "ready", ResourcePool: "default", Unit: "prepared@" + sb.ID + ".service"}); err != nil {
		t.Fatal(err)
	}
	o.runs.restore(sb.ID, "prepared@"+sb.ID+".service")
	if err := o.st.SavePreparedVM(ctx, lease); err != nil {
		t.Fatal(err)
	}
	barrier := newFakeRouteBarrier(errors.New("injected proxy failure"), nil)
	o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{barrier: barrier})
	done := make(chan error, 1)
	go func() { _, _, err := o.acceptFreshLaunch(ctx, sb, tmpl, nil, sb.ID); done <- err }()
	select {
	case <-barrier.waiting:
	case err := <-done:
		t.Fatalf("did not reach barrier: %v", err)
	case <-time.After(time.Second):
		t.Fatal("barrier timeout")
	}
	row, err := o.st.Get(context.Background(), sb.ID)
	if err != nil || row == nil || row.RunID != sb.ID || row.VswitchPort != lease.Port {
		t.Fatalf("ownership not atomic: %+v %v", row, err)
	}
	if lease, err := o.st.GetPreparedVM(ctx, sb.ID); err != nil || lease != nil {
		t.Fatal("two durable owners", err)
	}
	select {
	case err := <-done:
		t.Fatalf("returned before ACK: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if lc.starts.Load() != 0 {
		t.Fatal("launched before proxy ACK")
	}
	barrier.release()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ignored barrier failure")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("failed admission cleanup timeout")
	}
	if lc.stops.Load() == 0 {
		t.Fatal("failed proxy barrier did not fence runner")
	}
	row, err = o.st.Get(ctx, sb.ID)
	if err != nil || row == nil || row.State != types.StateDead || row.RunID != "" || row.VswitchPort != "" {
		t.Fatalf("failed barrier retained ownership: %+v %v", row, err)
	}
}
