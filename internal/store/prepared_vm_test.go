package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPreparedVMAtomicAdoption(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	id := "prepared-test"
	lease := PreparedVMLease{RunID: id, Port: "7", FloatingIP: "10.2.0.7", InnerIP: "10.0.0.2/24", MAC: "02:00:00:00:00:07", Spec: json.RawMessage(`{"SandboxID":"prepared-test"}`)}
	if st.SavePreparedVM(ctx, lease) == nil {
		t.Fatal("lease accepted without durable runner")
	}
	if err := st.SaveRunPoolOwnership(ctx, RunPoolOwnership{RunID: id, Kind: "sandbox", Pool: "ready", ResourcePool: "default", Unit: "prepared@" + id + ".service"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SavePreparedVM(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if err := st.SavePreparedVM(ctx, lease); err == nil {
		t.Fatal("overwrote existing lease")
	}
	if _, err := st.AdoptPreparedVM(ctx, "different", id); err == nil {
		t.Fatal("renamed prepared identity")
	}
	if _, err := st.AdoptPreparedVM(ctx, id, id); err == nil {
		t.Fatal("adopted absent sandbox")
	}
	if saved, err := st.GetPreparedVM(ctx, id); err != nil || saved == nil {
		t.Fatal("failed adoption lost owner", err)
	}
	sb := testSandboxWithServiceCredentials(id, types.ProfileE2B)
	sb.State = types.StateStarting
	sb.TemplateID = (types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSbx, Ref: "manifest://" + strings.Repeat("a", 64)}).String()
	sb.LaunchMode = types.LaunchCold
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	// Force failure after the sandbox UPDATE but before deleting the old owner.
	// A failed handoff must leave neither a half-bound task nor a lost pool lease.
	if _, err := st.db.Exec(`CREATE TRIGGER reject_lease_delete BEFORE DELETE ON prepared_vm_leases BEGIN SELECT RAISE(ABORT,'injected durability failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AdoptPreparedVM(ctx, id, id); err == nil {
		t.Fatal("ignored ownership transaction failure")
	}
	untouched, err := st.Get(ctx, id)
	if err != nil || untouched.RunID != "" || untouched.VswitchPort != "" {
		t.Fatal("failed transaction partially transferred ownership", err)
	}
	if _, err = st.db.Exec(`DROP TRIGGER reject_lease_delete`); err != nil {
		t.Fatal(err)
	}
	got, err := st.AdoptPreparedVM(ctx, id, id)
	if err != nil || got.Port != "7" {
		t.Fatal(got, err)
	}
	if saved, err := st.GetPreparedVM(ctx, id); err != nil || saved != nil {
		t.Fatal("two owners after adoption", err)
	}
	sb, err = st.Get(ctx, id)
	if err != nil || sb.RunID != id || sb.VswitchPort != "7" || sb.PortMAC != lease.MAC || sb.InnerIP != lease.InnerIP {
		t.Fatal(sb, err)
	}
	if _, err = st.AdoptPreparedVM(ctx, id, id); err == nil {
		t.Fatal("duplicate handoff succeeded")
	}
}
