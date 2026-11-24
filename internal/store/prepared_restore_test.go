package store

import (
	"context"
	"encoding/json"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"strings"
	"testing"
)

func TestPreparedRestorePreservesLogicalIdentityAndAtomicOwnership(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sid, run := "saved-sandbox", "empty-holder"
	if err := st.SaveRunPoolOwnership(ctx, RunPoolOwnership{RunID: run, Kind: "sandbox", Pool: "ready", ResourcePool: "default", Unit: "prepared@" + run + ".service"}); err != nil {
		t.Fatal(err)
	}
	lease := PreparedVMLease{RunID: run, Port: "7", MAC: "02:00:00:00:00:07", InnerIP: "10.0.0.2/24", Spec: json.RawMessage(`{"RestoreOnly":true,"SandboxID":"empty-holder","PathID":"empty-holder"}`)}
	if err := st.SavePreparedVM(ctx, lease); err != nil {
		t.Fatal(err)
	}
	sb := testSandboxWithServiceCredentials(sid, types.ProfileE2B)
	sb.State = types.StateStarting
	sb.LaunchMode = types.LaunchMemory
	sb.TemplateID = (types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSbx, Ref: "manifest://" + strings.Repeat("a", 64)}).String()
	sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("b", 64), SandboxRef: "manifest://" + strings.Repeat("c", 64)}
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AdoptPreparedVM(ctx, sid, run); err == nil {
		t.Fatal("ordinary adoption lost identity guard")
	}
	if _, err := st.db.Exec(`CREATE TRIGGER fail_restore_lease BEFORE DELETE ON prepared_vm_leases BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AdoptPreparedRestoreVM(ctx, sid, run); err == nil {
		t.Fatal("ignored commit failure")
	}
	unchanged, err := st.Get(ctx, sid)
	if err != nil || unchanged.RunID != "" || unchanged.VswitchPort != "" {
		t.Fatal("partial assignment", err)
	}
	if retained, err := st.GetPreparedVM(ctx, run); err != nil || retained == nil {
		t.Fatal("lost lease", err)
	}
	if _, err := st.db.Exec(`DROP TRIGGER fail_restore_lease`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AdoptPreparedRestoreVM(ctx, sid, run); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(ctx, sid)
	if err != nil || got.ID != sid || got.RunID != run || got.VswitchPort != lease.Port || got.ResumeSource != sb.ResumeSource {
		t.Fatal("logical/source identity changed", err)
	}
	if retained, err := st.GetPreparedVM(ctx, run); err != nil || retained != nil {
		t.Fatal("dual network owner", err)
	}
	if _, err := st.AdoptPreparedRestoreVM(ctx, sid, run); err == nil {
		t.Fatal("duplicate adoption")
	}
}
