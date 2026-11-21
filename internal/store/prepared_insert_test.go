package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPreparedVMInsertAtomicFailureAndSuccess(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	id := "prepared-insert"
	lease := PreparedVMLease{RunID: id, Port: "7", FloatingIP: "10.2.0.7", InnerIP: "10.0.0.2/24", MAC: "02:00:00:00:00:07", Spec: json.RawMessage(`{}`)}
	if err := st.SaveRunPoolOwnership(ctx, RunPoolOwnership{RunID: id, Kind: "sandbox", Pool: "ready", ResourcePool: "default", Unit: "prepared@" + id + ".service"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SavePreparedVM(ctx, lease); err != nil {
		t.Fatal(err)
	}
	sb := testSandboxWithServiceCredentials(id, types.ProfileE2B)
	sb.State = types.StateStarting
	sb.TemplateID = (types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSbx, Ref: "manifest://" + strings.Repeat("a", 64)}).String()
	sb.LaunchMode = types.LaunchCold
	if _, err := st.db.Exec(`CREATE TRIGGER reject_prepared_delete BEFORE DELETE ON prepared_vm_leases BEGIN SELECT RAISE(ABORT,'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertSandboxWithPreparedVM(ctx, sb, "", nil, id); err == nil {
		t.Fatal("ignored failed handoff")
	}
	if row, err := st.Get(ctx, id); err != nil || row != nil {
		t.Fatalf("partial sandbox: %+v %v", row, err)
	}
	if row, err := st.GetPreparedVM(ctx, id); err != nil || row == nil {
		t.Fatalf("lost original owner: %+v %v", row, err)
	}
	if _, err := st.db.Exec(`DROP TRIGGER reject_prepared_delete`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertSandboxWithPreparedVM(ctx, sb, "", nil, "different"); err == nil {
		t.Fatal("identity mismatch accepted")
	}
	if row, err := st.Get(ctx, id); err != nil || row != nil {
		t.Fatal("mismatch partially inserted", err)
	}
	got, err := st.InsertSandboxWithPreparedVM(ctx, sb, "", nil, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunID != id || got.Port != lease.Port {
		t.Fatal(got)
	}
	row, err := st.Get(ctx, id)
	if err != nil || row.RunID != id || row.VswitchPort != lease.Port {
		t.Fatal(row, err)
	}
	if row, err := st.GetPreparedVM(ctx, id); err != nil || row != nil {
		t.Fatal("duplicate owner", row, err)
	}
	if _, err := st.InsertSandboxWithPreparedVM(ctx, sb, "", nil, id); err == nil {
		t.Fatal("duplicate insert succeeded")
	}
	row, err = st.Get(ctx, id)
	if err != nil || row.RunID != id {
		t.Fatal("duplicate attempt changed owner", err)
	}

	var seq int
	var name, path string
	if err := st.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	row, err = reopened.Get(ctx, id)
	if err != nil || row.RunID != id || row.VswitchPort != lease.Port {
		t.Fatal("ownership lost after reopen", err)
	}
	if lease, err := reopened.GetPreparedVM(ctx, id); err != nil || lease != nil {
		t.Fatal("pool lease resurrected after reopen", err)
	}
}
