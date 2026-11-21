package store

import (
	"context"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"reflect"
	"testing"
)

func TestLaunchIdentityTracksCommittedOwnershipWithoutSecrets(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sb := testSandboxWithServiceCredentials("launch-projection", types.ProfileE2B)
	if err := s.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	full, err := s.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetLaunchIdentity(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != full.ID || got.RunID != full.RunID || got.State != full.State || got.TemplateID != full.TemplateID || got.CreatedUnix != full.CreatedUnix || got.LaunchMode != full.LaunchMode || !reflect.DeepEqual(got.ResumeSource, full.ResumeSource) {
		t.Fatal("identity projection changed")
	}
	if got.APISecret != "" || got.ManifestKey != "" || got.Metadata != nil || got.Env != nil {
		t.Fatal("secret/config copied into identity projection")
	}
	if _, err = s.db.ExecContext(ctx, `UPDATE sandboxes SET run_id='successor',state='running' WHERE id=?`, sb.ID); err != nil {
		t.Fatal(err)
	}
	next, err := s.GetLaunchIdentity(ctx, sb.ID)
	if err != nil || next.RunID != "successor" || next.State != types.StateRunning {
		t.Fatal("stale identity", err)
	}
	if _, err = s.db.ExecContext(ctx, `DELETE FROM sandboxes WHERE id=?`, sb.ID); err != nil {
		t.Fatal(err)
	}
	next, err = s.GetLaunchIdentity(ctx, sb.ID)
	if err != nil || next != nil {
		t.Fatal("deleted identity remained", err)
	}
}
