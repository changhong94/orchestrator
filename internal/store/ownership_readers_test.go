package store

import (
	"context"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"testing"
	"time"
)

func TestOwnershipReadersDoNotWaitForOrExposeUncommittedWriter(t *testing.T) {
	s := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sb := testSandboxWithServiceCredentials("reader-owner", types.ProfileE2B)
	if err := s.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE sandboxes SET run_id='uncommitted' WHERE id=?", sb.ID); err != nil {
		t.Fatal(err)
	}
	row, err := s.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.RunID == "uncommitted" {
		t.Fatal("read exposed uncommitted ownership")
	}
	if _, err = s.readDB().ExecContext(ctx, "DELETE FROM sandboxes"); err == nil {
		t.Fatal("reader permitted write")
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	row, err = s.Get(ctx, sb.ID)
	if err != nil || row.RunID != "uncommitted" {
		t.Fatal("committed ownership not visible", err)
	}
}
