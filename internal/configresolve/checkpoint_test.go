package configresolve

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointCompanionLookup(t *testing.T) {
	dir := t.TempDir()
	node := filepath.Join(dir, "node-ctl")
	e := ExecutablesForNodeCtl(node)
	if got := e.CheckpointCtl(); got != node {
		t.Fatal(got)
	}
	helper := filepath.Join(dir, "node-checkpoint")
	if err := os.WriteFile(helper, []byte("broken helper must not trigger fallback"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := e.CheckpointCtl(); got != helper {
		t.Fatal(got)
	}
	if err := os.Remove(helper); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", helper); err != nil {
		t.Fatal(err)
	}
	if got := e.CheckpointCtl(); got != helper {
		t.Fatal("dangling helper must fail, not retry", got)
	}
	if got := (Executables{}).CheckpointCtl(); got != "node-ctl" {
		t.Fatal("optional helper searched PATH", got)
	}
}
