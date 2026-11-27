package taskartifact

import (
	"context"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPrepareSlotKeepsAuthorityVersionsAndOneShotClaim(t *testing.T) {
	runtime := filepath.Join(t.TempDir(), "runtime.bundle")
	required := writeAdmissionRuntime(t, runtime, strings.Repeat("c", 64))
	dir, root := writeTaskSnapshot(t, "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot:\n  runtime_ref: "+required+"\n")
	cfg := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(cfg, []byte("crypto:\n  local: off\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MANIFEST_KEY", strings.Repeat("a", 64))
	t.Setenv("KUASAR_PREPARED_ARTIFACT_CACHE", filepath.Join(t.TempDir(), "cache"))
	ctx := context.Background()
	spec := configsock.ArtifactPrepareSpec{RunID: "slot", RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: root, RelativeDir: dir, ManifestConfig: cfg, SnapshotRuntime: "file://" + runtime}
	// Empty cache follows the original publication path before the warm read.
	if _, _, _, err := PrepareSlot(ctx, spec); err != nil {
		t.Fatal(err)
	}
	got, files, m, err := PrepareSlot(ctx, spec)
	if err != nil || m == nil {
		t.Fatalf("prepare: %v", err)
	}
	want, err := prepareUncached(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if got.Summary.ResolutionDigest != want.Summary.ResolutionDigest {
		t.Fatal("wrong binding")
	}
	stamps, err := StageFiles(spec, want)
	if err != nil || !reflect.DeepEqual(stamps, files) {
		t.Fatalf("wrong stamps: %v", err)
	}
	_, _, changed, err := PrepareSlot(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	a, err := preparedCacheFor(spec)
	if err != nil {
		t.Fatal(err)
	}
	// Claim uses the privately held authenticated entry, not a second cache read.
	if err = os.Remove(a.path); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MANIFEST_KEY", strings.Repeat("b", 64))
	if _, hit, _ := m.Claim(ctx, spec); hit {
		t.Fatal("wrong tenant accepted")
	}
	t.Setenv("MANIFEST_KEY", strings.Repeat("a", 64))
	spec.RunID = "assigned"
	if _, hit, err := m.Claim(ctx, spec); err != nil || !hit {
		t.Fatalf("claim: %v", err)
	}
	if _, hit, _ := m.Claim(ctx, spec); hit {
		t.Fatal("second claim accepted")
	}
	f, err := os.OpenFile(root, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte("changed")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, hit, err := changed.Claim(ctx, spec); hit || err == nil {
		t.Fatal("changed source accepted")
	}
}
