package taskartifact

import (
	"context"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreparedCacheRunBindingAndInvalidation(t *testing.T) {
	dir, root := writeTaskSnapshot(t, "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n")
	config := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(config, []byte("crypto:\n  local: off\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MANIFEST_KEY", strings.Repeat("a", 64))
	cacheDir := filepath.Join(t.TempDir(), "private")
	t.Setenv("KUASAR_PREPARED_ARTIFACT_CACHE", cacheDir)
	spec := configsock.ArtifactPrepareSpec{RunID: "first", RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: root, RelativeDir: dir, ManifestConfig: config}
	a, err := preparedCacheFor(spec)
	if err != nil || a == nil {
		t.Fatalf("cache %v", err)
	}
	first, err := prepareCached(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(a.path); err != nil {
		t.Fatalf("cache not prepared: %v", err)
	}
	spec.RunID = "second"
	got, err := a.load(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	want, err := prepareUncached(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if got.Summary.ResolutionDigest != want.Summary.ResolutionDigest || got.Summary.ResolutionDigest == first.Summary.ResolutionDigest {
		t.Fatal("run binding not recomputed")
	}
	raw, err := os.ReadFile(a.path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if err = os.WriteFile(a.path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = a.load(context.Background(), spec); err == nil {
		t.Fatal("tampered cache accepted")
	}
	if _, err = prepareCached(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MANIFEST_KEY", strings.Repeat("b", 64))
	other, err := preparedCacheFor(spec)
	if err != nil {
		t.Fatal(err)
	}
	if other.path == a.path {
		t.Fatal("tenant cache namespace reused")
	}
	if _, err = other.load(context.Background(), spec); err == nil {
		t.Fatal("cross-tenant cache accepted")
	}
	t.Setenv("MANIFEST_KEY", strings.Repeat("a", 64))
	f, err := os.OpenFile(root, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("changed"))
	_ = f.Close()
	if _, err = a.load(context.Background(), spec); err == nil {
		t.Fatal("changed carrier accepted")
	}
}
