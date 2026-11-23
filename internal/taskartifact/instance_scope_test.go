package taskartifact

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/instanceenv"
)

func TestPreparedMaterialsKeepConcurrentCustomerAuthority(t *testing.T) {
	dir, root := writeTaskSnapshot(t, "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n")
	cfg := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(cfg, []byte("crypto:\n  local: off\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MANIFEST_KEY", strings.Repeat("c", 64))
	t.Setenv("KUASAR_PREPARED_ARTIFACT_CACHE", filepath.Join(t.TempDir(), "cache"))
	spec := configsock.ArtifactPrepareSpec{RunID: "test", RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: root, RelativeDir: dir, ManifestConfig: cfg}
	var contexts [2]context.Context
	var raw [2][]byte
	var wg sync.WaitGroup
	for i, key := range []string{"a", "b"} {
		contexts[i], _ = instanceenv.With(context.Background(), []string{"MANIFEST_KEY=" + strings.Repeat(key, 64)})
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := prepareCached(contexts[i], spec); err != nil {
				t.Error(err)
				return
			}
			var err error
			raw[i], err = ExportSlotMaterial(contexts[i], spec)
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	for i := range contexts {
		var m SlotMaterial
		if err := json.Unmarshal(raw[i], &m); err != nil {
			t.Fatal(err)
		}
		if _, hit, err := m.Claim(contexts[1-i], spec); err != nil || hit {
			t.Fatalf("cross-tenant claim hit=%v err=%v", hit, err)
		}
		if _, hit, err := m.Claim(contexts[i], spec); err != nil || !hit {
			t.Fatalf("own claim hit=%v err=%v", hit, err)
		}
	}
	if os.Getenv("MANIFEST_KEY") != strings.Repeat("c", 64) {
		t.Fatal("changed process customer key")
	}
}

func TestTypedSlotMaterialsKeepWireIdentityAndIndependentConsumption(t *testing.T) {
	dir, root := writeTaskSnapshot(t, "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n")
	cfg := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(cfg, []byte("crypto:\n  local: off\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUASAR_PREPARED_ARTIFACT_CACHE", filepath.Join(t.TempDir(), "cache"))
	ctx, _ := instanceenv.With(context.Background(), []string{"MANIFEST_KEY=" + strings.Repeat("a", 64)})
	spec := configsock.ArtifactPrepareSpec{RunID: "typed", RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: root, RelativeDir: dir, ManifestConfig: cfg}
	if _, err := prepareCached(ctx, spec); err != nil {
		t.Fatal(err)
	}
	a, err := PrepareSlotMaterial(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	b, err := PrepareSlotMaterial(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := ExportSlotMaterial(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	typed, err := json.Marshal(a)
	if err != nil || string(typed) != string(wire) {
		t.Fatal("typed and wire metadata differ", err)
	}
	wrong, _ := instanceenv.With(context.Background(), []string{"MANIFEST_KEY=" + strings.Repeat("b", 64)})
	if _, hit, err := a.Claim(wrong, spec); err != nil || hit {
		t.Fatal("cross-key adoption", err)
	}
	if _, hit, err := a.Claim(ctx, spec); err != nil || !hit {
		t.Fatal("first claim", err)
	}
	if _, hit, err := a.Claim(ctx, spec); err != nil || hit {
		t.Fatal("reused one-shot material", err)
	}
	if _, hit, err := b.Claim(ctx, spec); err != nil || !hit {
		t.Fatal("one slot consumed peer material", err)
	}
}
