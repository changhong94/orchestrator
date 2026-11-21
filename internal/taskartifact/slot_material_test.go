package taskartifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSlotMaterialRebindsAndRejectsChangedAuthorityOrSource(t *testing.T) {
	dir, root := writeTaskSnapshot(t, "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n")
	config := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(config, []byte("crypto:\n  local: off\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MANIFEST_KEY", strings.Repeat("a", 64))
	t.Setenv("KUASAR_PREPARED_ARTIFACT_CACHE", filepath.Join(t.TempDir(), "private"))
	spec := configsock.ArtifactPrepareSpec{RunID: "prepared", RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: root, RelativeDir: dir, ManifestConfig: config}
	if _, err := prepareCached(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	raw, err := ExportSlotMaterial(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	decode := func() *SlotMaterial {
		var m SlotMaterial
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		return &m
	}
	// Required disk dependencies must survive the private helper's JSON wire.
	var wire SlotMaterial
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	dependency := filepath.Join(dir, "required-disk")
	if err := os.WriteFile(dependency, []byte("disk"), 0600); err != nil {
		t.Fatal(err)
	}
	wire.Entry.Resolution.RequiredRefs = append(wire.Entry.Resolution.RequiredRefs, "file://"+dependency)
	wire.Entry.Files[dependency], err = fileStamp(dependency)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	material := decode()
	spec.RunID = "assigned"
	spec.RootSandboxRef = material.Entry.Result.Summary.RootSource.SandboxRef
	t.Setenv("MANIFEST_KEY", strings.Repeat("b", 64))
	if _, hit, err := material.Claim(context.Background(), spec); err != nil || hit {
		t.Fatalf("wrong tenant accepted: %v", err)
	}
	t.Setenv("MANIFEST_KEY", strings.Repeat("a", 64))
	other := spec
	other.ReadSourceImageConfig = true
	if _, hit, _ := material.Claim(context.Background(), other); hit {
		t.Fatal("different request accepted")
	}
	got, hit, err := material.Claim(context.Background(), spec)
	if err != nil || !hit {
		t.Fatalf("claim: %v", err)
	}
	want, err := prepareUncached(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	want.cacheResolution.RequiredRefs = append(want.cacheResolution.RequiredRefs, "file://"+dependency)
	encoded, _ := json.Marshal(want.cacheResolution)
	digest := sha256.Sum256(encoded)
	if got.Summary.ResolutionDigest != hex.EncodeToString(digest[:]) {
		t.Fatal("wrong run binding")
	}
	if _, hit, _ := material.Claim(context.Background(), spec); hit {
		t.Fatal("reused one-shot material")
	}
	f, err := os.OpenFile(root, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte("changed"))
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, hit, err := decode().Claim(context.Background(), spec); err == nil || hit {
		t.Fatal("changed source accepted")
	}
}
