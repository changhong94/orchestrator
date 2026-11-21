package orch

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreparedCatalogRequiresExactCredentialAndLiveAllowlist(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	key := strings.Repeat("1", 64)
	secret, api := defaultTestCredentials(t, key)
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: secret, ManifestKey: key}, "stage", 0, ""); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	credential := filepath.Join(dir, "console.json")
	raw, _ := json.Marshal(map[string]string{"key": api})
	os.WriteFile(credential, raw, 0600)
	template := types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSnp, Ref: "manifest://" + strings.Repeat("b", 64)}.String()
	catalog := map[string]any{"identity": map[string]string{"credential_fingerprint": fmt.Sprintf("%x", sha256.Sum256([]byte(api)))}, "application_started": false, "template": map[string]string{"templateID": template}, "staging_credential_file": credential}
	path := filepath.Join(dir, "catalog.json")
	write := func() { raw, _ := json.Marshal(catalog); os.WriteFile(path, raw, 0600) }
	write()
	o.cfg.ImageTemplateCatalogs = []string{path}
	got, err := o.preparedCatalogs(ctx, "slot-A")
	if err != nil || len(got) != 1 || got[0].ManifestKey != key || got[0].Prepare.RunID != "slot-A" {
		t.Fatalf("registered owner: %d %v", len(got), err)
	}
	catalog["identity"] = map[string]string{"credential_fingerprint": strings.Repeat("0", 64)}
	write()
	if _, err = o.preparedCatalogs(ctx, "slot-A"); err == nil {
		t.Fatal("different credential accepted")
	}
	catalog["identity"] = map[string]string{"credential_fingerprint": fmt.Sprintf("%x", sha256.Sum256([]byte(api)))}
	write()
	hash, _ := store.APISecretHash(secret)
	o.st.RemoveKeyPairByAPISecretFingerprint(ctx, hash)
	got, err = o.preparedCatalogs(ctx, "slot-A")
	if err != nil || len(got) != 0 {
		t.Fatal("revoked catalog staged", err)
	}
}
