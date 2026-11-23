package orch

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestImageTemplateSelectionPreservesTenantAndColdSemantics(t *testing.T) {
	o := testOrch(t)
	image := types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}.String()
	snapshot := types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSnp, Ref: "manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}.String()
	key := "test-tenant"
	fingerprint := sha256.Sum256([]byte(key))
	catalog := map[string]any{"identity": map[string]any{"image_template": image, "credential_fingerprint": fmt.Sprintf("%x", fingerprint)}, "application_started": false, "template": map[string]any{"templateID": snapshot, "metadata": map[string]string{"kuasar-sandbox.restore": `{"fork_identity":true}`}}}
	path := filepath.Join(t.TempDir(), "catalog.json")
	raw, _ := json.Marshal(catalog)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	o.cfg.ImageTemplateCatalogs = []string{path}
	for _, tc := range []struct {
		name string
		req  api.CreateReq
		want string
	}{
		{"idle template", api.CreateReq{APIKey: key, TemplateID: image}, snapshot},
		{"other tenant", api.CreateReq{APIKey: "other", TemplateID: image}, image},
		{"custom metadata", api.CreateReq{APIKey: key, TemplateID: image, Metadata: map[string]string{"custom": "value"}}, image},
		{"custom environment", api.CreateReq{APIKey: key, TemplateID: image, EnvVars: map[string]string{"A": "B"}}, image},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := o.selectImageTemplate(tc.req)
			if err != nil || got.TemplateID != tc.want {
				t.Fatalf("got %s %v", got.TemplateID, err)
			}
		})
	}
	// Mutating the returned map must not modify the cached tenant catalog.
	got, err := o.selectImageTemplate(api.CreateReq{APIKey: key, TemplateID: image})
	if err != nil {
		t.Fatal(err)
	}
	got.Metadata["kuasar-sandbox.restore"] = `{"fork_identity":false}`
	if _, err := o.selectImageTemplate(api.CreateReq{APIKey: key, TemplateID: image}); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := o.selectImageTemplate(api.CreateReq{APIKey: key, TemplateID: image}); err == nil {
		t.Fatal("stale cached catalog accepted after same-time content change")
	}

	catalog["template"] = map[string]any{"templateID": snapshot, "metadata": map[string]string{"kuasar-sandbox.restore": `{"fork_identity":false}`}}
	raw, _ = json.Marshal(catalog)
	os.WriteFile(path, raw, 0600)
	if _, err := o.selectImageTemplate(api.CreateReq{APIKey: key, TemplateID: image}); err == nil {
		t.Fatal("missing identity barrier accepted")
	}
}
