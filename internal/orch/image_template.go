package orch

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"io"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// selectImageTemplate only substitutes a plain, immutable image request. Custom
// initialization, workspaces, environment or MMDS requests retain cold-create
// semantics. Snapshot restore still performs identity renewal and mandatory
// agent initialization before publishing Ready.
func (o *Orchestrator) selectImageTemplate(req api.CreateReq) (api.CreateReq, error) {
	if len(req.Metadata) != 0 || len(req.EnvVars) != 0 || len(req.Workspaces) != 0 || req.MMDSHeader != nil || req.AutoReset {
		return req, nil
	}
	image, err := types.ParseTemplateID(req.TemplateID)
	if err != nil || image.Kind != types.KindImg {
		return req, nil
	}
	fingerprint := sha256.Sum256([]byte(req.APIKey))
	for _, path := range o.cfg.ImageTemplateCatalogs {
		if !filepath.IsAbs(path) {
			return req, fmt.Errorf("image template catalog path must be absolute")
		}
		f, err := os.Open(path)
		if err != nil {
			return req, fmt.Errorf("image template catalog: %w", err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(f, (1<<20)+1))
		closeErr := f.Close()
		if readErr != nil {
			return req, readErr
		}
		if closeErr != nil {
			return req, closeErr
		}
		if len(raw) > 1<<20 {
			return req, fmt.Errorf("image template catalog too large")
		}
		var catalog *imageTemplateCatalog
		if cached, ok := o.imageTemplateCatalogCache.Load(path); ok && bytes.Equal(cached.(*imageTemplateCatalogEntry).raw, raw) {
			catalog = cached.(*imageTemplateCatalogEntry).catalog
		} else {
			catalog = new(imageTemplateCatalog)
			if err := json.Unmarshal(raw, catalog); err != nil {
				return req, fmt.Errorf("image template catalog: %w", err)
			}
			o.imageTemplateCatalogCache.Store(path, &imageTemplateCatalogEntry{raw: raw, catalog: catalog})
		}
		if catalog.Identity.Image != req.TemplateID || catalog.Identity.Credential != hex.EncodeToString(fingerprint[:]) {
			continue
		}
		snapshot, err := types.ParseTemplateID(catalog.Template.ID)
		if err != nil || snapshot.Kind != types.KindSnp || snapshot.Profile != image.Profile || catalog.ApplicationStarted == nil || *catalog.ApplicationStarted {
			return req, fmt.Errorf("invalid idle image snapshot catalog")
		}
		var restore struct {
			ForkIdentity bool `json:"fork_identity"`
		}
		if err := json.Unmarshal([]byte(catalog.Template.Metadata[sandboxcfg.NsRestore]), &restore); err != nil || !restore.ForkIdentity {
			return req, fmt.Errorf("image snapshot requires fork identity renewal")
		}
		req.TemplateID = catalog.Template.ID
		req.Metadata = cloneStringMap(catalog.Template.Metadata)
		return req, nil
	}
	return req, nil
}

// Cached values are immutable. Every lookup compares the complete file contents;
// filesystem timestamps never authorize reuse, and returned metadata is cloned.
type imageTemplateCatalog struct {
	Identity struct {
		Image      string `json:"image_template"`
		Credential string `json:"credential_fingerprint"`
	} `json:"identity"`
	ApplicationStarted *bool `json:"application_started"`
	Template           struct {
		ID       string            `json:"templateID"`
		Metadata map[string]string `json:"metadata"`
	} `json:"template"`
}
type imageTemplateCatalogEntry struct {
	raw     []byte
	catalog *imageTemplateCatalog
}
