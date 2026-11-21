package orch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Only operator-registered catalogs whose credential is still allowed may be
// staged. This private response is restricted to the exact idle runner PID;
// catalog credentials never enter argv, logs, guest RAM or the KVM process.
func (o *Orchestrator) preparedCatalogs(ctx context.Context, runID string) ([]configsock.PreparedCatalog, error) {
	if len(o.cfg.ImageTemplateCatalogs) > 64 {
		return nil, fmt.Errorf("at most 64 staged catalogs per generic slot")
	}
	var result []configsock.PreparedCatalog
	for _, path := range o.cfg.ImageTemplateCatalogs {
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("catalog path must be absolute")
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
		f.Close()
		if err != nil {
			return nil, err
		}
		if len(raw) > 1<<20 {
			return nil, fmt.Errorf("catalog too large")
		}
		var c struct {
			StagingCredentialFile string `json:"staging_credential_file"`
			Identity              struct {
				Credential string `json:"credential_fingerprint"`
			} `json:"identity"`
			ApplicationStarted *bool `json:"application_started"`
			Template           struct {
				ID string `json:"templateID"`
			} `json:"template"`
		}
		if err = json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		t, err := types.ParseTemplateID(c.Template.ID)
		if err != nil || t.Kind != types.KindSnp || c.ApplicationStarted == nil || *c.ApplicationStarted {
			return nil, fmt.Errorf("invalid idle snapshot catalog")
		}
		// Older catalogs remain valid for request-time restore until their
		// preparation tool records a private credential-file reference.
		if c.StagingCredentialFile == "" {
			continue
		}
		if !filepath.IsAbs(c.StagingCredentialFile) {
			return nil, fmt.Errorf("staging credential reference must be absolute")
		}
		cf, err := os.Open(c.StagingCredentialFile)
		if err != nil {
			return nil, err
		}
		cr, err := io.ReadAll(io.LimitReader(cf, (64<<10)+1))
		cf.Close()
		if err != nil {
			return nil, err
		}
		if len(cr) > 64<<10 {
			return nil, fmt.Errorf("staging credential file too large")
		}
		var credential struct {
			Key string `json:"key"`
		}
		if err = json.Unmarshal(cr, &credential); err != nil {
			return nil, err
		}
		clear(cr)
		fp := sha256.Sum256([]byte(credential.Key))
		if hex.EncodeToString(fp[:]) != c.Identity.Credential {
			return nil, fmt.Errorf("catalog credential fingerprint mismatch")
		}
		pair, err := o.resolveAllowed(ctx, credential.Key)
		credential.Key = ""
		if err != nil || pair.APISecret == "" || pair.ManifestKey == "" {
			continue
		} // revoked or expired owners cannot stage new slots
		ownerHash, err := store.APISecretHash(pair.APISecret)
		if err != nil {
			return nil, err
		}
		var stillAllowed bool
		pair, stillAllowed, err = o.st.AllowedKeyPairByAPISecretFingerprint(ctx, ownerHash)
		if err != nil {
			return nil, err
		}
		if !stillAllowed {
			continue
		}
		manifestConfig := o.cfg.ManifestConfig
		if manifestConfig != "" {
			manifestConfig, err = filepath.Abs(manifestConfig)
			if err != nil {
				return nil, err
			}
		}
		runtime, err := filepath.Abs(o.runtimeFileFor(t.Profile))
		if err != nil {
			return nil, err
		}
		result = append(result, configsock.PreparedCatalog{ManifestKey: pair.ManifestKey, Prepare: configsock.ArtifactPrepareSpec{
			RunID: runID, RootSourceKind: string(types.ResumeSourceSnapshot), RootRef: t.Ref, LaunchMode: string(types.LaunchMemory), ManifestConfig: manifestConfig,
			RefLocationParent: o.cfg.Checkpoint.Remote.RefLocationParent, RelativeDir: filepath.Join(nodepath.SandboxBaseRoot(o.cfg.Paths.BaseRoot), runID, "checkpoint"), MaxRefs: maxRequiredArtifactRefs, SnapshotRuntime: "file://" + runtime,
		}})
	}
	return result, nil
}
