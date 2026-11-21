package taskartifact

import (
	"encoding/json"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// WarmPreparedCodecs builds encoding/json's per-process field caches from an
// empty schema. It does not open artifacts, access keys, validate a tenant's
// metadata, or replace the authenticated cache checks performed at assignment.
func WarmPreparedCodecs() error {
	entry := preparedCacheEntry{Result: Result{SourceSandboxConfig: &rtconfig.PortableSandboxConfig{}}}
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	var decoded preparedCacheEntry
	return json.Unmarshal(raw, &decoded)
}
