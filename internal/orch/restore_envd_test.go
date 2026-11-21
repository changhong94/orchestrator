package orch

import (
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"testing"
)

func TestInlineRestoreEnvdEligibility(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	sb := &types.Sandbox{LaunchMode: types.LaunchMemory, Metadata: map[string]string{}}
	tmpl := types.TemplateID{Kind: types.KindSnp, Profile: types.ProfileE2B}
	o.cfg.Sandbox.RestoreEnvdInit = false
	if o.inlineRestoreEnvd(sb, tmpl) {
		t.Fatal("enabled by default")
	}
	o.cfg.Sandbox.RestoreEnvdInit = true
	o.cfg.MMDS.Enabled = false
	if !o.inlineRestoreEnvd(sb, tmpl) {
		t.Fatal("eligible restore rejected")
	}
	o.cfg.MMDS.Enabled = true
	if o.inlineRestoreEnvd(sb, tmpl) {
		t.Fatal("MMDS token initialization bypassed")
	}
	o.cfg.MMDS.Enabled = false
	sb.Metadata[nativeForkParentKey] = "parent"
	if o.inlineRestoreEnvd(sb, tmpl) {
		t.Fatal("live fork semantics changed")
	}
}
