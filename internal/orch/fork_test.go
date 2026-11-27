package orch

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestForkPublishesIndependentGraphBeforeCreate(t *testing.T) {
	for _, target := range []string{"location", "manifest"} {
		t.Run(target, func(t *testing.T) { testForkPublicationTarget(t, target) })
	}
}
func testForkPublicationTarget(t *testing.T, target string) {
	cfg := checkpointOrchestratorConfig(t, "local")
	cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31"
	cfg.Checkpoint.SnapshotTarget = target
	cfg.Checkpoint.Remote.RefLocationParent = "file://" + t.TempDir()
	o, parent, key, _, _, argsFile := newCheckpointPauseFixture(t, cfg, "")
	_ = createRequestFixture(t, o, "1")
	parent.Metadata = map[string]string{nativeForkParentKey: "original-host-owned-parent", sandboxcfg.NsIdentity: `{"id":"do-not-inherit"}`, "app": "test", sandboxcfg.NsLaunch: `{"exec":"/do-not-repeat"}`, sandboxcfg.NsFiles: `[{"path":"/do-not-repeat"}]`, sandboxcfg.NsCredentials: `{"service_secret":"do-not-inherit"}`}
	if err := o.st.Put(context.Background(), parent); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
printf '%s\n' "$@" >> "$CHECKPOINT_ARGS_FILE"
case "$1" in
snapshot) printf '%s\n' '{"snapshotRef":"file://captured.snapshot@digest:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sandboxRef":"file://captured.sandbox@digest:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","removedRefs":[]}' ;;
fork-publish) printf '%s\n' '{"snapshotRef":"manifest://cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","sandboxRef":"manifest://dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","removedRefs":[]}' ;;
*) exit 3 ;;
esac
`
	if err := os.WriteFile(filepath.Join(filepath.Dir(argsFile), config.BinSandboxCtl), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	o.executables.NodeCtl = filepath.Join(filepath.Dir(argsFile), "node-ctl")
	if err := os.WriteFile(o.executables.NodeCtl, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	child, err := o.ForkSandbox(context.Background(), key, parent.ID, 60)
	if err != nil {
		t.Fatal(err)
	}
	if child.ID == parent.ID || child.StableID() == parent.StableID() || child.ID == "do-not-inherit" {
		t.Fatal("inherited management identity")
	}
	if child.APISecret != parent.APISecret || child.ServiceSecret == parent.ServiceSecret {
		t.Fatal("tenant or sandbox secret scope incorrect")
	}
	for _, name := range []string{sandboxcfg.NsIdentity, sandboxcfg.NsLaunch, sandboxcfg.NsFiles, sandboxcfg.NsCredentials} {
		if _, exists := child.Metadata[name]; exists {
			t.Fatalf("replayed %s", name)
		}
	}
	if child.Metadata["app"] != "test" {
		t.Fatal("lost application metadata")
	}
	var restore sandboxcfg.RestoreSpec
	if err := json.Unmarshal([]byte(child.Metadata[sandboxcfg.NsRestore]), &restore); err != nil || !restore.ForkIdentity {
		t.Fatalf("missing fork policy: %v", err)
	}
	saved, err := o.st.Get(context.Background(), parent.ID)
	if err != nil || saved.State != types.StateRunning || saved.RunID != parent.RunID {
		t.Fatalf("parent changed: %+v %v", saved, err)
	}
	raw, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "--ref-location-parent\n"+cfg.Checkpoint.Remote.RefLocationParent+"\n") {
		t.Fatal("input namespace lost")
	}
	if strings.Contains(string(raw), "--to-ref-location\n") != (target == "location") {
		t.Fatal("wrong snapshot destination")
	}
	if !strings.Contains(string(raw), "--resume\n") || !strings.Contains(string(raw), "fork-publish\n") {
		t.Fatal("capture consumed parent or skipped publish")
	}
}

func TestForkRejectsWrongOwnerBeforeCapture(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, "local")
	cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31"
	o, parent, _, _, _, argsFile := newCheckpointPauseFixture(t, cfg, "")
	if _, err := o.ForkSandbox(context.Background(), "other", parent.ID, 30); !errors.Is(err, api.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := os.Stat(argsFile); !os.IsNotExist(err) {
		t.Fatal("unauthorized capture side effect")
	}
}

func TestForkIdentityFlagOnlyOnInitialSnapshot(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, "local")
	cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31"
	o, sb, _, _, _, _ := newCheckpointPauseFixture(t, cfg, "")
	spec := sandboxcfg.SandboxSpec{} // isolation is mandatory even without opt-in metadata
	tmpl := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSnp, Ref: "manifest://" + strings.Repeat("a", 64)}
	sb.LaunchMode = types.LaunchMemory
	initial := o.sandboxFinalLaunchSpec(sb, tmpl, spec, nil)
	if !strings.Contains(strings.Join(initial.Args, " "), "--fork-identity") {
		t.Fatal("initial child missing identity reset")
	}
	sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: tmpl.Ref, SandboxRef: "manifest://" + strings.Repeat("b", 64)}
	resumed := o.sandboxFinalLaunchSpec(sb, tmpl, spec, nil)
	if strings.Contains(strings.Join(resumed.Args, " "), "--fork-identity") {
		t.Fatal("same-sandbox resume repeated fork reset")
	}
}
