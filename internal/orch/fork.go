package orch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// ForkSandbox captures a running source without consuming it, publishes a
// self-contained artifact graph, then enters ordinary Create admission. Parent
// and child share neither stable identity, run ownership nor service secrets.
// The response has the same asynchronous starting semantics as POST /sandboxes.
func (o *Orchestrator) ForkSandbox(ctx context.Context, key, id string, timeout int) (*types.Sandbox, error) {
	if o.cfg.Sandbox.PrivateRAMFork {
		return o.forkPrivate(ctx, key, id, timeout)
	}
	return o.withForkTemplate(ctx, key, id, func(ctx context.Context, template string, metadata map[string]string) (*types.Sandbox, error) {
		return o.Create(ctx, api.CreateReq{APIKey: key, TemplateID: template, TimeoutSec: timeout, Metadata: metadata})
	})
}

// ForkSandboxes captures once for the entire SDK batch. Admission and child
// launch failures are individual results; a capture/publication error fails the
// request before any child is created. Ordinary launch owners run concurrently.
func (o *Orchestrator) ForkSandboxes(ctx context.Context, key, id string, timeout, count int) ([]api.ForkItem, error) {
	if count < 1 || count > 20 {
		return nil, fmt.Errorf("%w: fork count must be 1..20", api.ErrBadRequest)
	}
	items := make([]api.ForkItem, count)
	if o.cfg.Sandbox.PrivateRAMFork {
		for i := range items {
			items[i].Sandbox, items[i].Err = o.forkPrivate(ctx, key, id, timeout)
		}
		return items, nil
	}
	_, err := o.withForkTemplate(ctx, key, id, func(ctx context.Context, template string, metadata map[string]string) (*types.Sandbox, error) {
		for i := range items {
			items[i].Sandbox, items[i].Err = o.Create(ctx, api.CreateReq{APIKey: key, TemplateID: template, TimeoutSec: timeout, Metadata: cloneStringMap(metadata)})
		}
		for i := range items {
			if items[i].Err != nil {
				continue
			}
			ready, err := o.waitLaunchState(ctx, items[i].Sandbox.ID)
			if err != nil {
				items[i].Err = err
				items[i].Sandbox = nil
				continue
			}
			if ready == nil {
				items[i].Err = fmt.Errorf("child launch failed")
				items[i].Sandbox = nil
				continue
			}
			items[i].Sandbox = ready
		}
		return nil, nil
	})
	return items, err
}

func (o *Orchestrator) withForkTemplate(ctx context.Context, key, id string, consume func(context.Context, string, map[string]string) (*types.Sandbox, error), timings ...func(float64)) (*types.Sandbox, error) {
	if key == "" {
		return nil, api.ErrNotAllowed
	}
	opctx, finish, err := o.beginPauseOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	opctx, unlock, err := o.lockLifecycleMutation(opctx, id)
	if err != nil {
		return nil, err
	}
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	source, err := o.st.Get(opctx, id)
	if err != nil {
		return nil, err
	}
	if !ownsSandbox(source, key) {
		return nil, api.ErrNotFound
	}
	if source.State != types.StateRunning {
		return nil, fmt.Errorf("%w: fork source must be running", api.ErrBadRequest)
	}
	parentTemplate, err := types.ParseTemplateID(source.TemplateID)
	if err != nil {
		return nil, err
	}
	captureID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	captureDir := filepath.Join(o.cfg.Paths.BaseRoot, "fork-captures", captureID.String())
	if err := os.MkdirAll(captureDir, 0700); err != nil {
		return nil, err
	}
	request := sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}
	request.SnapshotPolicy, err = o.resolveSnapshotPolicy(source.Metadata, request.SnapshotPolicy)
	if err != nil {
		return nil, err
	}
	if err := o.preparePause(opctx, source, request); err != nil {
		return nil, err
	}
	// Parent route rollback follows the runtime's resume acknowledgement. Keep
	// the lifecycle lock through publication so source cleanup cannot race reads.
	defer func() {
		if locked {
			o.rollbackPause(opctx, source)
		}
	}()
	args := []string{"snapshot", "--json", "--resume", "--path-id", source.ID, "--output", captureDir, "--mode", o.cfg.Checkpoint.Mode, "--run-root", nodepath.SandboxRunRoot(o.cfg.Paths.RunRoot)}
	args = appendSnapshotPolicyArgs(args, request.SnapshotPolicy)
	captureStarted := time.Now()
	captured, err := o.runForkCaptureTool(opctx, source, args, func() {
		o.rollbackPause(opctx, source)
		o.log.Info("fork phase", "parent", id, "phase", "parent_route_resumed", "duration", time.Since(captureStarted))
	}, timings...)
	o.rollbackPause(opctx, source)
	o.log.Info("fork phase", "parent", id, "phase", "capture_and_parent_resume", "duration", time.Since(captureStarted), "success", err == nil)
	if err != nil {
		return nil, err
	}
	graph, err := decodeCaptureSource(captured, types.ResumeSourceSnapshot)
	if err != nil {
		return nil, err
	}
	ref, err := manifest.ParseRef(graph.Ref)
	if err != nil {
		return nil, err
	}
	root, err := (rtconfig.RefLocations{}).ResolveFile(ref, captureDir)
	if err != nil {
		return nil, err
	}
	ref.Path = root
	args = []string{"fork-publish", "--manifest-config", o.cfg.ManifestConfig, "--ref-location-parent", o.cfg.Checkpoint.Remote.RefLocationParent}
	if o.cfg.Checkpoint.SnapshotTarget != "manifest" && o.cfg.Checkpoint.Remote.RefLocationParent != "" {
		location := reflocation.PublicationName(captureID.String())
		uri, err := o.cfg.Checkpoint.RefLocationURI(location)
		if err != nil {
			return nil, err
		}
		args = append(args, "--to-ref-location", location+"="+uri)
	}
	args = append(args, ref.String())
	publishStarted := time.Now()
	published, err := o.runForkArtifactTool(opctx, source, args)
	o.log.Info("fork phase", "parent", id, "phase", "publication_after_parent_resume", "duration", time.Since(publishStarted), "success", err == nil)
	if err != nil {
		return nil, err
	}
	report, err := artifact.DecodePublishReport(published, artifactRole(types.ResumeSourceSnapshot))
	if err != nil {
		return nil, err
	}
	metadata := cloneStringMap(source.Metadata)
	if metadata == nil {
		metadata = map[string]string{}
	}
	// Memory restore already contains the workload and filesystem. Do not replay
	// cold-create mutations or reuse the parent's explicit management identity.
	for _, name := range []string{sandboxcfg.NsIdentity, sandboxcfg.NsLaunch,
		sandboxcfg.NsInit, sandboxcfg.NsMounts, sandboxcfg.NsFiles,
		sandboxcfg.NsCredentials, sandboxcfg.NsMMDS, nativeForkParentKey} {
		delete(metadata, name)
	}
	restore := map[string]any{}
	if raw := metadata[sandboxcfg.NsRestore]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &restore); err != nil {
			return nil, err
		}
	}
	restore["fork_identity"] = true
	encoded, _ := json.Marshal(restore)
	metadata[sandboxcfg.NsRestore] = string(encoded)
	template := types.TemplateID{Profile: parentTemplate.Profile, Kind: types.KindSnp, Ref: report.SnapshotRef}
	unlock()
	locked = false
	// Native resume capture does not rebase the live source onto its output.
	// Publication has committed the complete portable closure (with copied or
	// independently linked local carriers). The unpublished staging graph is
	// now redundant; keep failed publications for recovery, never remove inputs
	// from the source's own BaseDir or named publication namespace.
	if cleanupErr := os.RemoveAll(captureDir); cleanupErr != nil {
		o.log.Warn("fork capture staging cleanup", "capture_id", captureID.String(), "err", cleanupErr)
	}
	return consume(opctx, template.String(), metadata)
}

func (o *Orchestrator) runForkArtifactTool(ctx context.Context, sb *types.Sandbox, args []string) ([]byte, error) {
	tool := o.executables.SandboxCtl()
	if len(args) > 0 && args[0] == "fork-publish" {
		tool = o.executables.OrchestratorCtl()
	}
	cmd := exec.CommandContext(ctx, tool, args...)
	inheritSourceOperation(ctx, cmd)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+sb.ManifestKey)
	var out, diagnostics bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &diagnostics
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("fork %s: %w: %s", args[0], err, diagnostics.String())
	}
	return out.Bytes(), nil
}
