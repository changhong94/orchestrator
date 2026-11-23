package orch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"time"
)

// The caller already holds the authenticated sandbox lifecycle/source leases
// and has completed the proxy pause barrier. Reuse the same framed ctl protocol
// as sandbox-ctl without a short-lived process. Artifact interpretation and
// tenant keys remain in the runtime. Never retry a transport failure.
func (o *Orchestrator) captureLocalSocket(ctx context.Context, sb *types.Sandbox, request sandboxcfg.CaptureRequest, id string) (types.ResumeSource, bool, error) {
	socket := filepath.Join(nodepath.SandboxRunDir(o.cfg.Paths.RunRoot, sb.ID), "ctl.sock")
	st, err := os.Lstat(socket)
	if os.IsNotExist(err) {
		return types.ResumeSource{}, false, nil
	} // legacy executable adapters
	if err != nil {
		return types.ResumeSource{}, true, err
	}
	if st.Mode()&os.ModeSocket == 0 {
		return types.ResumeSource{}, true, fmt.Errorf("capture control endpoint is not a socket")
	}
	if err = o.validateSandboxCleanupPaths(sb); err != nil {
		return types.ResumeSource{}, true, err
	}
	dir := filepath.Join(sb.BaseDir, "checkpoint")
	if err = os.MkdirAll(dir, 0755); err != nil {
		return types.ResumeSource{}, true, err
	}
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return types.ResumeSource{}, true, err
	}
	if err = unix.Close(fd); err != nil {
		return types.ResumeSource{}, true, err
	}
	req := ctl.Request{Type: ctl.TypeCheckpointSnapshotRequest, CheckpointID: id, OutDir: dir, Mode: o.cfg.Checkpoint.Mode, MergeRef: request.SnapshotPolicy.MergeRef, DropCaches: request.SnapshotPolicy.DropCaches}
	start := time.Now()
	resp, err := ctl.Capture(ctx, socket, req)
	// Only an explicit unsupported response permits a non-checkpoint retry;
	// matching the existing CLI exit-78 compatibility contract.
	if errors.Is(err, ctl.ErrCheckpointUnsupported) {
		req.Type = ctl.TypeSnapshotRequest
		req.CheckpointID = ""
		resp, err = ctl.Capture(ctx, socket, req)
	}
	o.log.Info("capture client phase", "sid", sb.ID, "stage", "direct_socket", "duration_ms", float64(time.Since(start))/1e6)
	if err != nil {
		return types.ResumeSource{}, true, err
	}
	report, err := (artifact.PublishResult{Role: artifact.RoleSnapshot, Ref: resp.SnapshotRef, SandboxRef: resp.SandboxRef}).Report()
	if err != nil {
		return types.ResumeSource{}, true, err
	}
	data, err := json.Marshal(report)
	if err != nil {
		return types.ResumeSource{}, true, err
	}
	source, err := decodeCaptureSource(data, types.ResumeSourceSnapshot)
	return source, true, err
}
