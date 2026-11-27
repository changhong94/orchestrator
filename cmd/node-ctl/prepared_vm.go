package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/sandboxer/pkg/instanceenv"
	"github.com/kuasar-sandbox/sandboxer/pkg/memtrace"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// The lightweight entry point installs this opt-in transport. The normal
// node-ctl and unprepared/native-fork runners retain the subprocess transport.
var launchPreparedEmbedded func(string, string, []*os.File) (func(), <-chan error, int, int, error)

type preparedSandboxChild struct {
	sourceHandoff     chan *restore.PreparedSources
	executable        string
	cgroupFD, readyFD int
	cancel            func()

	shared       bool
	sharedCgroup *os.File
	materials    []*taskartifact.SlotMaterial
	cmd          *exec.Cmd
	input        *os.File
	readiness    *os.File
	done         chan struct{}
	err          error
}

func (p *preparedSandboxChild) Close() {
	_ = p.input.Close()
	_ = p.readiness.Close()
	select {
	case <-p.done:
		return
	default:
	}
	p.signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		if p.shared {
			f, e := os.OpenFile(fmt.Sprintf("/proc/self/fd/%d/cgroup.kill", p.sharedCgroup.Fd()), os.O_WRONLY, 0)
			if e == nil {
				_, _ = f.WriteString("1")
				f.Close()
			}
		} else if p.cancel != nil {
			// One VM per process. A stuck embedded runtime must never be
			// abandoned while the holder reports a completed teardown.
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		} else {
			_ = p.cmd.Process.Kill()
		}
		<-p.done
	}
}
func (p *preparedSandboxChild) signal(sig os.Signal) {
	if p.cancel != nil {
		p.cancel()
	} else {
		_ = p.cmd.Process.Signal(sig)
	}
}

func startPreparedSandbox(socket, runID string, cgroup *os.File) (_ *preparedSandboxChild, retErr error) {
	return startPreparedSandboxContext(context.Background(), socket, runID, cgroup)
}
func startPreparedSandboxContext(parent context.Context, socket, runID string, cgroup *os.File) (_ *preparedSandboxChild, retErr error) {
	traceLast := time.Now()
	trace := func(stage string) {
		if os.Getenv("KUASAR_POOL_TRACE") == "1" {
			now := time.Now()
			fmt.Fprintf(os.Stderr, "POOL_STAGE kind=runner run=%s stage=%s us=%d end_ns=%d\n", runID, stage, now.Sub(traceLast).Microseconds(), now.UnixNano())
			traceLast = now
		}
	}

	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	spec, err := configsock.FetchPreparedRuntime(ctx, socket, runID)
	if err != nil {
		return nil, err
	}
	if len(spec.Spec) == 0 {
		return nil, nil
	}
	if spec.Exec == "" {
		return nil, errors.New("prepared VM executable missing")
	}
	trace("lease_rpc")
	memtrace.Mark("runtime_spec_fetched")
	// Initialize only host-side serialization schemas while the generic slot
	// is being prepared. No image, tenant credential or guest command is read.
	if err := warmPreparedLaunchCodecs(); err != nil {
		return nil, fmt.Errorf("prepare launch codecs: %w", err)
	}
	trace("codecs")
	memtrace.Mark("codecs_prepared")
	var stages *os.File
	var values []restore.StagedCatalog
	var materials []*taskartifact.SlotMaterial
	if instanceenv.Shared(parent) {
		values, materials, err = stageCatalogValues(ctx, spec.Catalogs, runID)
	} else {
		stages, materials, err = stageCatalogFile(ctx, spec.Catalogs)
	}
	if err != nil {
		return nil, err
	}
	if stages != nil {
		defer stages.Close()
	}
	memtrace.Mark("catalog_descriptors_prepared")
	// All descriptors below are inherited once, at the sole child creation.
	// The ready pipe is bridged to the task's socket only after durable assignment.
	lr, lw, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer lr.Close()
	pr, pw, err := os.Pipe()
	if err != nil {
		lw.Close()
		return nil, err
	}
	defer pr.Close()
	defer pw.Close()
	rr, rw, err := os.Pipe()
	if err != nil {
		lw.Close()
		return nil, err
	}
	defer rw.Close()
	trace("stage_catalogs")
	specDir := filepath.Dir(socket)
	file, err := os.CreateTemp(specDir, "prepared-"+runID+"-*.json")
	if err != nil {
		lw.Close()
		rr.Close()
		return nil, err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(spec.Spec); err != nil {
		file.Close()
		lw.Close()
		rr.Close()
		return nil, err
	}
	if err = file.Close(); err != nil {
		lw.Close()
		rr.Close()
		return nil, err
	}
	cmd := exec.Command(spec.Exec, "prepare-run", "--spec", file.Name(), "--launch-fd=3", "--prepared-fd=4", "--cgroup-path=fd=5", "--run-ready-fd=6")
	cmd.ExtraFiles = []*os.File{lr, pw, cgroup, rw}
	if stages != nil {
		cmd.Args = append(cmd.Args, "--stage-fd=7")
		cmd.ExtraFiles = append(cmd.ExtraFiles, stages)
	}
	cmd.Env = taskEnvFrom(instanceenv.Environ(parent), nil)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	p := &preparedSandboxChild{materials: materials, cmd: cmd, executable: cmd.Path, cgroupFD: 5, readyFD: 6, input: lw, readiness: rr, done: make(chan struct{})}
	trace("spec_file")
	if instanceenv.Shared(parent) {
		if launchSharedPrepared == nil {
			return nil, fmt.Errorf("shared prepared runtime unavailable")
		}
		var result <-chan error
		p.shared = true
		p.sharedCgroup = cgroup
		p.sourceHandoff = make(chan *restore.PreparedSources, 1)
		parent = restore.WithSourceHandoff(parent, p.sourceHandoff)
		p.cancel, result, p.cgroupFD, p.readyFD, err = launchSharedPrepared(parent, spec.Exec, file.Name(), []*os.File{lr, pw, cgroup, rw, stages}, values)
		if err != nil {
			lw.Close()
			rr.Close()
			return nil, err
		}
		go func() { p.err = <-result; close(p.done) }()
	} else if launchPreparedEmbedded != nil {
		var result <-chan error
		p.cancel, result, p.cgroupFD, p.readyFD, err = launchPreparedEmbedded(spec.Exec, file.Name(), []*os.File{lr, pw, cgroup, rw, stages})
		if err != nil {
			lw.Close()
			rr.Close()
			return nil, err
		}
		go func() { p.err = <-result; close(p.done) }()
	} else {
		if err = cmd.Start(); err != nil {
			lw.Close()
			rr.Close()
			return nil, err
		}
		go func() { p.err = cmd.Wait(); close(p.done) }()
	}
	defer func() {
		if retErr != nil {
			p.Close()
		}
	}()
	lr.Close()
	pw.Close()
	rw.Close()
	trace("launch_handoff")
	stop := context.AfterFunc(ctx, func() { pr.Close() })
	defer stop()
	raw, err := io.ReadAll(io.LimitReader(pr, 64))
	if err != nil {
		return nil, err
	}
	if string(raw) != "runtime_ready\n" {
		return nil, fmt.Errorf("prepared VM readiness failed: %q", raw)
	}
	select {
	case <-p.done:
		return nil, fmt.Errorf("prepared VM exited: %v", p.err)
	default:
	}
	// Catalog decoding and helper transport allocate temporary buffers. Retire
	// them before advertising the idle slot, not on its eventual claim path.
	if !instanceenv.Shared(parent) {
		debug.FreeOSMemory()
	}
	trace("runtime_ready")
	memtrace.Mark("holder_ready")
	slog.Info("prepared VM ready", "run_id", runID)
	return p, nil
}
func (p *preparedSandboxChild) launchAndReport(socket, sid, runID, path string, argv, env []string, ready *os.File, identity runnerMetricsIdentity, stopSignals <-chan os.Signal, log *slog.Logger) error {
	if path != p.executable || len(argv) < 2 || argv[1] != "run" {
		return errors.New("prepared runner executable changed")
	}
	for _, arg := range argv[2:] {
		for _, owned := range []string{"--ready-fd", "--cgroup-path", "--cgroup-adopt"} {
			if arg == owned || strings.HasPrefix(arg, owned+"=") {
				return errors.New("prepared runner descriptor override")
			}
		}
	}
	markStage, flushStages := traceLaunchStages("prepared_handoff")
	defer flushStages()
	metrics := newRunnerMetrics(identity, log)
	if metrics != nil {
		metrics.captureUsageBaseline(context.Background())
	}
	markStage("usage_baseline")
	bridgeDone := make(chan struct{})
	go func() { _, _ = io.Copy(ready, p.readiness); _ = ready.Close(); close(bridgeDone) }()
	args := append(append([]string(nil), argv[2:]...), fmt.Sprintf("--cgroup-path=fd=%d", p.cgroupFD), fmt.Sprintf("--ready-fd=%d", p.readyFD))
	err := json.NewEncoder(p.input).Encode(map[string]any{"args": args, "env": env})
	markStage("pipe_encode_write")
	flushStages()
	closeErr := p.input.Close()
	err = errors.Join(err, closeErr)
	if err != nil {
		p.Close()
		<-bridgeDone
		result := sandboxExecutionResult(sid, runID, types.SandboxResultStart, err)
		return sandboxRunReportedError{err: errors.Join(err, postSandboxResultWithRetry(socket, sid, runID, result))}
	}
	// Forward stop throughout the runtime, not only at the handoff instant.
	go func() {
		select {
		case sig := <-stopSignals:
			p.signal(sig)
		case <-p.done:
		}
	}()
	log.Info("prepared VM workload claimed", "sandbox_id", sid, "run_id", runID)
	err = finishSandboxChildAndReport(func() error { <-p.done; return p.err }, socket, sid, runID, metrics)
	_ = p.readiness.Close()
	<-bridgeDone
	return err
}
