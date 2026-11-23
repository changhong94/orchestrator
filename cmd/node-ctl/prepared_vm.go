package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type preparedSandboxChild struct {
	cmd       *exec.Cmd
	input     *os.File
	readiness *os.File
	done      chan struct{}
	err       error
}

func (p *preparedSandboxChild) Close() {
	_ = p.input.Close()
	_ = p.readiness.Close()
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}
func startPreparedSandbox(socket, runID string, cgroup *os.File) (_ *preparedSandboxChild, retErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	parent := filepath.Dir(socket)
	file, err := os.CreateTemp(parent, "prepared-"+runID+"-*.json")
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
	cmd.Env = taskEnv(nil)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		lw.Close()
		rr.Close()
		return nil, err
	}
	p := &preparedSandboxChild{cmd: cmd, input: lw, readiness: rr, done: make(chan struct{})}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	defer func() {
		if retErr != nil {
			p.Close()
		}
	}()
	lr.Close()
	pw.Close()
	rw.Close()
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
	slog.Info("prepared VM ready", "run_id", runID)
	return p, nil
}
func (p *preparedSandboxChild) launchAndReport(socket, sid, runID, path string, argv, env []string, ready *os.File, identity runnerMetricsIdentity, stopSignals <-chan os.Signal, log *slog.Logger) error {
	if path != p.cmd.Path || len(argv) < 2 || argv[1] != "run" {
		return errors.New("prepared runner executable changed")
	}
	for _, arg := range argv[2:] {
		for _, owned := range []string{"--ready-fd", "--cgroup-path", "--cgroup-adopt"} {
			if arg == owned || strings.HasPrefix(arg, owned+"=") {
				return errors.New("prepared runner descriptor override")
			}
		}
	}
	metrics := newRunnerMetrics(identity, log)
	if metrics != nil {
		metrics.captureUsageBaseline(context.Background())
	}
	bridgeDone := make(chan struct{})
	go func() { _, _ = io.Copy(ready, p.readiness); _ = ready.Close(); close(bridgeDone) }()
	args := append(append([]string(nil), argv[2:]...), "--cgroup-path=fd=5", "--ready-fd=6")
	err := json.NewEncoder(p.input).Encode(map[string]any{"args": args, "env": env})
	closeErr := p.input.Close()
	err = errors.Join(err, closeErr)
	if err != nil {
		p.Close()
		<-bridgeDone
		result := sandboxExecutionResult(sid, runID, types.SandboxResultStart, err)
		return sandboxRunReportedError{err: errors.Join(err, postSandboxResultWithRetry(socket, sid, runID, result))}
	}
	select {
	case sig := <-stopSignals:
		_ = p.cmd.Process.Signal(sig)
	default:
	}
	log.Info("prepared VM workload claimed", "sandbox_id", sid, "run_id", runID)
	err = finishSandboxChildAndReport(func() error { <-p.done; return p.err }, socket, sid, runID, metrics)
	_ = p.readiness.Close()
	<-bridgeDone
	return err
}
