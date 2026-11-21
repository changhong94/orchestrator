package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"golang.org/x/sys/unix"
)

// runSandbox is the ExecStart of sandbox-runner@<run-id>.service: it waits until
// the run-id is assigned a sandbox id, fetches that sandbox's LaunchSpec over the
// config-socket, starts sandbox-ctl as its direct child, waits once, and reports
// the bounded child result before exiting.
//
//	node-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --run-id=<rid>
func runSandbox(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("run-sandbox", flag.ExitOnError)
	pidfile := fs.String("pidfile", "", "pidfile to lock+write (TASK_PIDFILE)")
	socket := fs.String("config-socket", "", "config-socket UDS (TASK_CONFIG_SOCKET)")
	runID := fs.String("run-id", "", "run id (TASK_RUN_ID)")
	_ = fs.Parse(args)
	envDefault(pidfile, "TASK_PIDFILE")
	envDefault(socket, "TASK_CONFIG_SOCKET")
	envDefault(runID, "TASK_RUN_ID")
	if *pidfile == "" || *socket == "" || *runID == "" {
		return fmt.Errorf("run-sandbox: --pidfile, --config-socket, and --run-id required")
	}
	var prepared *preparedSandboxChild
	var bootstrap *configsock.SandboxTaskSpec
	return runAssignedSandbox(*pidfile, *socket, *runID, runSandboxOps{
		prepareVM: func(socket, runID string, cgroup *os.File) (*preparedSandboxChild, error) {
			var err error
			prepared, err = startPreparedSandbox(socket, runID, cgroup)
			return prepared, err
		},
		lockPidfile:   lockPidfile,
		prepareCgroup: func() (*os.File, error) { return prepareRunnerCgroup(*runID) },
		openSession:   openRunSessionKeeper,
		waitAssignment: func(ctx context.Context, socket, kind, runID string) (string, error) {
			out, err := configsock.WaitSandboxBootstrap(ctx, socket, runID)
			if err != nil {
				return "", err
			}
			bootstrap = out.Bootstrap
			return out.TaskID, nil
		},
		connectReady: connectReadinessSocket,
		launchTask: func(socket, sid, runID string, ready, cgroup *os.File) error {
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
			defer stop()
			return launchTaskWithBootstrap(ctx, stop, socket, sid, runID, ready, cgroup, log, bootstrap, prepared)
		},
	})
}

type runSessionCloser interface{ Close() error }

func openRunSessionKeeper(ctx context.Context, socket, kind, runID string) (runSessionCloser, error) {
	return configsock.OpenRunSessionKeeper(ctx, socket, kind, runID)
}

type runSandboxOps struct {
	prepareVM      func(string, string, *os.File) (*preparedSandboxChild, error)
	lockPidfile    func(string) error
	prepareCgroup  func() (*os.File, error)
	openSession    func(context.Context, string, string, string) (runSessionCloser, error)
	waitAssignment func(context.Context, string, string, string) (string, error)
	connectReady   func(string) (*os.File, error)
	launchTask     func(string, string, string, *os.File, *os.File) error
}

func runAssignedSandbox(pidfile, socket, runID string, ops runSandboxOps) error {
	if err := ops.lockPidfile(pidfile); err != nil {
		return err
	}
	vmmCgroup, err := ops.prepareCgroup()
	if err != nil {
		return fmt.Errorf("prepare runner cgroup: %w", err)
	}
	defer vmmCgroup.Close()
	if ops.openSession != nil {
		session, err := ops.openSession(context.Background(), socket, "sandbox", runID)
		if err != nil {
			return fmt.Errorf("open run session: %w", err)
		}
		defer session.Close()
	}
	waitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	if ops.prepareVM != nil {
		prepared, err := ops.prepareVM(socket, runID, vmmCgroup)
		if err != nil {
			return err
		}
		if prepared != nil {
			defer prepared.Close()
			go func() {
				select {
				case <-prepared.done:
					cancelWait()
				case <-waitCtx.Done():
				}
			}()
		}
	}
	sid, err := waitSandboxAssignmentWithRetry(waitCtx, socket, runID, ops.waitAssignment)
	if err != nil {
		return fmt.Errorf("wait assignment: %w", err)
	}
	assignedAt := time.Now()
	assignedTID := syscall.Gettid()
	runRoot := filepath.Dir(filepath.Dir(pidfile))
	ready, err := ops.connectReady(configsock.ReadinessSocketPath(runRoot, sid))
	if err != nil {
		return fmt.Errorf("connect readiness socket: %w", err)
	}
	// The connection owns the bridge until child Start consumes it. Any config,
	// chdir, argv, or Start failure returns through this defer and turns into EOF
	// for the orchestrator instead of making it wait for the launch timeout.
	defer ready.Close()
	if os.Getenv("CH_FORK_TIMING") != "" {
		fmt.Fprintf(os.Stderr, "ASSIGNMENT_READY run_id=%s assigned_ns=%d ready_ns=%d pid=%d assigned_tid=%d ready_tid=%d\n", runID, assignedAt.UnixNano(), time.Now().UnixNano(), os.Getpid(), assignedTID, syscall.Gettid())
	}
	return ops.launchTask(socket, sid, runID, ready, vmmCgroup)
}

func waitSandboxAssignmentWithRetry(ctx context.Context, socket, runID string, wait func(context.Context, string, string, string) (string, error)) (string, error) {
	delay := 20 * time.Millisecond
	for {
		sid, err := wait(ctx, socket, "sandbox", runID)
		if err == nil || !configsock.IsRetryableError(err) {
			return sid, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return "", ctx.Err()
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

func connectReadinessSocket(path string) (*os.File, error) {
	// The caller needs an owned file descriptor, not a netpoll connection. Create
	// it close-on-exec atomically instead of registering, duplicating, and closing
	// a temporary net.UnixConn. Child inheritance remains owned by Start.
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("create readiness socket: %w", err)
	}
	if err := unix.Connect(fd, &unix.SockaddrUnix{Name: path}); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("connect readiness socket: %w", err)
	}
	return os.NewFile(uintptr(fd), "readiness-socket"), nil
}
