//go:build ignore

// Built explicitly by make node-vm-runner. The normal node-runner does not
// import the VM engine and retains its smaller dependency initialization.
package main

import (
	"context"
	"fmt"
	sandboxctl "github.com/kuasar-sandbox/sandboxer/app/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/controllease"
	"github.com/kuasar-sandbox/sandboxer/pkg/instanceenv"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"golang.org/x/sys/unix"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) == 3 && os.Args[1] == "shared-runs" {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		launchSharedPrepared = func(ctx context.Context, executable, spec string, files []*os.File, catalogs []restore.StagedCatalog) (func(), <-chan error, int, int, error) {
			expected := instanceenv.Get(ctx, "KUASAR_EMBEDDED_SANDBOX_SHA256")
			return embeddedLauncherContext(ctx, expected, catalogs)(executable, spec, files)
		}
		launchSharedRun = func(ctx context.Context, path string, argv, env []string, cg, ready *os.File) error {
			expected := instanceenv.Get(ctx, "KUASAR_EMBEDDED_SANDBOX_SHA256")
			if e := verifyExecutable(path, expected, true); e != nil {
				return e
			}
			dc, dr, e := ownedRunCapabilities(cg, ready)
			if e != nil {
				return e
			}
			return sandboxctl.RunShared(ctx, argv[2:], env, dc, dr)
		}
		if err := serveSharedRuns(ctx, os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if len(os.Args) == 3 && os.Args[1] == "shared-controller" {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		err := controllease.Serve(ctx, os.Args[2], func(ctx context.Context, req controllease.Request, files []*os.File) error {
			ctx, err := instanceenv.With(ctx, req.Env)
			if err != nil {
				return err
			}
			ctx = instanceenv.WithShared(ctx)
			var stages *os.File
			if len(files) == 5 {
				stages = files[4]
			}
			return sandboxctl.RunPrepared(ctx, req.Spec, sandboxctl.PreparedFiles{Input: files[0], PreparedReady: files[1], Cgroup: files[2], RunReady: files[3], Stages: stages})
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if digest := os.Getenv("KUASAR_EMBEDDED_SANDBOX_SHA256"); digest != "" {
		launchPreparedEmbedded = embeddedLauncher(digest)
	}
	runLightweightCLI()
}

// Explicit deployment opt-in binds the linked controller to the approved CLI
// binary. Hashing is paid during slot preparation, before eligibility.
func embeddedLauncher(expected string) func(string, string, []*os.File) (func(), <-chan error, int, int, error) {
	return embeddedLauncherContext(context.Background(), expected)
}
func embeddedLauncherContext(parent context.Context, expected string, catalogs ...[]restore.StagedCatalog) func(string, string, []*os.File) (func(), <-chan error, int, int, error) {
	return func(executable, spec string, files []*os.File) (func(), <-chan error, int, int, error) {
		if err := verifyExecutable(executable, expected, instanceenv.Shared(parent)); err != nil {
			return nil, nil, 0, 0, err
		}
		owned := make([]*os.File, len(files))
		closeOwned := func() {
			for _, f := range owned {
				if f != nil {
					f.Close()
				}
			}
		}
		for i, f := range files {
			if f == nil {
				continue
			}
			fd, err := unix.FcntlInt(f.Fd(), unix.F_DUPFD_CLOEXEC, 3)
			if err != nil {
				closeOwned()
				return nil, nil, 0, 0, err
			}
			owned[i] = os.NewFile(uintptr(fd), "embedded-"+f.Name())
		}
		cg, ready := int(owned[2].Fd()), int(owned[3].Fd())
		ctx, cancel := context.WithCancel(parent)
		done := make(chan error, 1)
		go func() {
			defer closeOwned()
			var values []restore.StagedCatalog
			if len(catalogs) > 0 {
				values = catalogs[0]
			}
			done <- sandboxctl.RunPrepared(ctx, spec, sandboxctl.PreparedFiles{Catalogs: values, Input: owned[0], PreparedReady: owned[1], Cgroup: owned[2], RunReady: owned[3], Stages: owned[4]})
		}()
		return cancel, done, cg, ready, nil
	}
}
