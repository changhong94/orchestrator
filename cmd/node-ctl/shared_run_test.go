package main

import (
	"context"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/sandboxer/pkg/instanceenv"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSharedRunReadinessEOFDoesNotWaitForRuntimeExit(t *testing.T) {
	cg, e := os.CreateTemp(t.TempDir(), "cg")
	if e != nil {
		t.Fatal(e)
	}
	defer cg.Close()
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	dc, dr, e := ownedRunCapabilities(cg, w)
	if e != nil {
		t.Fatal(e)
	}
	defer dc.Close()
	if _, e = dr.WriteString("ready\n"); e != nil {
		t.Fatal(e)
	}
	dr.Close()
	done := make(chan error, 1)
	go func() {
		b, e := io.ReadAll(r)
		if string(b) != "ready\n" && e == nil {
			e = io.ErrUnexpectedEOF
		}
		done <- e
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("readiness EOF blocked on original writer")
	}
	if _, e = cg.Stat(); e != nil {
		t.Fatal("lost caller cgroup ownership", e)
	}
}

func TestSharedFinalLaunchUsesInstanceDefaults(t *testing.T) {
	t.Setenv("SANDBOX_RUN_ROOT", "/wrong-host-default")
	ctx, e := instanceenv.With(context.Background(), []string{"SANDBOX_RUN_ROOT=/instance", "TASK_RUN_ID=private"})
	if e != nil {
		t.Fatal(e)
	}
	cg, e := os.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer cg.Close()
	called := false
	e = launchTaskWith(ctx, func() {}, "/test", "sid", "run", nil, cg, taskLaunchOps{
		fetchBootstrap: func(context.Context, string, string, string) (*configsock.SandboxTaskSpec, error) {
			return &configsock.SandboxTaskSpec{SandboxID: "sid", RunID: "run", Env: map[string]string{"MANIFEST_KEY": "authoritative"}, Final: &configsock.LaunchSpec{Exec: "/sandbox-ctl", Args: []string{"run"}}}, nil
		},
		setenv: func(string, string) error { return nil },
		startChild: func(_ string, _ []string, env []string, _, _ *os.File) error {
			called = true
			all := strings.Join(env, "\n")
			if !strings.Contains(all, "SANDBOX_RUN_ROOT=/instance") || strings.Contains(all, "wrong-host") || strings.Contains(all, "TASK_RUN_ID") || !strings.Contains(all, "MANIFEST_KEY=authoritative") {
				t.Fatalf("wrong scoped launch environment: %q", env)
			}
			return nil
		},
	})
	if e != nil || !called {
		t.Fatalf("launch: %v", e)
	}
}
