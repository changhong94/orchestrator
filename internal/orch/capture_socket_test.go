package orch

import (
	"context"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCaptureSocketCommittedPairAndNoTransportRetry(t *testing.T) {
	for _, mode := range []string{"ok", "unsupported", "torn", "transport"} {
		t.Run(mode, func(t *testing.T) {
			cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
			root := shortOrchestratorTestDir(t)
			cfg.Paths.RunRoot = filepath.Join(root, "run")
			cfg.Paths.BaseRoot = filepath.Join(root, "base")
			o, sb, _, _, _, args := newCheckpointPauseFixture(t, cfg, "")
			if e := os.MkdirAll(sb.RunDir, 0700); e != nil {
				t.Fatal(e)
			}
			ln, e := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(sb.RunDir, "ctl.sock"), Net: "unix"})
			if e != nil {
				t.Fatal(e)
			}
			defer ln.Close()
			ln.SetDeadline(time.Now().Add(5 * time.Second))
			done := make(chan error, 1)
			go func() {
				count := 1
				if mode == "unsupported" {
					count = 2
				}
				for i := 0; i < count; i++ {
					c, e := ln.Accept()
					if e != nil {
						done <- e
						return
					}
					var req ctl.Request
					e = ctl.ReadMessage(c, &req)
					if e != nil {
						c.Close()
						done <- e
						return
					}
					expected := ctl.TypeCheckpointSnapshotRequest
					if i == 1 {
						expected = ctl.TypeSnapshotRequest
					}
					if req.Type != expected || req.ResumeAfter || req.Upload || req.Mode != config.CheckpointLocal {
						t.Errorf("request=%+v", req)
					}
					if mode == "transport" {
						c.Close()
						break
					}
					resp := ctl.Response{Type: ctl.TypeSnapshotDone, SnapshotRef: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
					if mode == "unsupported" && i == 0 {
						resp = ctl.Response{Type: ctl.TypeError, Code: ctl.CheckpointUnsupportedCode, Msg: "unsupported"}
					}
					if mode == "torn" {
						resp.SandboxRef = ""
					}
					e = ctl.WriteMessage(c, &resp)
					c.Close()
					if e != nil {
						done <- e
						return
					}
				}
				done <- nil
			}()
			src, used, e := o.captureLocalSocket(context.Background(), sb, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, "01942000-0000-7000-8000-000000000001")
			if se := <-done; se != nil {
				t.Fatal(se)
			}
			if !used {
				t.Fatal("did not use trusted socket")
			}
			if mode == "torn" || mode == "transport" {
				if e == nil {
					t.Fatal("accepted incomplete capture")
				}
			} else if e != nil || src.Ref != checkpointSnapshotRef || src.SandboxRef != checkpointSandboxRef {
				t.Fatalf("%+v %v", src, e)
			}
			if _, e := os.Stat(args); !os.IsNotExist(e) {
				t.Fatal("unexpected executable invocation")
			}
		})
	}
}

func TestCaptureSocketRejectsSymlinkEndpointOrOutput(t *testing.T) {
	for _, endpoint := range []bool{true, false} {
		cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
		root := shortOrchestratorTestDir(t)
		cfg.Paths.RunRoot = filepath.Join(root, "run")
		cfg.Paths.BaseRoot = filepath.Join(root, "base")
		o, sb, _, _, _, _ := newCheckpointPauseFixture(t, cfg, "")
		if e := os.MkdirAll(sb.RunDir, 0700); e != nil {
			t.Fatal(e)
		}
		socket := filepath.Join(sb.RunDir, "ctl.sock")
		if endpoint {
			if e := os.Symlink(filepath.Join(root, "other.sock"), socket); e != nil {
				t.Fatal(e)
			}
		} else {
			ln, e := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
			if e != nil {
				t.Fatal(e)
			}
			defer ln.Close()
			if e = os.MkdirAll(sb.BaseDir, 0700); e != nil {
				t.Fatal(e)
			}
			if e = os.Symlink(t.TempDir(), filepath.Join(sb.BaseDir, "checkpoint")); e != nil {
				t.Fatal(e)
			}
		}
		_, used, e := o.captureLocalSocket(context.Background(), sb, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, "01942000-0000-7000-8000-000000000001")
		if !used || e == nil {
			t.Fatal("unsafe path was accepted")
		}
	}
}
