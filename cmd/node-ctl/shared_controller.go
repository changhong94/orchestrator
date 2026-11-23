package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/kuasar-sandbox/sandboxer/pkg/controllease"
	"io"
	"os"
)

// The per-run proxy retains config-socket authorization, cgroup ownership and
// durable result reporting. Only VM runtime capabilities cross this socket.
func remotePreparedLauncher(socket string) func(string, string, []*os.File) (func(), <-chan error, int, int, error) {
	return func(executable, spec string, files []*os.File) (func(), <-chan error, int, int, error) {
		expected := os.Getenv("KUASAR_EMBEDDED_SANDBOX_SHA256")
		f, e := os.Open(executable)
		if e != nil {
			return nil, nil, 0, 0, e
		}
		h := sha256.New()
		_, e = io.Copy(h, f)
		f.Close()
		if e != nil || expected == "" || hex.EncodeToString(h.Sum(nil)) != expected {
			return nil, nil, 0, 0, fmt.Errorf("shared sandbox executable digest mismatch")
		}
		return controllease.Start(socket, controllease.Request{Spec: spec, Env: taskEnv(nil)}, files)
	}
}
