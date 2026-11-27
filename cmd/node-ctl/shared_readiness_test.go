package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSharedReadinessFollowsPrivateListener(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	notify := filepath.Join(dir, "notify")
	n, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: notify, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	t.Setenv("NOTIFY_SOCKET", notify)
	t.Setenv("KUASAR_EMBEDDED_SANDBOX_SHA256", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	path := filepath.Join(dir, "runs.sock")
	go func() { done <- serveSharedRuns(ctx, path) }()
	n.SetReadDeadline(time.Now().Add(2 * time.Second))
	var raw [64]byte
	count, _, err := n.ReadFromUnix(raw[:])
	if err != nil {
		t.Fatal(err)
	}
	if string(raw[:count]) != "READY=1" {
		t.Fatalf("unexpected notice %q", raw[:count])
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatalf("listener not private at readiness: %v %v", st, err)
	}
	c, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("controller did not stop")
	}
}

func TestSharedReadinessFailureClosesListener(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NOTIFY_SOCKET", filepath.Join(dir, "missing-notify"))
	t.Setenv("KUASAR_EMBEDDED_SANDBOX_SHA256", "")
	path := filepath.Join(dir, "runs.sock")
	if err := serveSharedRuns(context.Background(), path); err == nil {
		t.Fatal("missing notification endpoint accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("failed startup left listener: %v", err)
	}
}
