package orch

import (
	"io"
	"strings"
	"testing"
	"time"
)

func TestForkProgressResumesBeforePublication(t *testing.T) {
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	resumed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := readForkCaptureProgress(r, func() { close(resumed) })
		done <- err
	}()
	if _, err := io.WriteString(w, `{"type":"snapshot_resumed"}`+"\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-resumed:
	case <-time.After(time.Second):
		t.Fatal("route waited for persistence")
	}
	select {
	case <-done:
		t.Fatal("resume was mistaken for durable completion")
	default:
	}
	if _, err := io.WriteString(w, `{"snapshotRef":"published","sandboxRef":"published"}`); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestForkProgressRejectsInvalidStream(t *testing.T) {
	for _, s := range []string{
		`{"type":"snapshot_resumed"}`,
		`{"type":"snapshot_resumed"}{"type":"snapshot_resumed"}{}`,
		`{"type":"unexpected"}{}`,
		`{}{}`,
		`{}{"type":"snapshot_resumed"}`,
	} {
		if _, err := readForkCaptureProgress(strings.NewReader(s), nil); err == nil {
			t.Fatalf("accepted %s", s)
		}
	}
	if _, err := readForkCaptureProgress(strings.NewReader(`{"snapshotRef":"fallback"}`), nil); err != nil {
		t.Fatal(err)
	}
}

func TestForkProgressTimingDoesNotReplaceDurableResult(t *testing.T) {
	var measured float64
	raw, err := readForkCaptureProgress(strings.NewReader(`{"type":"snapshot_resumed"}{"type":"snapshot_timing","guest_pause_ms":12.345}{"snapshotRef":"durable"}`), nil, func(ms float64) { measured = ms })
	if err != nil || measured != 12.345 || !strings.Contains(string(raw), "durable") {
		t.Fatalf("%s %v %v", raw, measured, err)
	}
	for _, invalid := range []string{
		`{"type":"snapshot_timing","guest_pause_ms":-1}{}`,
		`{"type":"snapshot_timing"}{}`,
		`{"type":"snapshot_timing","guest_pause_ms":1}{"type":"snapshot_timing","guest_pause_ms":2}{}`,
		`{"type":"snapshot_timing","guest_pause_ms":1}`,
	} {
		if _, err := readForkCaptureProgress(strings.NewReader(invalid), nil); err == nil {
			t.Fatalf("accepted %s", invalid)
		}
	}
}
