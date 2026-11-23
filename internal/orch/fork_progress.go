package orch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

// Only the trusted runtime's post-reattach acknowledgement releases the proxy
// pause. The source lifecycle lock still covers publication and protects its
// artifact inputs against deletion. Progress is not a durability acknowledgement.
func (o *Orchestrator) runForkCaptureTool(ctx context.Context, sb *types.Sandbox, args []string, resumed func(), timings ...func(float64)) ([]byte, error) {
	cmd := exec.CommandContext(ctx, o.executables.SandboxCtl(), append(args, "--progress")...)
	inheritSourceOperation(ctx, cmd)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+sb.ManifestKey)
	var diagnostics bytes.Buffer
	cmd.Stderr = &diagnostics
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	result, readErr := readForkCaptureProgress(stdout, resumed, timings...)
	if readErr != nil {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait()
	if err := errors.Join(readErr, waitErr); err != nil {
		return nil, fmt.Errorf("fork snapshot: %w: %s", err, diagnostics.String())
	}
	return result, nil
}

func readForkCaptureProgress(r io.Reader, resumed func(), timings ...func(float64)) ([]byte, error) {
	d := json.NewDecoder(r)
	var result []byte
	seenResume := false
	seenTiming := false
	for {
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) && result != nil {
				return result, nil
			}
			return nil, fmt.Errorf("capture stream: %w", err)
		}
		if result != nil {
			return nil, errors.New("capture stream: data after final result")
		}
		var event struct {
			Type         string   `json:"type"`
			GuestPauseMS *float64 `json:"guest_pause_ms"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, err
		}
		if event.Type == ctl.TypeSnapshotResumed {
			if seenResume {
				return nil, errors.New("capture stream: duplicate resume")
			}
			seenResume = true
			if resumed != nil {
				resumed()
			}
			continue
		}
		if event.Type == "snapshot_timing" {
			if seenTiming || event.GuestPauseMS == nil || *event.GuestPauseMS < 0 || math.IsNaN(*event.GuestPauseMS) || math.IsInf(*event.GuestPauseMS, 0) {
				return nil, errors.New("capture stream: invalid timing")
			}
			seenTiming = true
			for _, observe := range timings {
				if observe != nil {
					observe(*event.GuestPauseMS)
				}
			}
			continue
		}
		if event.Type != "" {
			return nil, fmt.Errorf("capture stream: unknown event %q", event.Type)
		}
		// The caller validates the native artifact result after process success.
		// A fallback fully-quiesced capture can legitimately omit progress.
		result = append([]byte(nil), raw...)
	}
}
