package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/instanceenv"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"golang.org/x/sys/unix"
)

func stageArtifactCmd() error {
	var spec configsock.ArtifactPrepareSpec
	if err := json.NewDecoder(io.LimitReader(os.Stdin, 1<<20)).Decode(&spec); err != nil {
		return err
	}
	result, err := taskartifact.Prepare(context.Background(), spec)
	if err != nil {
		return err
	}
	files, err := taskartifact.StageFiles(spec, result)
	if err != nil {
		return err
	}
	material, err := taskartifact.ExportSlotMaterial(context.Background(), spec)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"material": material, "ref": result.PreparedSource.Ref, "sandbox_hint": result.PreparedSource.SandboxRef, "locations": result.RefLocationURIs, "manifest_config": spec.ManifestConfig, "files": files})
}

// stageCatalogValues transfers private per-slot values in the shared host.
// Validation is unchanged; only the redundant wire roundtrip is removed.
func stageCatalogValues(ctx context.Context, catalogs []configsock.PreparedCatalog, runID ...string) ([]restore.StagedCatalog, []*taskartifact.SlotMaterial, error) {
	if len(catalogs) == 0 {
		return nil, nil, nil
	}
	if len(catalogs) > 64 {
		return nil, nil, fmt.Errorf("too many staged catalogs")
	}
	stages := make([]restore.StagedCatalog, 0, len(catalogs))
	var materials []*taskartifact.SlotMaterial
	for index, catalog := range catalogs {
		var spans strings.Builder
		last := time.Now()
		start := last
		trace := func(phase string) {
			if os.Getenv("KUASAR_POOL_TRACE") != "1" {
				return
			}
			now := time.Now()
			fmt.Fprintf(&spans, " %s_ns=%d", phase, now.Sub(last).Nanoseconds())
			last = now
		}
		scoped, err := instanceenv.With(ctx, taskEnvFrom(instanceenv.Environ(ctx), map[string]string{"MANIFEST_KEY": catalog.ManifestKey}))
		if err != nil {
			return nil, nil, err
		}
		result, files, material, err := taskartifact.PrepareSlot(scoped, catalog.Prepare)
		if err != nil {
			return nil, nil, fmt.Errorf("stage artifact: %w", err)
		}
		trace("prepare_once")
		// Keep the original per-catalog admission limit. No bytes are handed
		// across a process boundary or decoded back into another object graph.
		budget := &limitedStageWriter{w: io.Discard, left: 4 << 20}
		if err := json.NewEncoder(budget).Encode(map[string]any{"material": material, "ref": result.PreparedSource.Ref, "sandbox_hint": result.PreparedSource.SandboxRef, "locations": result.RefLocationURIs, "manifest_config": catalog.Prepare.ManifestConfig, "files": files}); err != nil {
			return nil, nil, err
		}
		trace("limits")
		if spans.Len() != 0 {
			id := ""
			if len(runID) > 0 {
				id = runID[0]
			}
			fmt.Fprintf(os.Stderr, "POOL_CATALOG run=%s index=%d start_ns=%d end_ns=%d%s\n", id, index, start.UnixNano(), last.UnixNano(), spans.String())
		}
		if material != nil {
			materials = append(materials, material)
		}
		stages = append(stages, restore.StagedCatalog{Ref: result.PreparedSource.Ref, SandboxHint: result.PreparedSource.SandboxRef, Locations: result.RefLocationURIs, ManifestConfig: catalog.Prepare.ManifestConfig, Key: catalog.ManifestKey, Files: files})
	}
	if err := json.NewEncoder(&limitedStageWriter{w: io.Discard, left: (8 << 20) + 1}).Encode(stages); err != nil {
		return nil, nil, err
	}
	return stages, materials, nil
}
func stageCatalogFile(ctx context.Context, catalogs []configsock.PreparedCatalog) (*os.File, []*taskartifact.SlotMaterial, error) {
	if len(catalogs) == 0 {
		return nil, nil, nil
	}
	stages, materials, err := stageCatalogValues(ctx, catalogs)
	if err != nil {
		return nil, nil, err
	}
	raw, err := json.Marshal(stages)
	if err != nil {
		return nil, nil, err
	}
	defer clear(raw)
	if len(raw) > 8<<20 {
		return nil, nil, fmt.Errorf("stage descriptor exceeds limit")
	}
	fd, err := unix.MemfdCreate("slot-template-stages", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, nil, err
	}
	f := os.NewFile(uintptr(fd), "slot-template-stages")
	if _, err = f.Write(raw); err == nil {
		_, err = f.Seek(0, 0)
	}
	if err == nil {
		_, err = unix.FcntlInt(f.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_SHRINK|unix.F_SEAL_GROW|unix.F_SEAL_SEAL)
	}
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	syscall.CloseOnExec(fd)
	return f, materials, nil
}

type limitedStageWriter struct {
	w    io.Writer
	left int
}

func (w *limitedStageWriter) Write(p []byte) (int, error) {
	if len(p) > w.left {
		return 0, fmt.Errorf("stage output too large")
	}
	w.left -= len(p)
	return w.w.Write(p)
}
