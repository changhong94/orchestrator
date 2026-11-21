package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
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
func stageCatalogFile(ctx context.Context, catalogs []configsock.PreparedCatalog) (*os.File, []*taskartifact.SlotMaterial, error) {
	if len(catalogs) == 0 {
		return nil, nil, nil
	}
	self, err := os.Executable()
	if err != nil {
		return nil, nil, err
	}
	var stages []map[string]any
	var materials []*taskartifact.SlotMaterial
	for _, catalog := range catalogs {
		input, err := json.Marshal(catalog.Prepare)
		if err != nil {
			return nil, nil, err
		}
		cmd := exec.CommandContext(ctx, self, "stage-artifact")
		cmd.Stdin = bytes.NewReader(input)
		cmd.Stderr = os.Stderr
		for _, item := range taskEnv(nil) {
			if !strings.HasPrefix(item, "MANIFEST_KEY=") {
				cmd.Env = append(cmd.Env, item)
			}
		}
		cmd.Env = append(cmd.Env, "MANIFEST_KEY="+catalog.ManifestKey)
		// The helper's stdout is bounded before accepting a stage descriptor.
		var out bytes.Buffer
		cmd.Stdout = &limitedStageWriter{w: &out, left: 4 << 20}
		if err = cmd.Run(); err != nil {
			return nil, nil, fmt.Errorf("stage artifact: %w", err)
		}
		var descriptor map[string]any
		decoder := json.NewDecoder(bytes.NewReader(out.Bytes()))
		decoder.UseNumber()
		if err = decoder.Decode(&descriptor); err != nil {
			return nil, nil, err
		}
		if value := descriptor["material"]; value != nil {
			raw, err := json.Marshal(value)
			if err != nil {
				return nil, nil, err
			}
			var material taskartifact.SlotMaterial
			if err = json.Unmarshal(raw, &material); err != nil {
				return nil, nil, err
			}
			materials = append(materials, &material)
		}
		delete(descriptor, "material")
		descriptor["key"] = catalog.ManifestKey
		stages = append(stages, descriptor)
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
