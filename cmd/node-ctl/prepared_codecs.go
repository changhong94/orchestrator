package main

import (
	"encoding/json"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"log/slog"
	"time"
)

func warmPreparedLaunchCodecs() error {
	start := time.Now()
	// Deliberately not a valid launch: both branches are populated only to
	// initialize their field schemas, never submitted to the launcher.
	schema := configsock.SandboxTaskSpec{Prepare: &configsock.ArtifactPrepareSpec{}, Final: &configsock.LaunchSpec{}}
	raw, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	var decoded configsock.SandboxTaskSpec
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	if err := taskartifact.WarmPreparedCodecs(); err != nil {
		return err
	}
	slog.Info("prepared launch codecs ready", "duration", time.Since(start))
	return nil
}
