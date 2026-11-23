package main

import (
	"fmt"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/sandboxer/pkg/memtrace"
	"log/slog"
	"os"
)

func runLightweightCLI() {
	componentexec.ClearEnvironment()
	if socket := os.Getenv("KUASAR_SHARED_CONTROLLER_SOCKET"); socket != "" {
		launchPreparedEmbedded = remotePreparedLauncher(socket)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	var err error
	if len(os.Args) < 2 {
		err = fmt.Errorf("node-runner requires run-sandbox or stage-artifact")
	} else {
		switch os.Args[1] {
		case "register-shared":
			err = registerSharedRun(os.Args[2:])
		case "run-sandbox":
			memtrace.Mark("holder_enter")
			err = runSandbox(os.Args[2:], logger)
		case "stage-artifact":
			err = stageArtifactCmd()
		default:
			err = fmt.Errorf("unsupported node-runner command %q", os.Args[1])
		}
	}
	if err != nil {
		logger.Error("node-runner", "err", err)
		os.Exit(1)
	}
}
