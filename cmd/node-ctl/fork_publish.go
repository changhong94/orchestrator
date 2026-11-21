package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

// This helper is the tenant boundary: graph reads and keys stay outside the
// conductor. Prepare resolves every named input location before publication.
func forkPublishCmd(args []string) error {
	fs := flag.NewFlagSet("fork-publish", flag.ContinueOnError)
	manifestConfig := fs.String("manifest-config", "", "manifest storage configuration")
	parent := fs.String("ref-location-parent", "", "input publication namespace")
	destination := fs.String("to-ref-location", "", "optional local destination name=URI")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("fork-publish requires one snapshot root")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	prepared, err := taskartifact.Prepare(ctx, configsock.ArtifactPrepareSpec{
		RootSourceKind: string(types.ResumeSourceSnapshot), RootRef: fs.Arg(0),
		LaunchMode: string(types.LaunchMemory), ManifestConfig: *manifestConfig,
		RefLocationParent: *parent,
	})
	if err != nil {
		return err
	}
	argv := []string{"publish", "--json", "--quiet", "--manifest-config", *manifestConfig}
	names := make([]string, 0, len(prepared.RefLocationURIs))
	for name := range prepared.RefLocationURIs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		argv = append(argv, "--ref-location", name+"="+prepared.RefLocationURIs[name])
	}
	if *destination != "" {
		argv = append(argv, "--to-ref-location", *destination)
	}
	argv = append(argv, fs.Arg(0))
	self, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, filepath.Join(filepath.Dir(self), "sandbox-ctl"), argv...)
	var result bytes.Buffer
	cmd.Stdout, cmd.Stderr = &result, os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	// The upstream publisher guarantees a verified logical graph. Before this
	// fork helper acknowledges durable completion, sync all referenced local
	// namespaces plus the new destination. Remote store commits have their own
	// durability contract. Do not print the success record before these barriers.
	locations := make([]string, 0, len(names)+1)
	for _, name := range names {
		locations = append(locations, prepared.RefLocationURIs[name])
	}
	if *destination != "" {
		_, uri, ok := strings.Cut(*destination, "=")
		if !ok {
			return fmt.Errorf("invalid publication destination")
		}
		locations = append(locations, uri)
	}
	for _, location := range locations {
		u, err := url.Parse(location)
		if err != nil || u.Scheme != "file" || u.Host != "" || !filepath.IsAbs(u.Path) {
			return fmt.Errorf("invalid local publication location")
		}
		if err := snapshot.SyncLocalPublication(ctx, u.Path); err != nil {
			return fmt.Errorf("persist fork publication: %w", err)
		}
	}
	_, err = os.Stdout.Write(result.Bytes())
	return err
}
