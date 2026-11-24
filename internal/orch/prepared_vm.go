package orch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/stdio"
)

// PreparedRuntimeFor is available only to an authenticated, durably owned idle
// runner. It contains host configuration, never a tenant image or credentials.
func (o *Orchestrator) PreparedRuntimeFor(ctx context.Context, runID string) (response *configsock.PreparedRuntimeResponse, resultErr error) {
	traceLast := time.Now()
	trace := func(stage string) {
		if os.Getenv("KUASAR_POOL_TRACE") == "1" {
			now := time.Now()
			fmt.Fprintf(os.Stderr, "POOL_STAGE kind=lease run=%s stage=%s us=%d end_ns=%d\n", runID, stage, now.Sub(traceLast).Microseconds(), now.UnixNano())
			traceLast = now
		}
	}

	_, pool, waiting := o.runs.ownerPool(runID)
	if pool == nil || !pool.preparedVM {
		return &configsock.PreparedRuntimeResponse{}, nil
	}
	if !waiting {
		return nil, errors.New("prepared VM runner is already assigned")
	}
	if o.cfg.ResourceListen != nil && o.cfg.ResourceListen.Enabled {
		return nil, errors.New("prepared VM pool requires static resource policy")
	}
	catalogs, err := o.preparedCatalogs(ctx, runID)
	if err != nil {
		return nil, err
	}
	trace("catalog")
	o.preparedVMMu.Lock()
	trace("lock_wait")
	defer o.preparedVMMu.Unlock()
	if lease, err := o.st.GetPreparedVM(ctx, runID); err != nil {
		return nil, err
	} else if lease != nil {
		return &configsock.PreparedRuntimeResponse{Exec: o.executables.SandboxCtl(), Spec: lease.Spec, Catalogs: catalogs}, nil
	}
	resources, err := sandboxcfg.ResolveResources(sandboxcfg.ResourceResolveInput{Node: configresolve.SandboxResources(o.cfg.Sandbox.Resources)})
	if err != nil {
		return nil, err
	}
	network, err := o.resolveNetwork(types.ProfileE2B, sandboxcfg.NetworkSpec{}, o.cfg.Sandbox.Network.Hostname)
	if err != nil {
		return nil, err
	}
	capacity, err := preparedWritableCapacity(o.cfg.Sandbox.Boot.OverlayDiffTemplate)
	if err != nil {
		return nil, err
	}
	trace("spec_setup")
	port, err := o.attachNetwork(ctx, network)
	trace("network_attach")
	if err != nil {
		return nil, err
	}
	retained := false
	var recoveryLease *store.PreparedVMLease
	defer func() {
		if !retained {
			cleanup, cancel := cleanupContext()
			defer cancel()
			if detachErr := o.vs.Detach(cleanup, port.Port); detachErr != nil && !errors.Is(detachErr, vswitch.ErrPortNotAttached) {
				resultErr = errors.Join(resultErr, fmt.Errorf("release unclaimed prepared port: %w", detachErr))
				// Request cancellation must not erase the exact runner/port
				// ownership needed by retirement to retry a failed detach.
				if recoveryLease != nil {
					resultErr = errors.Join(resultErr, o.st.SavePreparedVM(cleanup, *recoveryLease))
				}
			}
		}
	}()
	sb := &types.Sandbox{ID: runID, RunID: runID}
	mode, err := stdio.FromFlags(nil, nil, nil, "", "", "", nil, sandboxJournalTarget(configsock.ConsoleTag, sb))
	if err != nil {
		return nil, err
	}
	tap := o.vs.TapFD(port.Port)
	spec, err := json.Marshal(map[string]any{
		"RestoreOnly":    pool.preparedRestore,
		"PrivateRAMFork": o.cfg.Sandbox.PrivateRAMFork,
		"SandboxID":      runID, "PathID": runID, "EnableCheckpoint": true,
		"Kernel": preparedFileRef(o.cfg.Sandbox.Boot.Kernel), "Bundle": preparedFileRef(o.cfg.Sandbox.Boot.Runtime),
		"CHBinary":    filepath.Join(filepath.Dir(o.executables.SandboxCtl()), "cloud-hypervisor"),
		"RuntimeRoot": nodepath.SandboxRunRoot(o.cfg.Paths.RunRoot), "BaseRoot": nodepath.SandboxBaseRoot(o.cfg.Paths.BaseRoot),
		"Resources": resources, "Timeouts": o.cfg.Sandbox.Timeouts.Runtime(), "Usage": o.cfg.Sandbox.Usage.Runtime(),
		"Console": mode.Console, "StatsInterval": 30 * time.Second,
		"Network": sandbox.NetworkDeviceSpec{MAC: port.MAC, TapFD: &rtconfig.TapFDConfig{Exec: tap.Exec, Socket: tap.Socket, Request: tap.Request, Timeout: tap.Timeout}},
		"Disks":   []sandbox.RuntimeDiskSpec{{Name: "root", Overlay: true, LateBindBase: true, WritableCapacity: capacity}},
	})
	if err != nil {
		return nil, err
	}
	lease := store.PreparedVMLease{RunID: runID, Port: port.Port, FloatingIP: port.FloatingIP, MAC: port.MAC, InnerIP: network.InnerIP, Spec: spec}
	recoveryLease = &lease
	trace("encode")
	if err = o.st.SavePreparedVM(ctx, lease); err != nil {
		return nil, err
	}
	trace("lease_persist")
	retained = true
	return &configsock.PreparedRuntimeResponse{Exec: o.executables.SandboxCtl(), Spec: spec, Catalogs: catalogs}, nil
}
func preparedFileRef(path string) string {
	if strings.HasPrefix(path, "file://") {
		return path
	}
	return (&url.URL{Scheme: "file", Path: path}).String()
}
func preparedWritableCapacity(ref string) (int64, error) {
	if !strings.Contains(ref, "://") {
		ref = preparedFileRef(ref)
	}
	source, err := rtconfig.ParseDiffTemplate(ref)
	if err != nil {
		return 0, err
	}
	if source.Kind == rtconfig.DiffTemplateMkfs {
		return source.Size, nil
	}
	if source.Kind != rtconfig.DiffTemplateRaw {
		return 0, fmt.Errorf("prepared pool needs a local raw or mkfs writable template")
	}
	info, err := os.Stat(source.Ref.Path)
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 {
		return 0, errors.New("invalid pool disk template")
	}
	return info.Size(), nil
}

// Called only by the exact runner retirement path, after Stop has fenced CH.
// Failed detach keeps both durable records and capacity for the existing retry.
func (o *Orchestrator) releasePreparedVM(ctx context.Context, runID string) error {
	o.preparedVMMu.Lock()
	defer o.preparedVMMu.Unlock()
	lease, err := o.st.GetPreparedVM(ctx, runID)
	if err != nil || lease == nil {
		return err
	}
	o.networkAllocationMu.Lock()
	defer o.networkAllocationMu.Unlock()
	if _, detached := o.detachedPortsPending[lease.Port]; !detached {
		if err = o.vs.Detach(ctx, lease.Port); err != nil && !errors.Is(err, vswitch.ErrPortNotAttached) {
			return err
		}
	}
	if o.detachedPortsPending == nil {
		o.detachedPortsPending = map[string]struct{}{}
	}
	o.detachedPortsPending[lease.Port] = struct{}{}
	if err = o.st.DeletePreparedVM(ctx, runID); err != nil {
		return err
	}
	delete(o.detachedPortsPending, lease.Port)
	return nil
}

func (o *Orchestrator) adoptPreparedForLaunch(ctx context.Context, sb *types.Sandbox, attempt *launchAttempt, preparation *launchPreparation, runID string, adopted *store.PreparedVMLease) error {
	if (runID != sb.ID && sb.LaunchMode != types.LaunchMemory) || (sb.LaunchMode != types.LaunchCold && sb.LaunchMode != types.LaunchImage && sb.LaunchMode != types.LaunchMemory) {
		return errors.New("invalid prepared VM admission")
	}
	if err := os.MkdirAll(sb.RunDir, 0700); err != nil {
		return err
	}
	listener, err := listenRuntimeReadiness(o.cfg.Paths.RunRoot, sb.ID)
	if err != nil {
		return err
	}
	if preparation.Source.Empty() {
		lease, err := o.st.GetPreparedVM(ctx, runID)
		if err != nil || lease == nil {
			listener.Close()
			return errors.Join(err, errors.New("prepared lease missing"))
		}
		if preparation.Network.InnerIP != lease.InnerIP {
			listener.Close()
			return errors.New("prepared network incompatible")
		}
		copy := cloneSandbox(sb)
		copy.RunID = runID
		copy.VswitchPort = lease.Port
		copy.FloatingIP = lease.FloatingIP
		copy.PortMAC = lease.MAC
		copy.InnerIP = lease.InnerIP
		tmpl, err := types.ParseTemplateID(sb.TemplateID)
		if err != nil {
			listener.Close()
			return err
		}
		params := o.sandboxParams(copy, tmpl, preparation.Spec, preparation.Network, preparation.Resources)
		if err = params.WriteYAML(o.sandboxConfigPath(copy)); err != nil {
			listener.Close()
			return err
		}
	}
	lease := adopted
	if lease == nil {
		if runID != sb.ID {
			lease, err = o.st.AdoptPreparedRestoreVM(ctx, sb.ID, runID)
		} else {
			lease, err = o.st.AdoptPreparedVM(ctx, sb.ID, runID)
		}
	}
	if err != nil {
		listener.Close()
		return err
	}
	sb.RunID = runID
	sb.VswitchPort = lease.Port
	sb.PortMAC = lease.MAC
	sb.FloatingIP = lease.FloatingIP
	sb.InnerIP = lease.InnerIP
	attempt.SetRunID(runID)
	if o.sandboxReadyTimeout > 0 {
		attempt.SetDeadline(time.Now().Add(o.sandboxReadyTimeout))
	}
	preparation.PreparedListener = listener
	o.cache(cloneSandbox(sb))
	o.publishUpsert(sb)
	o.observeSandboxUpsert(sb)
	return nil
}
func (o *Orchestrator) finishFailedPreparedAdmission(_ context.Context, attempt *launchAttempt, sb *types.Sandbox, cause error) error {
	return errors.Join(cause, o.rollbackLaunch(attempt, sb))
}
