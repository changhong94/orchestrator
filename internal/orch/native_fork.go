package orch

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"gopkg.in/yaml.v3"
	"os"
)

const nativeForkParentKey = "kuasar-sandbox.internal.native-fork-parent"

// forkPrivate keeps the authenticated source incarnation locked until the new
// runner has completed capture, child admission, identity reset and reattach.
// Normal Create owns capacity, route, network and exact-run cleanup.
func (o *Orchestrator) forkPrivate(ctx context.Context, key, id string, timeout int) (*types.Sandbox, error) {
	if key == "" {
		return nil, api.ErrNotAllowed
	}
	op, finish, err := o.beginPauseOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	op, unlock, err := o.lockLifecycleMutation(op, id)
	if err != nil {
		return nil, err
	}
	defer unlock()
	parent, err := o.st.Get(op, id)
	if err != nil {
		return nil, err
	}
	if !ownsSandbox(parent, key) {
		return nil, api.ErrNotFound
	}
	if parent.State != types.StateRunning {
		return nil, fmt.Errorf("%w: source must be running", api.ErrBadRequest)
	}
	pair, err := o.resolveAllowed(op, key)
	if err != nil {
		return nil, err
	}
	metadata := cloneStringMap(parent.Metadata)
	delete(metadata, nativeForkParentKey)
	tmpl, normalized, doc, err := o.normalizeSandboxCreateDefinition(op, key, parent.TemplateID, metadata, nil)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = o.cfg.Sandbox.TimeoutSec
	}
	if err := o.preparePause(op, parent, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}); err != nil {
		return nil, err
	}
	defer o.rollbackPause(op, parent)
	child, _, err := o.acceptSandboxCreate(op, sandboxCreateSpec{ID: uuid.NewString(), NativeParent: id, Template: tmpl, Pair: pair, Config: normalized, Env: cloneStringMap(parent.Env), TimeoutSeconds: timeout, InitialMMDS: initialMMDSRouteSecretValues(doc)})
	if err != nil {
		return nil, err
	}
	ready, err := o.waitLaunchState(op, child.ID)
	if err != nil {
		return nil, err
	}
	if ready == nil {
		return nil, fmt.Errorf("native child launch failed")
	}
	return ready, nil
}

func (o *Orchestrator) writeLaunchConfig(p sandboxcfg.Params, sb *types.Sandbox) error {
	if sb.ResumeSource.Empty() && sb.Metadata[nativeForkParentKey] != "" {
		cfg, err := p.BuildForkHostConfig()
		if err != nil {
			return err
		}
		raw, err := yaml.Marshal(cfg)
		if err != nil {
			return err
		}
		return os.WriteFile(o.sandboxConfigPath(sb), raw, 0600)
	}
	return p.WriteYAML(o.sandboxConfigPath(sb))
}
