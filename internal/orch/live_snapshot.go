package orch

import (
	"context"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// SnapshotRunningSandbox shares exactly the fork capture/publication barrier,
// but does not allocate a child. The resulting template is reusable through the
// normal Create API after the source is deleted. Publication owns its immutable
// graph; the temporary capture directory is not a retention mechanism.
func (o *Orchestrator) SnapshotRunningSandbox(ctx context.Context, key, id string) (api.LiveSnapshot, error) {
	started := time.Now()
	var result api.LiveSnapshot
	_, err := o.withForkTemplate(ctx, key, id, func(_ context.Context, template string, metadata map[string]string) (*types.Sandbox, error) {
		result.TemplateID = template
		result.Metadata = metadata
		return nil, nil
	}, func(ms float64) { result.GuestPauseMS = &ms })
	if err != nil {
		return api.LiveSnapshot{}, err
	}
	result.ElapsedMS = float64(time.Since(started).Microseconds()) / 1000
	return result, nil
}
