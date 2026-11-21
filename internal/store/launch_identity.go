package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// GetLaunchIdentity reads the committed ownership/source/network projection.
// It is deliberately not a general sandbox load: task handoff needs no secret
// decryption or metadata/environment JSON decoding. Bootstrap still uses Get.
// Callers must not publish this partial row or use it to launch a workload.
func (s *Store) GetLaunchIdentity(ctx context.Context, id string) (*types.Sandbox, error) {
	sb := &types.Sandbox{}
	err := s.readDB().QueryRowContext(ctx, `SELECT id,state,run_id,template_id,created_unix,launch_mode,
 resume_source_kind,resume_source_ref,resume_sandbox_ref,vswitch_port,floatingip,inner_ip,port_mac
 FROM sandboxes WHERE id=?`, id).Scan(&sb.ID, &sb.State, &sb.RunID, &sb.TemplateID, &sb.CreatedUnix, &sb.LaunchMode,
		&sb.ResumeSource.Kind, &sb.ResumeSource.Ref, &sb.ResumeSource.SandboxRef, &sb.VswitchPort, &sb.FloatingIP, &sb.InnerIP, &sb.PortMAC)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read launch identity: %w", err)
	}
	return sb, nil
}
