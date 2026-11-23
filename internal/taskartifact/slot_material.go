package taskartifact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"reflect"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// SlotMaterial is private host metadata passed only from an authenticated
// preparation helper to its owning slot runner. It admits no VM. Claim must
// match the key-bound request identity and recheck all physical file versions.
// It is not accepted from HTTP clients or persisted as a plaintext receipt.
type SlotMaterial struct {
	Request        configsock.ArtifactPrepareSpec
	RootSandboxRef string
	Identity       []byte
	Entry          preparedCacheEntry
	consumed       bool
}

func ExportSlotMaterial(ctx context.Context, spec configsock.ArtifactPrepareSpec) (json.RawMessage, error) {
	material, err := PrepareSlotMaterial(ctx, spec)
	if err != nil {
		return nil, err
	}
	if material == nil {
		return nil, nil
	}
	return json.Marshal(material)
}

// PrepareSlotMaterial preserves the same authenticated cache and physical-file
// checks as the legacy exporter, without serializing inside the shared host.
func PrepareSlotMaterial(ctx context.Context, spec configsock.ArtifactPrepareSpec) (*SlotMaterial, error) {
	access, err := preparedCacheForContext(ctx, spec)
	if err != nil || access == nil {
		return nil, err
	}
	entry, err := access.readEntry(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := consumePreparedEntry(ctx, spec, entry); err != nil {
		return nil, err
	}
	return &SlotMaterial{Request: spec, RootSandboxRef: spec.RootSandboxRef, Identity: access.aad, Entry: *entry}, nil
}

func (s *SlotMaterial) Claim(ctx context.Context, spec configsock.ArtifactPrepareSpec) (*Result, bool, error) {
	if s == nil || s.consumed {
		return nil, false, nil
	}
	started := time.Now()
	identitySpec := spec
	if spec.RootSandboxRef != s.RootSandboxRef {
		if spec.RootSandboxRef != s.Entry.Result.Summary.RootSource.SandboxRef {
			return nil, false, nil
		}
		identitySpec.RootSandboxRef = s.RootSandboxRef
	}
	access, err := preparedCacheForContext(ctx, identitySpec)
	if err != nil {
		return nil, false, err
	}
	if access == nil || !bytes.Equal(access.aad, s.Identity) {
		a, b := reflect.ValueOf(s.Request), reflect.ValueOf(spec)
		var fields []string
		for i := 0; i < a.NumField(); i++ {
			if !reflect.DeepEqual(a.Field(i).Interface(), b.Field(i).Interface()) {
				fields = append(fields, a.Type().Field(i).Name)
			}
		}
		log.Printf("slot artifact material miss changed_fields=%v", fields)
		return nil, false, nil
	}
	if s.Entry.Version != preparedCacheVersion {
		return nil, false, errors.New("slot material version")
	}
	result, err := consumePreparedEntry(ctx, spec, &s.Entry)
	if err != nil {
		return nil, false, err
	}
	s.consumed = true
	result.PrepareDuration = time.Since(started)
	result.ConfigReadDuration = result.PrepareDuration
	log.Printf("slot artifact material adopted duration=%s", result.PrepareDuration)
	return result, true, nil
}
