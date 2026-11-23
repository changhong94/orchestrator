// Package telemetryview carries the telemetry projection selector without
// importing the proxy implementation into configuration clients and VM slots.
package telemetryview

import "context"

type physicalKey struct{}

// WithPhysical selects the original proxy idle timestamp for legacy collectors.
func WithPhysical(ctx context.Context) context.Context {
	return context.WithValue(ctx, physicalKey{}, true)
}

func IsPhysical(ctx context.Context) bool {
	physical, _ := ctx.Value(physicalKey{}).(bool)
	return physical
}
