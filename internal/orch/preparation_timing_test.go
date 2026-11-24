package orch

import (
	"testing"
	"time"
)

func TestPreparationTimingOnlyCountsReadyAndIsBounded(t *testing.T) {
	p := &runPool{kind: "sandbox", name: "prepared"}
	r := &runIndex{owners: map[string]*runPool{"r": p}, preparationStarts: map[string]time.Time{}}
	r.preparationStarts["r"] = time.Now().Add(-time.Millisecond)
	r.markState("r", "starting")
	if len(r.preparationHistory) != 0 {
		t.Fatal("starting is not ready")
	}
	r.markState("r", "idle")
	h := r.preparationHistory["sandbox/prepared"]
	if len(h) != 1 || h[0].ElapsedNano <= 0 || h[0].ReadyUnixNano < h[0].StartedUnixNano {
		t.Fatalf("invalid timing: %+v", h)
	}
	r.markState("r", "idle")
	if len(r.preparationHistory["sandbox/prepared"]) != 1 {
		t.Fatal("duplicate ready counted")
	}
	for i := 0; i < 4100; i++ {
		r.preparationStarts["r"] = time.Now()
		r.markState("r", "idle")
	}
	if len(r.preparationHistory["sandbox/prepared"]) != 4096 {
		t.Fatal("unbounded history")
	}
	r.preparationStarts["r"] = time.Now()
	r.markState("r", "stopping")
	if len(r.preparationStarts) != 0 {
		t.Fatal("stopped preparation retained")
	}
}
