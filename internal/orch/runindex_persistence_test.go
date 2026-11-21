package orch

import (
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"testing"
	"time"
)

func TestRunIndexRegistrationDoesNotBlockCommittedOwnerReads(t *testing.T) {
	r := &runIndex{}
	p := &runPool{name: "pool", kind: "sandbox", unitName: func(id string) string { return "test-" + id }}
	if err := r.register("old", p); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	r.persist = func(store.RunPoolOwnership) error { close(entered); <-release; return nil }
	done := make(chan error, 1)
	go func() { done <- r.register("new", p) }()
	<-entered
	reads := make(chan bool, 1)
	go func() {
		old, _, _ := r.ownerPool("old")
		fresh, _, _ := r.ownerPool("new")
		r.assigned("old", nil)
		reads <- old != "" && fresh == ""
	}()
	select {
	case ok := <-reads:
		if !ok {
			close(release)
			<-done
			t.Fatal("uncommitted slot visible or old slot lost")
		}
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("committed owner read blocked by unrelated persistence")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if name, _, _ := r.ownerPool("new"); name == "" {
		t.Fatal("durable registration not published")
	}
}
