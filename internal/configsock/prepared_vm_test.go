package configsock

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
)

type preparedProviderStub struct {
	stubProvider
	calls *atomic.Int32
}

func (s preparedProviderStub) PreparedRuntimeFor(context.Context, string) (*PreparedRuntimeResponse, error) {
	s.calls.Add(1)
	return &PreparedRuntimeResponse{Exec: "/private/sandbox-ctl", Spec: json.RawMessage(`{"SandboxID":"sr-test"}`)}, nil
}
func TestPreparedRuntimeAuthenticatesRunnerBeforeAllocating(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "runner.pid")
	calls := &atomic.Int32{}
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+999999))
	sock, _ := startTestServer(t, Deps{Provider: preparedProviderStub{stubProvider{pidFile: pf}, calls}})
	if _, err := FetchPreparedRuntime(context.Background(), sock, "sr-test"); err == nil {
		t.Fatal("unauthorized runner got VM lease")
	}
	if calls.Load() != 0 {
		t.Fatal("allocated before peer authentication")
	}
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	result, err := FetchPreparedRuntime(context.Background(), sock, "sr-test")
	if err != nil || result.Exec != "/private/sandbox-ctl" || calls.Load() != 1 {
		t.Fatal(result, err)
	}
	if _, err = FetchPreparedRuntime(context.Background(), sock, "sr-missing"); err == nil {
		t.Fatal("unknown runner accepted")
	}
	if calls.Load() != 1 {
		t.Fatal("unknown runner reached allocator")
	}
}
