package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"net/http"
	"strings"
	"testing"
)

type forkTestCore struct {
	Core
	called      bool
	key, parent string
	timeout     int
}

func (c *forkTestCore) ForkSandbox(_ context.Context, key, parent string, timeout int) (*types.Sandbox, error) {
	c.called = true
	c.key = key
	c.parent = parent
	c.timeout = timeout
	return nil, ErrNotFound
}
func TestForkRouteValidationAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		body   string
		status int
		call   bool
	}{
		{`{}`, 404, true}, {`{"timeout":30}`, 404, true}, {`{"timeout":-1}`, 400, false}, {`{"count":1}`, 404, true}, {`{"count":21}`, 400, false}, {`{"count":0}`, 400, false}, {`{"count":2}`, 501, false},
		{`{"templateID":"other"}`, 400, false}, {`{} {}`, 400, false}, {`null`, 400, false},
	} {
		t.Run(tc.body, func(t *testing.T) {
			core := &forkTestCore{}
			handler, key := newMigrationTestHandler(t, core)
			response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes/parent/fork", strings.NewReader(tc.body), nil)
			if response.Code != tc.status || core.called != tc.call {
				t.Fatalf("status=%d called=%t body=%s", response.Code, core.called, response.Body.String())
			}
			if core.called && (core.key != key || core.parent != "parent") {
				t.Fatal("lost authenticated source identity")
			}
		})
	}
}

func TestV2ConnectPreservesResumeSelection(t *testing.T) {
	core := &migrationCoreStub{connect: func(_ context.Context, id, _, _ string, options ConnectOptions) (*types.Sandbox, error) {
		if id != "sandbox" || options.Memory == nil || *options.Memory || options.TimeoutSec != 19 {
			t.Fatalf("lost v2 request: %+v", options)
		}
		return &types.Sandbox{ID: id, Profile: types.ProfileE2B}, nil
	}}
	h, key := newMigrationTestHandler(t, core)
	response := migrationRequest(t, h, key, http.MethodPost, "/v2/sandboxes/sandbox/connect", strings.NewReader(`{"memory":false,"timeout":19}`), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

type batchForkTestCore struct {
	forkTestCore
	count int
}

func (c *batchForkTestCore) ForkSandboxes(_ context.Context, key, id string, timeout, count int) ([]ForkItem, error) {
	c.count = count
	return []ForkItem{{Sandbox: &types.Sandbox{ID: "child", Profile: types.ProfileE2B}}, {Err: errors.New("private diagnostic must not escape")}}, nil
}
func TestForkBatchSDKResultEnvelope(t *testing.T) {
	core := &batchForkTestCore{}
	h, key := newMigrationTestHandler(t, core)
	response := migrationRequest(t, h, key, http.MethodPost, "/sandboxes/parent/fork", strings.NewReader(`{"count":2}`), nil)
	if response.Code != 201 || core.count != 2 {
		t.Fatalf("status=%d count=%d", response.Code, core.count)
	}
	var result []struct {
		Sandbox map[string]any `json:"sandbox"`
		Error   map[string]any `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 || result[0].Sandbox["sandboxID"] != "child" || result[1].Error["code"] != float64(500) {
		t.Fatalf("bad SDK result %s", response.Body.String())
	}
	if strings.Contains(response.Body.String(), "private diagnostic") {
		t.Fatal("leaked internal error")
	}
}
