package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

type liveSnapshotTestCore struct {
	Core
	called  bool
	key, id string
}

func (c *liveSnapshotTestCore) SnapshotRunningSandbox(_ context.Context, key, id string) (LiveSnapshot, error) {
	c.called = true
	c.key = key
	c.id = id
	return LiveSnapshot{TemplateID: "durable-template", ElapsedMS: 42}, nil
}
func TestLiveSnapshotRouteUsesAuthenticatedOwnerAndFixedStorage(t *testing.T) {
	for _, body := range []string{"", "{}", "{ }", "null", `{"upload":true}`, `{} {}`} {
		core := &liveSnapshotTestCore{}
		h, key := newMigrationTestHandler(t, core)
		response := migrationRequest(t, h, key, http.MethodPost, "/sandboxes/parent/snapshots", strings.NewReader(body), nil)
		valid := body == "" || body == "{}" || body == "{ }"
		if valid {
			if response.Code != 201 || !core.called || core.key != key || core.id != "parent" {
				t.Fatalf("bad snapshot dispatch %d", response.Code)
			}
			if !strings.Contains(response.Body.String(), `"templateID":"durable-template"`) {
				t.Fatal(response.Body.String())
			}
		} else if response.Code != 400 || core.called {
			t.Fatal("bad snapshot body accepted", body)
		}
	}
}
