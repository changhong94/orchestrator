package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ForkCore is optional: cluster routing must not silently emulate node-local fork.
type ForkCore interface {
	ForkSandbox(context.Context, string, string, int) (*types.Sandbox, error)
}

type ForkItem struct {
	Sandbox *types.Sandbox
	Err     error
}
type ForkBatchCore interface {
	ForkSandboxes(context.Context, string, string, int, int) ([]ForkItem, error)
}

func (a *API) forkSandbox(w http.ResponseWriter, r *http.Request) {
	core, ok := a.core.(ForkCore)
	if !ok {
		writeErr(w, http.StatusNotImplemented, "fork is unavailable on this endpoint")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var body struct {
		Timeout int  `json:"timeout"`
		Count   *int `json:"count,omitempty"`
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, 400, "bad fork body")
		return
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] != '{' {
		writeErr(w, 400, "fork body must be an object")
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&body)
	if err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, 400, "bad fork body")
		return
	}
	if err == nil {
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			writeErr(w, 400, "bad fork body")
			return
		}
	}
	if body.Count != nil && (*body.Count < 1 || *body.Count > 20) {
		writeErr(w, 400, "fork count must be 1..20")
		return
	}
	if body.Timeout < 0 {
		writeErr(w, 400, "timeout must be nonnegative")
		return
	}
	count := 1
	if body.Count != nil {
		count = *body.Count
	}
	if batch, ok := a.core.(ForkBatchCore); ok {
		items, err := batch.ForkSandboxes(r.Context(), apiKeyFrom(r.Context()), r.PathValue("id"), body.Timeout, count)
		if err != nil {
			a.fail(w, err)
			return
		}
		results := make([]map[string]any, len(items))
		for i, item := range items {
			if item.Err != nil || item.Sandbox == nil {
				// Do not expose task diagnostics, filesystem paths or tenant credentials.
				code := http.StatusInternalServerError
				if errors.Is(item.Err, ErrNotAllowed) {
					code = http.StatusForbidden
				}
				if errors.Is(item.Err, ErrBadRequest) {
					code = http.StatusBadRequest
				}
				if errors.Is(item.Err, ErrResourceUnavailable) || errors.Is(item.Err, ErrProxyUnavailable) || errors.Is(item.Err, ErrExtensionUnavailable) {
					code = http.StatusServiceUnavailable
				}
				if errors.Is(item.Err, ErrNotFound) {
					code = http.StatusNotFound
				}
				if errors.Is(item.Err, ErrAlreadyExists) || errors.Is(item.Err, ErrSandboxChanged) {
					code = http.StatusConflict
				}
				if errors.Is(item.Err, context.DeadlineExceeded) {
					code = http.StatusGatewayTimeout
				}
				results[i] = map[string]any{"error": map[string]any{"code": code, "message": "forked sandbox could not start"}}
			} else {
				results[i] = map[string]any{"sandbox": a.sandboxResp(item.Sandbox)}
			}
		}
		writeJSON(w, http.StatusCreated, results)
		return
	}
	if count != 1 {
		writeErr(w, 501, "batch fork is unavailable on this endpoint")
		return
	}
	sb, err := core.ForkSandbox(r.Context(), apiKeyFrom(r.Context()), r.PathValue("id"), body.Timeout)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, []map[string]any{{"sandbox": a.sandboxResp(sb)}})
}
