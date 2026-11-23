package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// LiveSnapshot is a durable reusable native snapshot template. Metadata contains
// only launch-safe inherited settings; the original sandbox stays running.
// ElapsedMS includes capture and publication, not just application pause.
type LiveSnapshot struct {
	TemplateID   string            `json:"templateID"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	GuestPauseMS *float64          `json:"guestPauseMs,omitempty"`
	ElapsedMS    float64           `json:"elapsedMs"`
}
type LiveSnapshotCore interface {
	SnapshotRunningSandbox(context.Context, string, string) (LiveSnapshot, error)
}

func (a *API) snapshotRunningSandbox(w http.ResponseWriter, r *http.Request) {
	core, ok := a.core.(LiveSnapshotCore)
	if !ok {
		writeErr(w, 501, "live snapshots are unavailable on this endpoint")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
	if err != nil {
		writeErr(w, 400, "bad snapshot body")
		return
	}
	raw = bytes.TrimSpace(raw)
	var body map[string]json.RawMessage
	if len(raw) > 0 && (raw[0] != '{' || json.Unmarshal(raw, &body) != nil || len(body) != 0) {
		writeErr(w, 400, "live snapshot uses the configured storage policy; body must be empty")
		return
	}
	result, err := core.SnapshotRunningSandbox(r.Context(), apiKeyFrom(r.Context()), r.PathValue("id"))
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}
