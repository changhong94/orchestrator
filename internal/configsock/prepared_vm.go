package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
)

const PathRunPrepareVM = "/internal/run/prepare-vm"

type PreparedCatalog struct {
	Prepare     ArtifactPrepareSpec `json:"prepare"`
	ManifestKey string              `json:"manifest_key"`
}

type PreparedRuntimeResponse struct {
	Catalogs []PreparedCatalog `json:"catalogs,omitempty"`
	Exec     string            `json:"exec,omitempty"`
	Spec     json.RawMessage   `json:"spec,omitempty"`
	Error    string            `json:"error,omitempty"`
}
type preparedRuntimeProvider interface {
	PreparedRuntimeFor(context.Context, string) (*PreparedRuntimeResponse, error)
}

func (s *Server) handleRunPrepareVM(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, 403, &PreparedRuntimeResponse{Error: "no peer credentials"})
		return
	}
	var req AssignmentRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, taskBootstrapRequestMaxBytes)).Decode(&req); err != nil || req.Kind != "sandbox" || req.RunID == "" {
		writeJSON(w, 400, &PreparedRuntimeResponse{Error: "bad request"})
		return
	}
	pidfile, ok := s.deps.Provider.RunPidFile(req.Kind, req.RunID)
	if !ok {
		writeJSON(w, 404, &PreparedRuntimeResponse{Error: "unknown runner"})
		return
	}
	if !s.taskAuthed(req.Kind+":"+req.RunID, pidfile, peer) {
		writeJSON(w, 403, &PreparedRuntimeResponse{Error: "not authorized"})
		return
	}
	provider, ok := s.deps.Provider.(preparedRuntimeProvider)
	if !ok {
		writeJSON(w, 200, &PreparedRuntimeResponse{})
		return
	}
	result, err := provider.PreparedRuntimeFor(r.Context(), req.RunID)
	if err != nil {
		writeJSON(w, 500, &PreparedRuntimeResponse{Error: err.Error()})
		return
	}
	writeJSON(w, 200, result)
}
func FetchPreparedRuntime(ctx context.Context, socket, runID string) (*PreparedRuntimeResponse, error) {
	body, _ := json.Marshal(AssignmentRequest{Kind: "sandbox", RunID: runID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathRunPrepareVM, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	resp, err := HTTPClientWithTimeout(socket, 0).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result PreparedRuntimeResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if result.Error != "" || resp.StatusCode >= 400 {
		return nil, buildResponseError(resp.StatusCode, result.Error)
	}
	return &result, nil
}
