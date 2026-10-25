package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/raft"
)

// Backend is the only path through which HTTP reads and writes reach Raft.
// Reads must be linearizable; writes must wait for majority commitment.
type Backend interface {
	Set(context.Context, string, string) error
	Get(context.Context, string) (string, bool, error)
	Delete(context.Context, string) error
}

type API struct{ backend Backend }
type SetRequest struct {
	Value string `json:"value"`
}
type GetResponse struct {
	Value string `json:"value"`
}

// NewAPI constructs the public client API using a consensus-backed backend.
func NewAPI(backend Backend) *API { return &API{backend: backend} }

// SetHandler proposes a SET command and acknowledges it only after commitment.
func (a *API) SetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if strings.TrimSpace(key) == "" {
		http.Error(w, "key cannot be empty", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var raw struct {
		Value *string `json:"value"`
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&raw) != nil || raw.Value == nil {
		http.Error(w, "invalid JSON value", http.StatusBadRequest)
		return
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		http.Error(w, "unexpected trailing JSON", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.backend.Set(ctx, key, *raw.Value); err != nil {
		writeBackendError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// GetHandler performs a quorum-verified read rather than a follower-local read.
func (a *API) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if strings.TrimSpace(key) == "" {
		http.Error(w, "key cannot be empty", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	value, exists, err := a.backend.Get(ctx, key)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	if !exists {
		http.Error(w, "key does not exist", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(GetResponse{Value: value})
}

// DeleteHandler proposes DELETE through Raft and waits for commitment.
func (a *API) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if strings.TrimSpace(key) == "" {
		http.Error(w, "key cannot be empty", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.backend.Delete(ctx, key); err != nil {
		writeBackendError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeBackendError distinguishes absence of leadership from uncertain outcomes.
func writeBackendError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrRuntimeStopped), errors.Is(err, raft.ErrRuntimeBusy):
		http.Error(w, "node is not the available Raft leader", http.StatusServiceUnavailable)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		http.Error(w, "operation outcome uncertain; retry carefully", http.StatusGatewayTimeout)
	default:
		http.Error(w, "Raft operation failed", http.StatusInternalServerError)
	}
}
