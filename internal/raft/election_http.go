package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	votePath          = "/internal/raft/request-vote"
	appendEntriesPath = "/internal/raft/append-entries"
)

// NewElectionHTTPHandler registers incoming Raft vote and replication RPCs.
//
// Both endpoints delegate state changes to the serialized
// Raft runtime, which persists required changes before replying.
//
// This transport assumes a trusted development network.
// Sender identity is not authenticated.
func NewElectionHTTPHandler(rt *Runtime) http.Handler {
	mux := http.NewServeMux()

	// Process incoming vote requests.
	mux.HandleFunc("POST "+votePath, func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var request RequestVoteRequest
		decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&request); err != nil {
			http.Error(w, "invalid RequestVote request", http.StatusBadRequest)
			return
		}

		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			http.Error(w, "unexpected trailing JSON", http.StatusBadRequest)
			return
		}

		response, _, err := rt.SubmitRequestVote(
			r.Context(),
			request.CandidateID,
			request,
		)
		if err != nil {
			http.Error(w, "RequestVote processing failed", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})

	// Process both empty heartbeats and entries carrying log updates.
	mux.HandleFunc("POST "+appendEntriesPath, func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var request AppendEntriesRequest
		decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&request); err != nil {
			http.Error(w, "invalid AppendEntries request", http.StatusBadRequest)
			return
		}

		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			http.Error(w, "unexpected trailing JSON", http.StatusBadRequest)
			return
		}

		response, err := rt.SubmitAppendEntries(
			r.Context(),
			request.LeaderID,
			request,
		)
		if err != nil {
			http.Error(w, "AppendEntries processing failed", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})

	return mux
}

// NewElectionEffectHandler sends RequestVote and AppendEntries RPCs.
//
// Network calls run in separate goroutines so slow peers
// do not block the ordered effect queue.
func NewElectionEffectHandler(
	rt *Runtime,
	peerURLs map[string]string,
) (EffectHandler, error) {
	if rt == nil {
		return nil, errors.New("nil raft runtime")
	}

	peers := make(map[string]string, len(peerURLs))

	for id, address := range peerURLs {
		if strings.TrimSpace(id) == "" {
			return nil, errors.New("empty peer ID")
		}

		u, err := url.Parse(address)
		if err != nil ||
			u.Scheme != "http" ||
			u.Host == "" ||
			u.User != nil ||
			u.RawQuery != "" ||
			u.Fragment != "" ||
			(u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("invalid peer URL for %q", id)
		}

		peers[id] = strings.TrimRight(address, "/")
	}

	if len(peers) != len(rt.peers) {
		return nil, errors.New("peer URL count does not match membership")
	}

	for _, peer := range rt.peers {
		if _, ok := peers[peer]; !ok {
			return nil, fmt.Errorf("missing URL for peer %q", peer)
		}
	}

	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	return func(effects Effects) error {
		// State-machine application will be connected
		// after log replication and commitment.
		if len(effects.Apply) != 0 {
			return errors.New("state-machine application not connected")
		}

		for _, message := range effects.Messages {
			address, ok := peers[message.To]
			if !ok {
				return fmt.Errorf("unknown peer %q", message.To)
			}

			switch message.Type {
			case MessageRequestVote:
				if message.RequestVote == nil {
					return errors.New("missing RequestVote payload")
				}

				go sendRequestVote(rt, client, address, message)

			case MessageAppendEntries:
				if message.AppendEntries == nil {
					return errors.New("missing AppendEntries payload")
				}

				go sendAppendEntries(rt, client, address, message)

			default:
				return fmt.Errorf(
					"unsupported Raft message type %d",
					message.Type,
				)
			}
		}

		return nil
	}, nil
}

// sendRequestVote sends a vote request to one peer and
// submits its response back to the serialized Raft runtime.
//
// It skips requests for elections that are no longer active.
func sendRequestVote(
	rt *Runtime,
	client *http.Client,
	address string,
	message OutboundMessage,
) {
	request := message.RequestVote
	if request == nil {
		return
	}

	rt.raft.mu.RLock()
	active := !rt.raft.failed &&
		rt.raft.role == Candidate &&
		rt.raft.currentTerm == request.Term
	rt.raft.mu.RUnlock()

	if !active {
		return
	}

	body, err := json.Marshal(request)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		address+votePath,
		bytes.NewReader(body),
	)
	if err != nil {
		return
	}

	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := client.Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return
	}

	var response RequestVoteResponse
	decoder := json.NewDecoder(
		io.LimitReader(httpResp.Body, 64<<10),
	)

	if err := decoder.Decode(&response); err != nil {
		return
	}

	event := Event{
		Type:                EventRequestVoteResponse,
		From:                message.To,
		RequestID:           request.Term,
		RequestVoteResponse: &response,
	}

	submitRPCResponse(rt, ctx, event)
}

// sendAppendEntries sends a heartbeat or log replication request and
// correlates the response with the exact suffix originally sent.
func sendAppendEntries(
	rt *Runtime,
	client *http.Client,
	address string,
	message OutboundMessage,
) {
	request := message.AppendEntries
	if request == nil {
		return
	}

	rt.raft.mu.RLock()
	active := !rt.raft.failed &&
		rt.raft.role == Leader &&
		rt.raft.currentTerm == request.Term
	rt.raft.mu.RUnlock()

	if !active {
		return
	}

	body, err := json.Marshal(request)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		address+appendEntriesPath,
		bytes.NewReader(body),
	)
	if err != nil {
		return
	}

	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := client.Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return
	}

	var response AppendEntriesResponse
	decoder := json.NewDecoder(
		io.LimitReader(httpResp.Body, 64<<10),
	)

	if err := decoder.Decode(&response); err != nil {
		return
	}

	event := Event{
		Type:                  EventAppendEntriesResponse,
		From:                  message.To,
		RequestID:             request.Term,
		SentPrevLogIndex:      request.PrevLogIndex,
		SentLastIndex:         request.PrevLogIndex + uint64(len(request.Entries)),
		AppendEntriesResponse: &response,
	}

	submitRPCResponse(rt, ctx, event)
}

// submitRPCResponse retries submitting an RPC response
// when the runtime queue is temporarily full.
//
// It stops on cancellation, shutdown, or a permanent error.
func submitRPCResponse(
	rt *Runtime,
	ctx context.Context,
	event Event,
) {
	for ctx.Err() == nil {
		_, err := rt.Submit(ctx, event)

		if err == nil {
			return
		}

		if !errors.Is(err, ErrRuntimeBusy) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-rt.stopCh:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
