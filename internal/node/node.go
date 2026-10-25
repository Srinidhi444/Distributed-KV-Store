package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/api"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/config"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/raft"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/transport"
)

var (
	ErrNodeStarted = errors.New("node already started")
	ErrNodeStopped = errors.New("node already stopped")
)

// stateMachine is an in-memory materialized view of the committed Raft log.
// On restart it is rebuilt by replaying committed entries from the Raft WAL.
type stateMachine struct {
	mu   sync.RWMutex
	data map[string]string
}

// Get returns the locally applied value; client GET requests use a Raft read barrier.
func (db *stateMachine) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	v, ok := db.data[key]
	return v, ok
}

// apply updates the state machine only after the Raft runtime commits an entry.
func (db *stateMachine) apply(entry raft.LogEntry) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	switch entry.Command.Type {
	case raft.CommandSet:
		db.data[entry.Command.Key] = entry.Command.Value
	case raft.CommandDelete:
		delete(db.data, entry.Command.Key)
	case raft.CommandNoop:
	default:
		return fmt.Errorf("unknown committed command: %d", entry.Command.Type)
	}
	return nil
}

// raftBackend routes every public KV request through the consensus engine.
type raftBackend struct {
	runtime *raft.Runtime
	db      *stateMachine
}

func (b raftBackend) Set(ctx context.Context, key, value string) error {
	_, err := b.runtime.Propose(ctx, raft.Command{Type: raft.CommandSet, Key: key, Value: value})
	return err
}
func (b raftBackend) Delete(ctx context.Context, key string) error {
	_, err := b.runtime.Propose(ctx, raft.Command{Type: raft.CommandDelete, Key: key})
	return err
}
func (b raftBackend) Get(ctx context.Context, key string) (string, bool, error) {
	return b.runtime.LinearizableRead(ctx, func() (string, bool) { return b.db.Get(key) })
}

// Node ties together transport, durable Raft storage, consensus, and public HTTP.
type Node struct {
	cfg         config.Config
	db          *stateMachine
	peers       *transport.PeerManager
	raftNode    *raft.Raft
	runtime     *raft.Runtime
	raftStorage *raft.WALStorage
	effects     *raft.EffectQueue

	server   *http.Server
	listener net.Listener
	gate     *api.RequestGate

	mu               sync.Mutex
	started, stopped bool
	serveErr         chan error
	serveDone        chan struct{}
	serveResult      error
}

// handleHealth responds to health probes without promising Raft leadership.
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// New validates configuration and assembles a single WAL-backed Raft node.
func New(cfg config.Config) (_ *Node, retErr error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid node config: %w", err)
	}
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	// Never silently ignore data written by the pre-Raft standalone KV WAL.
	if info, err := os.Stat(cfg.WALPath()); filepath.Clean(cfg.WALPath()) != filepath.Join(cfg.DataDir, "raft.wal") && err == nil && info.Size() > 8 {
		return nil, fmt.Errorf("legacy KV WAL %q contains data: migration is required before starting Raft", cfg.WALPath())
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	storage, err := raft.OpenWALStorage(filepath.Join(cfg.DataDir, "raft.wal"))
	if err != nil {
		return nil, fmt.Errorf("open Raft WAL: %w", err)
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, storage.Close())
		}
	}()

	r, err := raft.NewWithStorage(cfg.NodeID, storage)
	if err != nil {
		return nil, err
	}
	peerIDs := make([]string, 0, len(cfg.Peers))
	peerURLs := make(map[string]string, len(cfg.Peers))
	for id, address := range cfg.Peers {
		peerIDs = append(peerIDs, id)
		if !strings.Contains(address, "://") {
			address = "http://" + address
		}
		peerURLs[id] = address
	}
	sort.Strings(peerIDs)
	rt, err := raft.NewElectionRuntime(r, peerIDs, 128)
	if err != nil {
		return nil, err
	}
	db := &stateMachine{data: make(map[string]string)}
	if err := rt.SetApplyFunc(db.apply); err != nil {
		return nil, err
	}
	handler, err := raft.NewElectionEffectHandler(rt, peerURLs)
	if err != nil {
		return nil, err
	}
	queue, err := raft.NewEffectQueue(256, handler)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, queue.Close())
		}
	}()
	if err := rt.SetEffectQueue(queue); err != nil {
		return nil, err
	}

	pm := transport.NewPeerManager(cfg.NodeID, cfg.Peers, transport.NewClient())
	mux := http.NewServeMux()
	kv := api.NewAPI(raftBackend{runtime: rt, db: db})
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("PUT /kv/{key}", kv.SetHandler)
	mux.HandleFunc("GET /kv/{key}", kv.GetHandler)
	mux.HandleFunc("DELETE /kv/{key}", kv.DeleteHandler)
	mux.Handle("POST /internal/ping", transport.NewPingHandler(cfg.NodeID))
	mux.Handle("/internal/raft/", raft.NewElectionHTTPHandler(rt))
	mux.HandleFunc("GET /internal/raft/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"node_id": cfg.NodeID, "role": roleName(r.Role()), "term": r.CurrentTerm()})
	})
	gate := api.NewRequestGate()
	server := &http.Server{Addr: cfg.Address, Handler: gate.Wrap(mux), ReadHeaderTimeout: 5 * time.Second}
	return &Node{cfg: cfg, db: db, peers: pm, raftNode: r, runtime: rt, raftStorage: storage, effects: queue,
		server: server, gate: gate, serveErr: make(chan error, 1), serveDone: make(chan struct{})}, nil
}

// PingPeer contacts one configured peer using the existing transport package.
func (n *Node) PingPeer(ctx context.Context, peerID string) (transport.PingResponse, error) {
	return n.peers.PingPeer(ctx, peerID)
}

// ServeErrors lets main notice an unexpected HTTP server termination.
func (n *Node) ServeErrors() <-chan error { return n.serveErr }

// Start binds HTTP, replays committed WAL entries, and starts Raft timers.
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return ErrNodeStopped
	}
	if n.started {
		return ErrNodeStarted
	}
	listener, err := net.Listen("tcp", n.cfg.Address)
	if err != nil {
		n.stopped = true
		return errors.Join(fmt.Errorf("listen on %s: %w", n.cfg.Address, err), n.effects.Close(), n.raftStorage.Close())
	}
	if err := n.runtime.Start(); err != nil {
		n.stopped = true
		return errors.Join(err, listener.Close(), n.runtime.Stop(context.Background()), n.effects.Close(), n.raftStorage.Close())
	}
	n.listener = listener
	n.started = true
	go func() {
		err := n.server.Serve(listener)
		n.mu.Lock()
		n.serveResult = err
		n.mu.Unlock()
		n.serveErr <- err
		close(n.serveDone)
	}()
	return nil
}

// Shutdown stops external requests before stopping Raft and releasing the WAL.
func (n *Node) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil
	}
	n.stopped = true
	started := n.started
	n.mu.Unlock()
	// Stop consensus first so in-flight HTTP handlers leave rt.submit promptly.
	// This also prevents shutdown from waiting for unnecessary replication RPCs.
	rtErr := n.runtime.Stop(context.Background())
	var serverErr, serveErr error
	if started {
		// Give HTTP a short grace period. Peers may still have in-flight
		// AppendEntries calls; forcing them closed is safe after Raft stops.
		graceCtx, cancel := context.WithTimeout(ctx, time.Second)
		serverErr = n.server.Shutdown(graceCtx)
		cancel()
		if serverErr != nil {
			// A successful force-close completes shutdown even when the
			// optional grace period expired.
			if closeErr := n.server.Close(); closeErr != nil {
				serverErr = errors.Join(serverErr, closeErr)
			} else {
				serverErr = nil
			}
		}
		n.gate.StopAndWait()
		<-n.serveDone
		n.mu.Lock()
		serveErr = n.serveResult
		n.mu.Unlock()
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
	} else {
		n.gate.StopAndWait()
	}
	// The runtime owns all consensus decisions; close it before its effect
	// worker and WAL, so no transition can outlive durable storage.
	queueErr := n.effects.Close()
	closeErr := n.raftStorage.Close()
	return errors.Join(serverErr, serveErr, rtErr, queueErr, closeErr)
}

// roleName exposes Raft state in the diagnostic endpoint.
func roleName(role raft.Role) string {
	switch role {
	case raft.Follower:
		return "follower"
	case raft.Candidate:
		return "candidate"
	case raft.Leader:
		return "leader"
	default:
		return "unknown"
	}
}
