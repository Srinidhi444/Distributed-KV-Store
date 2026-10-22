package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/api"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/config"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/persistence"
)

var (
	ErrNodeStarted = errors.New("node already started")
	ErrNodeStopped = errors.New("node already stopped")
)

type Node struct {
	cfg config.Config
	db  *persistence.PersistentStore

	server   *http.Server
	listener net.Listener
	gate     *api.RequestGate

	mu       sync.Mutex
	started  bool
	stopped  bool
	serveErr chan error
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func New(cfg config.Config) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid node config: %w", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	db, err := persistence.OpenPersistentStore(cfg.WALPath())
	if err != nil {
		return nil, fmt.Errorf("open persistent store: %w", err)
	}
	apiHandler := api.NewAPI(db)
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("PUT /kv/{key}", apiHandler.SetHandler)
	mux.HandleFunc("GET /kv/{key}", apiHandler.GetHandler)
	mux.HandleFunc("DELETE /kv/{key}", apiHandler.DeleteHandler)

	gate := api.NewRequestGate()

	server := &http.Server{
		Addr:              cfg.Address,
		Handler:           gate.Wrap(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return &Node{
		cfg:      cfg,
		db:       db,
		server:   server,
		gate:     gate,
		serveErr: make(chan error, 1),
	}, nil
}

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
		return errors.Join(
			fmt.Errorf("listen on %s:%w", n.cfg.Address, err),
			n.db.Close(),
		)
	}
	n.listener = listener
	n.started = true

	go func() {
		err := n.server.Serve(listener)
		n.serveErr <- err
	}()
	return nil
}

func (n *Node) Shutdown(ctx context.Context) error {
	n.mu.Lock()

	if n.stopped {
		n.mu.Unlock()
		return nil
	}

	n.stopped = true
	started := n.started

	n.mu.Unlock()

	// If the node was created but never started, only storage
	// needs to be closed.
	if !started {
		n.gate.StopAndWait()
		return n.db.Close()
	}

	// Gracefully stop accepting HTTP connections and wait for
	// active requests, subject to the caller's deadline.
	shutdownErr := n.server.Shutdown(ctx)

	if shutdownErr != nil {
		// Force-close remaining connections after graceful
		// shutdown fails.
		shutdownErr = errors.Join(shutdownErr, n.server.Close())
	}

	// Ensure no request handler can use the WAL before closing it.
	n.gate.StopAndWait()

	// Collect the serving goroutine's result.
	serveErr := <-n.serveErr
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}

	// Release the WAL file and its exclusive lock.
	closeErr := n.db.Close()

	return errors.Join(shutdownErr, serveErr, closeErr)
}
