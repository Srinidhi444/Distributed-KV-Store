package node

import (
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
