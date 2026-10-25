package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/config"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/node"
)

// main runs one Raft-backed KV node and reports startup/shutdown failures.
func main() {
	if err := run(); err != nil {
		log.Printf("application error: %v", err)
		os.Exit(1)
	}
}

// run starts the node and shuts it down on a signal or unexpected server exit.
func run() error {
	cfg, err := config.ParseFlags(os.Args[1:])
	if err != nil {
		return fmt.Errorf("parse node configuration: %w", err)
	}
	n, err := node.New(cfg)
	if err != nil {
		return fmt.Errorf("initialize node %s: %w", cfg.NodeID, err)
	}
	if err := n.Start(); err != nil {
		return fmt.Errorf("start node %s: %w", cfg.NodeID, err)
	}
	log.Printf("node %s listening on %s (Raft, data: %s)", cfg.NodeID, cfg.Address, cfg.DataDir)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var runErr error
	select {
	case <-ctx.Done():
	case err := <-n.ServeErrors():
		if err != nil {
			runErr = fmt.Errorf("HTTP server exited: %w", err)
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return errors.Join(runErr, n.Shutdown(shutdownCtx))
}
