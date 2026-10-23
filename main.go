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

func main() {
	if err := run(); err != nil {
		log.Printf("application error: %v", err)
		os.Exit(1)
	}
}

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

	log.Printf(
		"node %s listening on %s, data directory: %s",
		cfg.NodeID,
		cfg.Address,
		cfg.DataDir,
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	<-ctx.Done()

	log.Printf("shutting down node %s", cfg.NodeID)

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := n.Shutdown(shutdownCtx); err != nil {
		return errors.Join(
			fmt.Errorf("shutdown node %s: %w", cfg.NodeID, err),
		)
	}

	log.Printf("node %s stopped successfully", cfg.NodeID)

	return nil
}
