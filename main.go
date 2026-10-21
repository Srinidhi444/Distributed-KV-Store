package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/api"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/persistence"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "OK")
}

func main() {
	if err := run(); err != nil {
		log.Printf("server stopped with error: %v", err)
		os.Exit(1)
	}
}

func run() (retErr error) {
	if err := os.MkdirAll("data", 0700); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	persistentDB, err := persistence.OpenPersistentStore("data/store.wal")
	if err != nil {
		return fmt.Errorf("open persistent database: %w", err)
	}

	defer func() {
		retErr = errors.Join(retErr, persistentDB.Close())
	}()

	apiHandler := api.NewAPI(persistentDB)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("PUT /kv/{key}", apiHandler.SetHandler)
	mux.HandleFunc("GET /kv/{key}", apiHandler.GetHandler)
	mux.HandleFunc("DELETE /kv/{key}", apiHandler.DeleteHandler)

	gate := api.NewRequestGate()

	server := &http.Server{
		Addr:              ":8080",
		Handler:           gate.Wrap(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	serverErr := make(chan error, 1)

	go func() {
		serverErr <- server.ListenAndServe()
	}()

	log.Println("Server running on port 8080")

	select {
	case err := <-serverErr:
		gate.StopAndWait()

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server failed: %w", err)
		}
		return nil

	case <-ctx.Done():
		log.Println("Shutdown signal received")
	}

	stop()

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	shutdownErr := server.Shutdown(shutdownCtx)

	if shutdownErr != nil {
		log.Printf("graceful shutdown failed: %v", shutdownErr)

		// Stop connections, then wait for handlers that
		// may still be running.
		shutdownErr = errors.Join(shutdownErr, server.Close())
	}

	// Prevent any newly entering handler from using storage,
	// and wait for handlers already admitted to finish.
	gate.StopAndWait()

	err = <-serverErr
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		shutdownErr = errors.Join(shutdownErr, err)
	}

	if shutdownErr != nil {
		return fmt.Errorf("HTTP shutdown: %w", shutdownErr)
	}

	log.Println("HTTP server shut down successfully")
	return nil
}
