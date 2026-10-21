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

func run() error {
	// Step 1: Ensure the data directory exists.
	if err := os.MkdirAll("data", 0700); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	// Step 2: Open the WAL and recover existing database state.
	walPath := "data/store.wal"

	persistentDB, err := persistence.OpenPersistentStore(walPath)
	if err != nil {
		return fmt.Errorf("open persistent database: %w", err)
	}

	// Ensure the WAL is eventually closed.
	defer func() {
		if err := persistentDB.Close(); err != nil {
			log.Printf("failed to close persistent database: %v", err)
		}
	}()

	// Step 3: Initialize HTTP handlers.
	apiHandler := api.NewAPI(persistentDB)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("PUT /kv/{key}", apiHandler.SetHandler)
	mux.HandleFunc("GET /kv/{key}", apiHandler.GetHandler)
	mux.HandleFunc("DELETE /kv/{key}", apiHandler.DeleteHandler)

	// Step 4: Configure the HTTP server.
	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Step 5: Listen for operating system shutdown signals.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Step 6: Start the HTTP server in a goroutine.
	serverErr := make(chan error, 1)

	go func() {
		serverErr <- server.ListenAndServe()
	}()

	log.Println("Server running on port 8080")

	// Step 7: Wait for shutdown or an unexpected server failure.
	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server failed: %w", err)
		}
		return nil

	case <-ctx.Done():
		log.Println("Shutdown signal received")
	}

	// Stop receiving further shutdown signals.
	stop()

	// Step 8: Give active requests time to finish.
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		// Force the HTTP server to close its connections.
		// In-flight handlers may still be running.
		_ = server.Close()

		// Do not close the database while handlers may
		// still be accessing its WAL.
		return fmt.Errorf("HTTP shutdown failed: %w", err)
	}

	// Step 9: Wait for the serving goroutine to exit.
	if err := <-serverErr; err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server stopped: %w", err)
	}

	log.Println("HTTP server shut down successfully")
	return nil
}
