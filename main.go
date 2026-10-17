package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/api"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/persistence"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/store"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET requests are allowed", http.StatusMethodNotAllowed)
		return
	}

	fmt.Fprintln(w, "OK")
}

func main() {
	db := store.NewStore()

	logPath := "data/store.log"

	// Restore database state from disk
	if err := persistence.ReplayLog(logPath, db); err != nil {
		log.Fatalf("failed to recover database: %v", err)
	}

	// Wrap the in-memory store with persistence
	persistentDB := persistence.NewPersistentStore(db, logPath)

	apiHandler := api.NewAPI(persistentDB)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("PUT /kv/{key}", apiHandler.SetHandler)
	mux.HandleFunc("GET /kv/{key}", apiHandler.GetHandler)
	mux.HandleFunc("DELETE /kv/{key}", apiHandler.DeleteHandler)

	log.Println("Server running on port 8080")

	log.Fatal(http.ListenAndServe(":8080", mux))
}
