package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/api"
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
	apiHandler := api.NewAPI(db)
	mux := http.NewServeMux()

	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("PUT /kv/{key}", apiHandler.SetHandler)
	mux.HandleFunc("GET /kv/{key}", apiHandler.GetHandler)
	mux.HandleFunc("DELETE /kv/{key}", apiHandler.DeleteHandler)
	log.Println("Server running on port 8080")

	log.Fatal(http.ListenAndServe(":8080", mux))
}
