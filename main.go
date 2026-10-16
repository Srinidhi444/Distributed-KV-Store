package main

import (
	"fmt"
	"log"
	"net/http"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET requests are allowed", http.StatusMethodNotAllowed)
		return
	}

	fmt.Fprintln(w, "OK")
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", healthHandler)

	log.Println("Server running on port 8080")

	log.Fatal(http.ListenAndServe(":8080", mux))
}
