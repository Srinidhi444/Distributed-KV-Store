package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/store"
)

type API struct {
	db *store.Store
}
type SetRequest struct {
	Value string `json:"value"`
}

type GetResponse struct {
	Value string `json:"value"`
}

func NewAPI(db *store.Store) *API {
	return &API{db: db}
}

func (a *API) SetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	var req SetRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if key == "" {
		http.Error(w, "Key cannot be empty", http.StatusBadRequest)
		return
	}

	a.db.Set(key, req.Value)
	w.WriteHeader(http.StatusOK)
}

func (a *API) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	if key == "" {
		http.Error(w, "Key cannot be empty", http.StatusBadRequest)
		return
	}

	value, exists := a.db.Get(key)
	if !exists {
		http.Error(w, "Key doesn't exist", http.StatusNotFound)
		return
	}

	res := GetResponse{
		Value: value,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Printf("failed to write GET response: %v", err)
	}
}

func (a *API) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "key cannot be empty", http.StatusBadRequest)
		return
	}
	a.db.Delete(key)
	w.WriteHeader(http.StatusNoContent)
}
