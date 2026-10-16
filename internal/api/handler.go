package api

import (
	"encoding/json"
	"net/http"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/store"
)

type API struct {
	db *store.Store
}
type SetRequest struct {
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
