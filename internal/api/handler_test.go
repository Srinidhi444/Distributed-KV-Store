package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/persistence"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/store"
)

// Creates a fresh persistent database for each test.
func setupTestAPI(t *testing.T) (*API, *store.Store) {
	t.Helper()

	db := store.NewStore()

	logPath := filepath.Join(t.TempDir(), "store.log")

	persistentDB := persistence.NewPersistentStore(db, logPath)

	return NewAPI(persistentDB), db
}

// Test 1: GET existing key
func TestGetHandler(t *testing.T) {
	apiHandler, db := setupTestAPI(t)

	db.Set("language", "Go")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", apiHandler.GetHandler)

	req := httptest.NewRequest(
		http.MethodGet,
		"/kv/language",
		nil,
	)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var response GetResponse

	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response.Value != "Go" {
		t.Errorf("expected value Go, got %q", response.Value)
	}
}

// Test 2: PUT valid JSON
func TestSetHandler(t *testing.T) {
	apiHandler, db := setupTestAPI(t)

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", apiHandler.SetHandler)

	body := strings.NewReader(`{"value":"Go"}`)
	req := httptest.NewRequest(
		http.MethodPut,
		"/kv/language",
		body,
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	value, exists := db.Get("language")
	if !exists {
		t.Fatal("expected key language to exist")
	}

	if value != "Go" {
		t.Errorf("expected Go, got %q", value)
	}
}

// Test 3: PUT invalid JSON
func TestSetInvalidJSON(t *testing.T) {
	apiHandler, db := setupTestAPI(t)

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", apiHandler.SetHandler)

	body := strings.NewReader(`{"value":}`)
	req := httptest.NewRequest(
		http.MethodPut,
		"/kv/language",
		body,
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}

	_, exists := db.Get("language")
	if exists {
		t.Error("invalid JSON should not store a value")
	}
}

// Test 4: DELETE existing key
func TestDeleteHandler(t *testing.T) {
	apiHandler, db := setupTestAPI(t)

	db.Set("language", "Go")

	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /kv/{key}", apiHandler.DeleteHandler)

	req := httptest.NewRequest(
		http.MethodDelete,
		"/kv/language",
		nil,
	)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}

	_, exists := db.Get("language")
	if exists {
		t.Error("expected key to be deleted")
	}
}

// Test 5: DELETE nonexistent key
func TestDeleteNonExistentKey(t *testing.T) {
	apiHandler, db := setupTestAPI(t)

	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /kv/{key}", apiHandler.DeleteHandler)

	req := httptest.NewRequest(
		http.MethodDelete,
		"/kv/unknown",
		nil,
	)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}

	_, exists := db.Get("unknown")
	if exists {
		t.Error("key should not exist")
	}
}

// Test 6: GET nonexistent key
func TestGetNonExistentKey(t *testing.T) {
	apiHandler, _ := setupTestAPI(t)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", apiHandler.GetHandler)

	req := httptest.NewRequest(
		http.MethodGet,
		"/kv/unknown",
		nil,
	)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}
