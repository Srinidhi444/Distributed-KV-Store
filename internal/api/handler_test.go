package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/raft"
)

type fakeBackend struct {
	mu   sync.Mutex
	data map[string]string
	err  error
}

func (b *fakeBackend) Set(_ context.Context, key, value string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	b.data[key] = value
	return nil
}
func (b *fakeBackend) Get(_ context.Context, key string) (string, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return "", false, b.err
	}
	v, ok := b.data[key]
	return v, ok, nil
}
func (b *fakeBackend) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	delete(b.data, key)
	return nil
}
func newAPIForTest() (*API, *fakeBackend, *http.ServeMux) {
	b := &fakeBackend{data: make(map[string]string)}
	a := NewAPI(b)
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", a.SetHandler)
	mux.HandleFunc("GET /kv/{key}", a.GetHandler)
	mux.HandleFunc("DELETE /kv/{key}", a.DeleteHandler)
	return a, b, mux
}
func serve(mux *http.ServeMux, method, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/kv/language", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}
func TestCRUD(t *testing.T) {
	_, _, mux := newAPIForTest()
	if w := serve(mux, "PUT", `{"value":"Go"}`); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := serve(mux, "GET", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"Go"`) {
		t.Fatalf("GET: %d %s", w.Code, w.Body.String())
	}
	if w := serve(mux, "DELETE", ""); w.Code != 204 {
		t.Fatal(w.Code)
	}
	if w := serve(mux, "GET", ""); w.Code != 404 {
		t.Fatal(w.Code)
	}
}
func TestInvalidSetAndEmptyValue(t *testing.T) {
	_, _, mux := newAPIForTest()
	for _, body := range []string{`{"value":}`, `{}`, `{"value":null}`, `{"value":"a"}{"value":"b"}`} {
		if w := serve(mux, "PUT", body); w.Code != 400 {
			t.Errorf("%q: %d", body, w.Code)
		}
	}
	if w := serve(mux, "PUT", `{"value":""}`); w.Code != 200 {
		t.Fatal(w.Code)
	}
}
func TestNotLeader(t *testing.T) {
	_, b, mux := newAPIForTest()
	b.err = raft.ErrNotLeader
	if w := serve(mux, "PUT", `{"value":"x"}`); w.Code != 503 {
		t.Fatalf("got %d", w.Code)
	}
	if w := serve(mux, "GET", ""); w.Code != 503 {
		t.Fatalf("got %d", w.Code)
	}
	b.err = errors.New("IO failed")
	if w := serve(mux, "DELETE", ""); w.Code != 500 {
		t.Fatalf("got %d", w.Code)
	}
}
