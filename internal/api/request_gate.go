package api

import (
	"net/http"
	"sync"
)

type RequestGate struct {
	mu      sync.Mutex
	cond    *sync.Cond
	active  int
	closing bool
}

func NewRequestGate() *RequestGate {
	g := &RequestGate{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *RequestGate) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()

		if g.closing {
			g.mu.Unlock()
			http.Error(w, "server shutting down", http.StatusServiceUnavailable)
			return
		}

		g.active++
		g.mu.Unlock()

		defer func() {
			g.mu.Lock()
			g.active--
			g.cond.Broadcast()
			g.mu.Unlock()
		}()

		next.ServeHTTP(w, r)
	})
}

func (g *RequestGate) StopAndWait() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.closing = true

	for g.active > 0 {
		g.cond.Wait()
	}
}
