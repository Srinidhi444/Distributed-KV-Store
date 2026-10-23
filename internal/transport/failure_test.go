package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func failureTestAddress(server *httptest.Server) string {
	return strings.TrimPrefix(server.URL, "http://")
}

func TestPingUnexpectedHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		},
	))
	defer server.Close()

	client := NewClient()

	_, err := client.Ping(
		context.Background(),
		failureTestAddress(server),
		PingRequest{From: "node1"},
	)

	if !errors.Is(err, ErrUnexpectedStatus) {
		t.Fatalf("expected ErrUnexpectedStatus, got %v", err)
	}

	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("expected HTTP status in error, got %v", err)
	}
}

func TestPingInvalidResponses(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"malformed JSON", `{invalid}`},
		{"empty response", ""},
		{"missing node ID", `{"message":"pong"}`},
		{"empty node ID", `{"node_id":"","message":"pong"}`},
		{"whitespace node ID", `{"node_id":"   ","message":"pong"}`},
		{"wrong message", `{"node_id":"node2","message":"hello"}`},
		{"missing message", `{"node_id":"node2"}`},
		{"unknown field", `{"node_id":"node2","message":"pong","extra":true}`},
		{"multiple objects", `{"node_id":"node2","message":"pong"}{"node_id":"node3"}`},
		{"trailing garbage", `{"node_id":"node2","message":"pong"}garbage`},
		{"JSON null", `null`},
		{"JSON array", `[{"node_id":"node2","message":"pong"}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(tt.body))
				},
			))
			defer server.Close()

			client := NewClient()

			_, err := client.Ping(
				context.Background(),
				failureTestAddress(server),
				PingRequest{From: "node1"},
			)

			if !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("expected ErrInvalidResponse, got %v", err)
			}
		})
	}
}

func TestPingOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			body := `{"node_id":"` +
				strings.Repeat("x", maxPingResponseSize) +
				`","message":"pong"}`

			_, _ = w.Write([]byte(body))
		},
	))
	defer server.Close()

	client := NewClient()

	_, err := client.Ping(
		context.Background(),
		failureTestAddress(server),
		PingRequest{From: "node1"},
	)

	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("expected ErrInvalidResponse, got %v", err)
	}
}

func TestPingConnectionRefused(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	address := listener.Addr().String()

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	client := NewClientWithTimeout(time.Second)

	_, err = client.Ping(
		context.Background(),
		address,
		PingRequest{From: "node1"},
	)

	if !errors.Is(err, ErrPeerUnavailable) {
		t.Fatalf("expected ErrPeerUnavailable, got %v", err)
	}
}

func TestPingContextCancelled(t *testing.T) {
	server := httptest.NewServer(NewPingHandler("node2"))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := NewClient()

	_, err := client.Ping(
		ctx,
		failureTestAddress(server),
		PingRequest{From: "node1"},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestPingContextDeadlineExceeded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"node_id":"node2","message":"pong"}`))
		},
	))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client := NewClient()

	_, err := client.Ping(
		ctx,
		failureTestAddress(server),
		PingRequest{From: "node1"},
	)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

func TestPingHTTPClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
			_, _ = w.Write([]byte(`{"node_id":"node2","message":"pong"}`))
		},
	))
	defer server.Close()

	client := NewClientWithTimeout(50 * time.Millisecond)

	_, err := client.Ping(
		context.Background(),
		failureTestAddress(server),
		PingRequest{From: "node1"},
	)

	if err == nil {
		t.Fatal("expected client timeout")
	}

	var netErr net.Error
	if !errors.Is(err, context.DeadlineExceeded) &&
		!(errors.As(err, &netErr) && netErr.Timeout()) {
		t.Fatalf("expected a recognizable timeout error, got %v", err)
	}
}

func TestPeerIdentityMismatch(t *testing.T) {
	// This endpoint claims to be node3, but is registered as node2.
	server := httptest.NewServer(NewPingHandler("node3"))
	defer server.Close()

	manager := NewPeerManager(
		"node1",
		map[string]string{
			"node2": failureTestAddress(server),
		},
		NewClient(),
	)

	_, err := manager.PingPeer(context.Background(), "node2")

	if !errors.Is(err, ErrPeerIdentityMismatch) {
		t.Fatalf("expected ErrPeerIdentityMismatch, got %v", err)
	}
}

func TestUnknownPeer(t *testing.T) {
	manager := NewPeerManager(
		"node1",
		map[string]string{
			"node2": "127.0.0.1:8002",
		},
		NewClient(),
	)

	_, err := manager.PingPeer(context.Background(), "node99")

	if !errors.Is(err, ErrPeerNotFound) {
		t.Fatalf("expected ErrPeerNotFound, got %v", err)
	}
}

func TestPeerManagerCopiesConfig(t *testing.T) {
	peers := map[string]string{
		"node2": "127.0.0.1:8002",
	}

	manager := NewPeerManager("node1", peers, NewClient())

	peers["node2"] = "127.0.0.1:9999"
	peers["node3"] = "127.0.0.1:8003"

	address, err := manager.GetPeer("node2")
	if err != nil {
		t.Fatal(err)
	}

	if address != "127.0.0.1:8002" {
		t.Fatalf("original map mutation affected manager: %q", address)
	}

	if _, err := manager.GetPeer("node3"); !errors.Is(err, ErrPeerNotFound) {
		t.Fatalf("expected node3 to be absent, got %v", err)
	}
}
