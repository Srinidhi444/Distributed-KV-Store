package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPingHandlerSuccess(t *testing.T) {
	handler := NewPingHandler("node2")

	req := httptest.NewRequest(
		http.MethodPost,
		"/internal/ping",
		strings.NewReader(`{"from":"node1"}`),
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp PingResponse

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}

	if resp.NodeID != "node2" {
		t.Fatalf("expected node2, got %q", resp.NodeID)
	}

	if resp.Message != "pong" {
		t.Fatalf("expected pong, got %q", resp.Message)
	}
}

func TestPingHandlerRejectsInvalidMethod(t *testing.T) {
	handler := NewPingHandler("node2")

	req := httptest.NewRequest(
		http.MethodGet,
		"/internal/ping",
		nil,
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}

	if rec.Header().Get("Allow") != http.MethodPost {
		t.Fatal("expected Allow: POST header")
	}
}

func TestPingHandlerRejectsInvalidJSON(t *testing.T) {
	handler := NewPingHandler("node2")

	bodies := []string{
		`{invalid}`,
		`{"from":`,
		`not-json`,
		``,
	}

	for _, body := range bodies {
		t.Run(fmt.Sprintf("body_%q", body), func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodPost,
				"/internal/ping",
				strings.NewReader(body),
			)

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf(
					"expected 400 for %q, got %d",
					body,
					rec.Code,
				)
			}
		})
	}
}

func TestPingHandlerRejectsEmptySender(t *testing.T) {
	handler := NewPingHandler("node2")

	bodies := []string{
		`{}`,
		`{"from":""}`,
		`{"from":"   "}`,
	}

	for _, body := range bodies {
		t.Run(body, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodPost,
				"/internal/ping",
				strings.NewReader(body),
			)

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rec.Code)
			}
		})
	}
}

func TestPingHandlerRejectsMultipleJSONObjects(t *testing.T) {
	handler := NewPingHandler("node2")

	bodies := []string{
		`{"from":"node1"}{"from":"node3"}`,
		"{\"from\":\"node1\"}\n{\"from\":\"node3\"}",
		`{"from":"node1"} garbage`,
	}

	for _, body := range bodies {
		t.Run(body, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodPost,
				"/internal/ping",
				strings.NewReader(body),
			)

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rec.Code)
			}
		})
	}
}

func TestPingHandlerRejectsUnknownFields(t *testing.T) {
	handler := NewPingHandler("node2")

	req := httptest.NewRequest(
		http.MethodPost,
		"/internal/ping",
		strings.NewReader(`{"from":"node1","unexpected":true}`),
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPingHandlerRejectsOversizedBody(t *testing.T) {
	handler := NewPingHandler("node2")

	body := `{"from":"` + strings.Repeat("x", 2048) + `"}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/internal/ping",
		strings.NewReader(body),
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPingClientSuccess(t *testing.T) {
	server := httptest.NewServer(NewPingHandler("node2"))
	defer server.Close()

	client := NewClient()

	address := strings.TrimPrefix(server.URL, "http://")

	resp, err := client.Ping(
		context.Background(),
		address,
		PingRequest{From: "node1"},
	)

	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	if resp.NodeID != "node2" || resp.Message != "pong" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestPingClientRejectsEmptySender(t *testing.T) {
	client := NewClient()

	_, err := client.Ping(
		context.Background(),
		"127.0.0.1:8002",
		PingRequest{},
	)

	if err == nil {
		t.Fatal("expected empty sender to fail")
	}
}

func TestPingClientHandlesServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		},
	))
	defer server.Close()

	client := NewClient()

	_, err := client.Ping(
		context.Background(),
		strings.TrimPrefix(server.URL, "http://"),
		PingRequest{From: "node1"},
	)

	if err == nil {
		t.Fatal("expected HTTP 503 to return an error")
	}

	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("expected status in error, got %v", err)
	}
}

func TestPingClientRejectsMalformedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{invalid-json}`))
		},
	))
	defer server.Close()

	client := NewClient()

	_, err := client.Ping(
		context.Background(),
		strings.TrimPrefix(server.URL, "http://"),
		PingRequest{From: "node1"},
	)

	if err == nil {
		t.Fatal("expected malformed response error")
	}
}

func TestPingClientRejectsMissingNodeID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"node_id":"","message":"pong"}`))
		},
	))
	defer server.Close()

	client := NewClient()

	_, err := client.Ping(
		context.Background(),
		strings.TrimPrefix(server.URL, "http://"),
		PingRequest{From: "node1"},
	)

	if err == nil {
		t.Fatal("expected missing node ID to fail")
	}
}

func TestPingClientContextCancellation(t *testing.T) {
	server := httptest.NewServer(NewPingHandler("node2"))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := NewClient()

	_, err := client.Ping(
		ctx,
		strings.TrimPrefix(server.URL, "http://"),
		PingRequest{From: "node1"},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestPingClientContextDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			// Simulate a slow peer without blocking forever.
			time.Sleep(200 * time.Millisecond)

			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"node_id":"node2","message":"pong"}`))
		},
	))
	defer server.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer cancel()

	client := NewClient()

	_, err := client.Ping(
		ctx,
		strings.TrimPrefix(server.URL, "http://"),
		PingRequest{From: "node1"},
	)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestPingClientUnreachablePeer(t *testing.T) {
	client := NewClient()

	// Port 0 is not a valid remote service endpoint.
	_, err := client.Ping(
		context.Background(),
		"127.0.0.1:0",
		PingRequest{From: "node1"},
	)

	if err == nil {
		t.Fatal("expected unreachable peer error")
	}
}
