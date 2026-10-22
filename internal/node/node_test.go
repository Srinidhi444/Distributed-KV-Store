package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/config"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()

	return config.Config{
		NodeID:  "node1",
		Address: "127.0.0.1:0",
		DataDir: t.TempDir(),
	}
}

// testConfig uses port 0 so the OS can choose a free port.
// This is useful for isolated networking tests.
// Configuration validation currently rejects port 0, so replace
// it with a real available port before passing it to New().
func availableAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	address := listener.Addr().String()

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	return address
}

func newTestConfig(t *testing.T, id string) config.Config {
	t.Helper()

	return config.Config{
		NodeID:  id,
		Address: availableAddress(t),
		DataDir: filepath.Join(t.TempDir(), id),
	}
}

func newTestNode(t *testing.T, id string) *Node {
	t.Helper()

	n, err := New(newTestConfig(t, id))
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := n.Shutdown(ctx); err != nil {
			t.Errorf("cleanup node: %v", err)
		}
	})

	return n
}

func nodeURL(n *Node, path string) string {
	return "http://" + n.listener.Addr().String() + path
}

func httpRequest(
	t *testing.T,
	method string,
	url string,
	body string,
) (int, string) {
	t.Helper()

	req, err := http.NewRequest(
		method,
		url,
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}

	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{
		Timeout: 3 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return resp.StatusCode, string(responseBody)
}

func TestNewNode(t *testing.T) {
	cfg := newTestConfig(t, "node1")

	n, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer n.Shutdown(context.Background())

	if n.db == nil {
		t.Fatal("expected initialized persistent store")
	}

	if n.server == nil {
		t.Fatal("expected initialized HTTP server")
	}

	if n.gate == nil {
		t.Fatal("expected initialized request gate")
	}

	if n.started {
		t.Fatal("new node must not already be started")
	}

	if n.stopped {
		t.Fatal("new node must not already be stopped")
	}

	if n.server.Addr != cfg.Address {
		t.Fatalf(
			"expected address %q, got %q",
			cfg.Address,
			n.server.Addr,
		)
	}
}

func TestNewNodeRejectsInvalidConfig(t *testing.T) {
	cfg := config.Config{
		NodeID:  "",
		Address: "127.0.0.1:8001",
		DataDir: t.TempDir(),
	}

	n, err := New(cfg)

	if err == nil {
		if n != nil {
			n.Shutdown(context.Background())
		}
		t.Fatal("expected invalid configuration to fail")
	}

	if n != nil {
		t.Fatal("expected nil node on validation failure")
	}
}

func TestNewNodeRejectsCorruptedWAL(t *testing.T) {
	cfg := newTestConfig(t, "node1")

	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := n.db.Set("key", "value"); err != nil {
		t.Fatal(err)
	}

	if err := n.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Replace the WAL header with invalid bytes.
	if err := os.WriteFile(cfg.WALPath(), []byte("INVALID!"), 0600); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(cfg)
	if err == nil {
		reopened.Shutdown(context.Background())
		t.Fatal("expected corrupted WAL to prevent initialization")
	}
}

func TestNodeStartAndHealthEndpoint(t *testing.T) {
	n := newTestNode(t, "node1")

	if err := n.Start(); err != nil {
		t.Fatalf("start node: %v", err)
	}

	status, body := httpRequest(
		t,
		http.MethodGet,
		nodeURL(n, "/health"),
		"",
	)

	if status != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", status)
	}

	if strings.TrimSpace(body) != "OK" {
		t.Fatalf("unexpected health response: %q", body)
	}
}

func TestNodeCRUD(t *testing.T) {
	n := newTestNode(t, "node1")

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	baseURL := nodeURL(n, "/kv/language")

	status, _ := httpRequest(
		t,
		http.MethodPut,
		baseURL,
		`{"value":"Go"}`,
	)

	if status < 200 || status >= 300 {
		t.Fatalf("PUT failed with status %d", status)
	}

	status, body := httpRequest(
		t,
		http.MethodGet,
		baseURL,
		"",
	)

	if status != http.StatusOK {
		t.Fatalf("expected GET 200, got %d", status)
	}

	if !strings.Contains(body, `"Go"`) {
		t.Fatalf("expected stored value, got %s", body)
	}

	status, _ = httpRequest(
		t,
		http.MethodDelete,
		baseURL,
		"",
	)

	if status != http.StatusNoContent {
		t.Fatalf("expected DELETE 204, got %d", status)
	}

	status, _ = httpRequest(
		t,
		http.MethodGet,
		baseURL,
		"",
	)

	if status != http.StatusNotFound {
		t.Fatalf("expected missing key to return 404, got %d", status)
	}
}

func TestNodeRejectsDoubleStart(t *testing.T) {
	n := newTestNode(t, "node1")

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	err := n.Start()

	if !errors.Is(err, ErrNodeStarted) {
		t.Fatalf("expected ErrNodeStarted, got %v", err)
	}
}

func TestNodeRejectsStartAfterShutdown(t *testing.T) {
	n := newTestNode(t, "node1")

	if err := n.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	err := n.Start()

	if !errors.Is(err, ErrNodeStopped) {
		t.Fatalf("expected ErrNodeStopped, got %v", err)
	}
}

func TestNodeShutdownIsIdempotent(t *testing.T) {
	n := newTestNode(t, "node1")

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	if err := n.Shutdown(context.Background()); err != nil {
		t.Fatalf("first shutdown failed: %v", err)
	}

	if err := n.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown failed: %v", err)
	}
}

func TestNodeShutdownBeforeStart(t *testing.T) {
	n := newTestNode(t, "node1")

	if err := n.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown before start failed: %v", err)
	}

	if err := n.Start(); !errors.Is(err, ErrNodeStopped) {
		t.Fatalf("expected ErrNodeStopped, got %v", err)
	}
}

func TestNodePortConflict(t *testing.T) {
	cfg := newTestConfig(t, "node1")

	// Reserve the configured port before attempting Start().
	blocker, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()

	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err == nil {
		n.Shutdown(context.Background())
		t.Fatal("expected port conflict")
	}

	// A failed Start() must release WAL ownership.
	reopened, err := New(cfg)
	if err != nil {
		t.Fatalf("WAL lock was not released: %v", err)
	}

	if err := reopened.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNodeRecoveryAfterRestart(t *testing.T) {
	cfg := newTestConfig(t, "node1")

	first, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := first.Start(); err != nil {
		t.Fatal(err)
	}

	status, _ := httpRequest(
		t,
		http.MethodPut,
		nodeURL(first, "/kv/language"),
		`{"value":"Go"}`,
	)
	if status < 200 || status >= 300 {
		t.Fatalf("PUT failed: %d", status)
	}

	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	second, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen node: %v", err)
	}
	defer second.Shutdown(context.Background())

	if err := second.Start(); err != nil {
		t.Fatal(err)
	}

	status, body := httpRequest(
		t,
		http.MethodGet,
		nodeURL(second, "/kv/language"),
		"",
	)

	if status != http.StatusOK {
		t.Fatalf("expected recovered key, got %d", status)
	}

	if !strings.Contains(body, `"Go"`) {
		t.Fatalf("unexpected recovered value: %s", body)
	}
}

func TestIndependentNodes(t *testing.T) {
	node1 := newTestNode(t, "node1")
	node2 := newTestNode(t, "node2")
	node3 := newTestNode(t, "node3")

	for _, n := range []*Node{node1, node2, node3} {
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
	}

	for i, n := range []*Node{node1, node2, node3} {
		value := fmt.Sprintf("value-%d", i+1)

		status, _ := httpRequest(
			t,
			http.MethodPut,
			nodeURL(n, "/kv/shared"),
			fmt.Sprintf(`{"value":%q}`, value),
		)

		if status < 200 || status >= 300 {
			t.Fatalf("node %d PUT failed: %d", i+1, status)
		}
	}

	for i, n := range []*Node{node1, node2, node3} {
		status, body := httpRequest(
			t,
			http.MethodGet,
			nodeURL(n, "/kv/shared"),
			"",
		)

		expected := fmt.Sprintf("value-%d", i+1)

		if status != http.StatusOK {
			t.Fatalf("node %d GET returned %d", i+1, status)
		}

		if !strings.Contains(body, fmt.Sprintf("%q", expected)) {
			t.Fatalf(
				"node %d expected %q, got %s",
				i+1,
				expected,
				body,
			)
		}
	}
}

func TestNodeShutdownReleasesWALLock(t *testing.T) {
	cfg := newTestConfig(t, "node1")

	first, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := first.Start(); err != nil {
		t.Fatal(err)
	}

	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	second, err := New(cfg)
	if err != nil {
		t.Fatalf("expected WAL lock to be released: %v", err)
	}
	defer second.Shutdown(context.Background())
}
