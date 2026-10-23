package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/config"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/transport"
)

func clusterConfigs(t *testing.T) []config.Config {
	t.Helper()

	ids := []string{"node1", "node2", "node3"}

	addresses := make(map[string]string, len(ids))

	for _, id := range ids {
		addresses[id] = availableAddress(t)
	}

	configs := make([]config.Config, 0, len(ids))

	for _, id := range ids {
		peers := make(map[string]string)

		for peerID, address := range addresses {
			if peerID != id {
				peers[peerID] = address
			}
		}

		configs = append(configs, config.Config{
			NodeID:  id,
			Address: addresses[id],
			DataDir: filepath.Join(t.TempDir(), id),
			Peers:   peers,
		})
	}

	return configs
}

func startClusterNode(t *testing.T, cfg config.Config) *Node {
	t.Helper()

	n, err := New(cfg)
	if err != nil {
		t.Fatalf("create %s: %v", cfg.NodeID, err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := n.Shutdown(ctx); err != nil {
			t.Errorf("cleanup %s: %v", cfg.NodeID, err)
		}
	})

	if err := n.Start(); err != nil {
		t.Fatalf("start %s: %v", cfg.NodeID, err)
	}

	return n
}

func shutdownClusterNode(t *testing.T, n *Node) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := n.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown %s: %v", n.cfg.NodeID, err)
	}
}

func pingClusterPeer(
	t *testing.T,
	from *Node,
	peerID string,
) transport.PingResponse {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	resp, err := from.PingPeer(ctx, peerID)
	if err != nil {
		t.Fatalf(
			"%s failed to ping %s: %v",
			from.cfg.NodeID,
			peerID,
			err,
		)
	}

	if resp.NodeID != peerID {
		t.Fatalf(
			"expected response from %s, got %s",
			peerID,
			resp.NodeID,
		)
	}

	if resp.Message != "pong" {
		t.Fatalf("unexpected ping message: %q", resp.Message)
	}

	return resp
}

func TestThreeNodeClusterConnectivity(t *testing.T) {
	configs := clusterConfigs(t)

	nodes := make([]*Node, 0, len(configs))

	for _, cfg := range configs {
		nodes = append(nodes, startClusterNode(t, cfg))
	}

	// Every node must be able to contact every other node.
	for _, from := range nodes {
		for _, to := range nodes {
			if from.cfg.NodeID == to.cfg.NodeID {
				continue
			}

			pingClusterPeer(t, from, to.cfg.NodeID)
		}
	}
}

func TestThreeNodeClusterUnknownPeer(t *testing.T) {
	configs := clusterConfigs(t)

	node1 := startClusterNode(t, configs[0])

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	_, err := node1.PingPeer(ctx, "unknown-node")

	if !errors.Is(err, transport.ErrPeerNotFound) {
		t.Fatalf("expected ErrPeerNotFound, got %v", err)
	}
}

func TestThreeNodeClusterPeerFailureAndRestart(t *testing.T) {
	configs := clusterConfigs(t)

	node1 := startClusterNode(t, configs[0])
	node2 := startClusterNode(t, configs[1])
	node3 := startClusterNode(t, configs[2])

	// Verify initial connectivity.
	pingClusterPeer(t, node1, "node2")
	pingClusterPeer(t, node1, "node3")

	// Stop node2.
	shutdownClusterNode(t, node2)

	// Node1 should not be able to contact node2.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)

	_, err := node1.PingPeer(ctx, "node2")
	cancel()

	if err == nil {
		t.Fatal("expected ping to stopped node2 to fail")
	}

	if !errors.Is(err, transport.ErrPeerUnavailable) {
		t.Fatalf(
			"expected ErrPeerUnavailable, got %v",
			err,
		)
	}

	// Node1 and Node3 must still communicate.
	pingClusterPeer(t, node1, "node3")
	pingClusterPeer(t, node3, "node1")

	// Restart node2 with the same identity and data directory.
	restartedNode2 := startClusterNode(t, configs[1])

	// Connectivity should be restored.
	pingClusterPeer(t, node1, "node2")
	pingClusterPeer(t, restartedNode2, "node1")
	pingClusterPeer(t, restartedNode2, "node3")
}

func TestThreeNodeClusterStorageIsolation(t *testing.T) {
	configs := clusterConfigs(t)

	nodes := make([]*Node, 0, len(configs))

	for _, cfg := range configs {
		nodes = append(nodes, startClusterNode(t, cfg))
	}

	// Write a distinct value to each node through HTTP.
	for i, n := range nodes {
		value := fmt.Sprintf("value-%d", i+1)

		status, _ := httpRequest(
			t,
			http.MethodPut,
			nodeURL(n, "/kv/shared"),
			fmt.Sprintf(`{"value":%q}`, value),
		)

		if status < 200 || status >= 300 {
			t.Fatalf(
				"PUT to %s failed: HTTP %d",
				n.cfg.NodeID,
				status,
			)
		}
	}

	// Verify each node has its own value.
	for i, n := range nodes {
		expected := fmt.Sprintf("value-%d", i+1)

		value, exists := n.db.Get("shared")

		if !exists || value != expected {
			t.Fatalf(
				"%s: expected %q, got %q",
				n.cfg.NodeID,
				expected,
				value,
			)
		}
	}

	// Stop every node, releasing its WAL.
	for _, n := range nodes {
		shutdownClusterNode(t, n)
	}

	// Restart and verify independent recovery.
	for i, cfg := range configs {
		restarted := startClusterNode(t, cfg)

		expected := fmt.Sprintf("value-%d", i+1)

		value, exists := restarted.db.Get("shared")

		if !exists || value != expected {
			t.Fatalf(
				"%s: expected recovered value %q, got %q",
				cfg.NodeID,
				expected,
				value,
			)
		}
	}
}
