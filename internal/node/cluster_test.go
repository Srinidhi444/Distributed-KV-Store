package node

import (
	"context"
	"errors"
	"github.com/Srinidhi444/Distributed-KV-Store/internal/raft"
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

// TestThreeNodeClusterReplicatedWrites proves all three nodes share one committed log.
func TestThreeNodeClusterReplicatedWrites(t *testing.T) {
	configs := clusterConfigs(t)
	nodes := make([]*Node, 0, 3)
	for _, cfg := range configs {
		nodes = append(nodes, startClusterNode(t, cfg))
	}
	var leader *Node
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		leader = nil
		for _, n := range nodes {
			if n.raftNode.Role() == raft.Leader {
				count++
				leader = n
			}
		}
		if count == 1 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}
	status, _ := httpRequest(t, http.MethodPut, nodeURL(leader, "/kv/shared"), `{"value":"replicated"}`)
	if status != http.StatusOK {
		t.Fatalf("leader PUT returned %d", status)
	}
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		matched := 0
		for _, n := range nodes {
			if v, ok := n.db.Get("shared"); ok && v == "replicated" {
				matched++
			}
		}
		if matched == 3 {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("followers never applied committed value")
}

// TestClusterFailoverAndRestart checks public writes, leadership replacement,
// and WAL replay into the restarted node's state machine.
func TestClusterFailoverAndRestart(t *testing.T) {
	configs := clusterConfigs(t)
	nodes := make([]*Node, 0, 3)
	for _, cfg := range configs {
		nodes = append(nodes, startClusterNode(t, cfg))
	}
	waitUniqueLeader := func(except *Node) *Node {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var selected *Node
			count := 0
			for _, n := range nodes {
				if n != except && !n.stopped && n.raftNode.Role() == raft.Leader {
					selected = n
					count++
				}
			}
			if count == 1 {
				return selected
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatal("cluster failed to elect a unique leader")
		return nil
	}
	oldLeader := waitUniqueLeader(nil)
	status, _ := httpRequest(t, http.MethodPut, nodeURL(oldLeader, "/kv/pre"), `{"value":"before"}`)
	if status != http.StatusOK {
		t.Fatalf("initial write: HTTP %d", status)
	}
	shutdownClusterNode(t, oldLeader)
	leader := waitUniqueLeader(oldLeader)
	status, _ = httpRequest(t, http.MethodPut, nodeURL(leader, "/kv/post"), `{"value":"after"}`)
	if status != http.StatusOK {
		t.Fatalf("new leader write: HTTP %d", status)
	}
	var oldConfig config.Config
	for _, cfg := range configs {
		if cfg.NodeID == oldLeader.cfg.NodeID {
			oldConfig = cfg
			break
		}
	}
	restarted := startClusterNode(t, oldConfig)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		before, ok1 := restarted.db.Get("pre")
		after, ok2 := restarted.db.Get("post")
		if ok1 && ok2 && before == "before" && after == "after" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("restarted node did not replay and catch up with committed writes")
}
