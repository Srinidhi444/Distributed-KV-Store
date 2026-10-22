package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/transport"
)

func TestNodeToNodePing(t *testing.T) {
	cfg1 := newTestConfig(t, "node1")
	cfg2 := newTestConfig(t, "node2")

	node1, err := New(cfg1)
	if err != nil {
		t.Fatal(err)
	}
	defer node1.Shutdown(context.Background())

	node2, err := New(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer node2.Shutdown(context.Background())

	if err := node1.Start(); err != nil {
		t.Fatal(err)
	}

	if err := node2.Start(); err != nil {
		t.Fatal(err)
	}

	client := transport.NewClient()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	resp, err := client.Ping(
		ctx,
		node2.listener.Addr().String(),
		transport.PingRequest{
			From: cfg1.NodeID,
		},
	)

	if err != nil {
		t.Fatalf("node1 failed to ping node2: %v", err)
	}

	if resp.NodeID != cfg2.NodeID {
		t.Fatalf(
			"expected response from %q, got %q",
			cfg2.NodeID,
			resp.NodeID,
		)
	}

	if resp.Message != "pong" {
		t.Fatalf("expected pong, got %q", resp.Message)
	}
}

func TestPingStoppedNode(t *testing.T) {
	cfg := newTestConfig(t, "node2")

	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	address := n.listener.Addr().String()

	if err := n.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	client := transport.NewClient()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	_, err = client.Ping(
		ctx,
		address,
		transport.PingRequest{From: "node1"},
	)

	if err == nil {
		t.Fatal("expected ping to stopped node to fail")
	}
}

func TestPingCancelledBeforeSending(t *testing.T) {
	n := newTestNode(t, "node2")

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := transport.NewClient()

	_, err := client.Ping(
		ctx,
		n.listener.Addr().String(),
		transport.PingRequest{From: "node1"},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
