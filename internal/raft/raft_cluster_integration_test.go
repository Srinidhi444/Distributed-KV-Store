package raft

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestElectionCluster exercises real RequestVote/AppendEntries HTTP traffic,
// leader failover, and restart from the same Raft WAL.
func TestElectionCluster(t *testing.T) {
	const n = 3
	type member struct {
		id      string
		path    string
		server  *httptest.Server
		active  atomic.Pointer[Runtime]
		raft    *Raft
		storage *WALStorage
		queue   *EffectQueue
		runtime *Runtime
	}
	members := make([]*member, n)
	for i := range members {
		m := &member{id: fmt.Sprintf("node%d", i+1), path: filepath.Join(t.TempDir(), fmt.Sprintf("node%d.raft", i+1))}
		members[i] = m
		m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rt := m.active.Load()
			if rt == nil {
				http.Error(w, "node offline", http.StatusServiceUnavailable)
				return
			}
			NewElectionHTTPHandler(rt).ServeHTTP(w, r)
		}))
		t.Cleanup(m.server.Close)
	}
	stop := func(m *member) {
		if m.runtime == nil {
			return
		}
		// Mark offline first: no new inbound RPCs can be admitted.
		m.active.Store(nil)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.runtime.Stop(ctx); err != nil {
			t.Fatalf("stop %s: %v", m.id, err)
		}
		if err := m.queue.Close(); err != nil {
			t.Fatalf("close effects %s: %v", m.id, err)
		}
		if err := m.storage.Close(); err != nil {
			t.Fatalf("close WAL %s: %v", m.id, err)
		}
		m.runtime, m.queue, m.storage, m.raft = nil, nil, nil, nil
	}
	start := func(m *member) {
		storage, err := OpenWALStorage(m.path)
		if err != nil {
			t.Fatalf("open WAL %s: %v", m.id, err)
		}
		raftNode, err := NewWithStorage(m.id, storage)
		if err != nil {
			storage.Close()
			t.Fatalf("load raft %s: %v", m.id, err)
		}
		peerIDs := make([]string, 0, n-1)
		peerURLs := make(map[string]string, n-1)
		for _, other := range members {
			if other != m {
				peerIDs = append(peerIDs, other.id)
				peerURLs[other.id] = other.server.URL
			}
		}
		rt, err := NewElectionRuntime(raftNode, peerIDs, 256)
		if err != nil {
			storage.Close()
			t.Fatal(err)
		}
		handler, err := NewElectionEffectHandler(rt, peerURLs)
		if err != nil {
			storage.Close()
			t.Fatal(err)
		}
		queue, err := NewEffectQueue(256, handler)
		if err != nil {
			storage.Close()
			t.Fatal(err)
		}
		if err := rt.SetEffectQueue(queue); err != nil {
			queue.Close()
			storage.Close()
			t.Fatal(err)
		}
		m.raft, m.storage, m.runtime, m.queue = raftNode, storage, rt, queue
		m.active.Store(rt)
		if err := rt.Start(); err != nil {
			m.active.Store(nil)
			queue.Close()
			storage.Close()
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, m := range members {
			stop(m)
		}
	})
	for _, m := range members {
		start(m)
	}

	// waitLeader requires exactly one live leader, not just the first to appear.
	waitLeader := func(excluded *member) *member {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var leader *member
			count := 0
			for _, m := range members {
				if m == excluded || m.raft == nil {
					continue
				}
				if m.raft.Role() == Leader {
					leader = m
					count++
				}
			}
			if count == 1 {
				return leader
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatal("no unique leader elected in 10 seconds")
		return nil
	}

	first := waitLeader(nil)
	t.Logf("initial leader: %s (term %d)", first.id, first.raft.CurrentTerm())
	time.Sleep(1200 * time.Millisecond) // several election timeout intervals
	if first.raft.Role() != Leader {
		t.Fatalf("leader %s lost leadership despite heartbeats", first.id)
	}
	for _, m := range members {
		if m != first && m.raft.Role() != Follower {
			t.Fatalf("%s isn't follower during stable leadership: %v", m.id, m.raft.Role())
		}
	}
	oldTerm := first.raft.CurrentTerm()
	stop(first)
	replacement := waitLeader(first)
	if replacement.raft.CurrentTerm() <= oldTerm {
		t.Fatalf("replacement term %d did not exceed old term %d", replacement.raft.CurrentTerm(), oldTerm)
	}
	t.Logf("replacement leader: %s (term %d)", replacement.id, replacement.raft.CurrentTerm())

	start(first)
	// The old leader must learn the newer term and remain follower.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if first.raft.Role() == Follower && first.raft.CurrentTerm() >= replacement.raft.CurrentTerm() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if first.raft.Role() != Follower {
		t.Fatalf("restarted %s is %v, expected follower", first.id, first.raft.Role())
	}
	if first.raft.CurrentTerm() < replacement.raft.CurrentTerm() {
		t.Fatalf("restarted node has stale term")
	}
	t.Logf("restarted %s caught up to term %d", first.id, first.raft.CurrentTerm())
}
