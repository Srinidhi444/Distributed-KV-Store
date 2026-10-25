package raft

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
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
		kvMu    sync.Mutex
		kv      map[string]string
		server  *httptest.Server
		active  atomic.Pointer[Runtime]
		raft    *Raft
		storage *WALStorage
		queue   *EffectQueue
		runtime *Runtime
	}

	members := make([]*member, n)

	for i := range members {
		m := &member{
			id:   fmt.Sprintf("node%d", i+1),
			path: filepath.Join(t.TempDir(), fmt.Sprintf("node%d.raft", i+1)),
			kv:   make(map[string]string),
		}
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

		if err := rt.SetApplyFunc(func(entry LogEntry) error {
			m.kvMu.Lock()
			defer m.kvMu.Unlock()

			switch entry.Command.Type {
			case CommandSet:
				m.kv[entry.Command.Key] = entry.Command.Value
			case CommandDelete:
				delete(m.kv, entry.Command.Key)
			}

			return nil
		}); err != nil {
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

	// Wait until exactly one live leader exists.
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

	// Wait for continuous stable leadership, rather than assuming
	// the first observed leader remains leader during startup.
	waitStableLeader := func() *member {
		t.Helper()

		deadline := time.Now().Add(12 * time.Second)

		var stableCandidate *member
		var stableTerm uint64
		var stableSince time.Time

		for time.Now().Before(deadline) {
			var candidate *member
			leaderCount := 0

			for _, m := range members {
				if m.raft == nil {
					continue
				}

				if m.raft.Role() == Leader {
					candidate = m
					leaderCount++
				}
			}

			consistent := leaderCount == 1
			var term uint64

			if consistent {
				term = candidate.raft.CurrentTerm()

				// Verify the candidate is still leader and all
				// other nodes are followers in the same term.
				if candidate.raft.Role() != Leader {
					consistent = false
				}

				for _, m := range members {
					if !consistent {
						break
					}

					if m == candidate {
						continue
					}

					if m.raft == nil ||
						m.raft.Role() != Follower ||
						m.raft.CurrentTerm() != term {
						consistent = false
						break
					}
				}
			}

			if consistent {
				if stableCandidate != candidate ||
					stableTerm != term ||
					stableSince.IsZero() {

					stableCandidate = candidate
					stableTerm = term
					stableSince = time.Now()
				} else if time.Since(stableSince) >= 1200*time.Millisecond {
					t.Logf(
						"stable leader: %s (term %d)",
						stableCandidate.id,
						stableTerm,
					)

					return stableCandidate
				}
			} else {
				// Any leadership change or term mismatch resets
				// the continuous stability observation window.
				stableCandidate = nil
				stableSince = time.Time{}
			}

			time.Sleep(25 * time.Millisecond)
		}

		for _, m := range members {
			if m.raft == nil {
				t.Logf("%s: offline", m.id)
				continue
			}

			t.Logf(
				"%s role=%v term=%d",
				m.id,
				m.raft.Role(),
				m.raft.CurrentTerm(),
			)
		}

		t.Fatal("no leader stayed stable for 1.2 seconds within 12 seconds")
		return nil
	}

	// Phase 1: Ensure elections settle and heartbeats maintain
	// a stable leader before proceeding.
	first := waitStableLeader()

	// Phase 2: Persist, replicate, commit, and apply a real command.
	writeCtx, writeCancel := context.WithTimeout(
		context.Background(),
		8*time.Second,
	)

	index, err := first.runtime.Propose(
		writeCtx,
		Command{
			Type:  CommandSet,
			Key:   "x",
			Value: "42",
		},
	)
	writeCancel()

	if err != nil {
		t.Fatalf("replicated SET failed: %v", err)
	}

	if index == 0 {
		t.Fatal("invalid committed index")
	}

	deadlineKV := time.Now().Add(8 * time.Second)

	for time.Now().Before(deadlineKV) {
		matched := 0

		for _, m := range members {
			m.kvMu.Lock()
			if m.kv["x"] == "42" {
				matched++
			}
			m.kvMu.Unlock()
		}

		if matched == 3 {
			break
		}

		time.Sleep(25 * time.Millisecond)
	}

	for _, m := range members {
		m.kvMu.Lock()
		value := m.kv["x"]
		m.kvMu.Unlock()

		if value != "42" {
			t.Fatalf(
				"%s did not apply committed SET: %q",
				m.id,
				value,
			)
		}
	}

	// Phase 3: A quorum-backed read must observe the committed value.
	readCtx, readCancel := context.WithTimeout(
		context.Background(),
		8*time.Second,
	)

	value, found, readErr := first.runtime.LinearizableRead(
		readCtx,
		func() (string, bool) {
			first.kvMu.Lock()
			defer first.kvMu.Unlock()

			v, ok := first.kv["x"]
			return v, ok
		},
	)
	readCancel()

	if readErr != nil || !found || value != "42" {
		t.Fatalf(
			"linearizable read: value %q, found=%v, err=%v",
			value,
			found,
			readErr,
		)
	}

	// Phase 4: Stop the leader and elect a replacement.
	oldTerm := first.raft.CurrentTerm()

	stop(first)

	replacement := waitLeader(first)

	if replacement.raft.CurrentTerm() <= oldTerm {
		t.Fatalf(
			"replacement term %d did not exceed old term %d",
			replacement.raft.CurrentTerm(),
			oldTerm,
		)
	}

	t.Logf(
		"replacement leader: %s (term %d)",
		replacement.id,
		replacement.raft.CurrentTerm(),
	)

	// Phase 5: Commit a write through the replacement leader.
	// The original leader must catch up after restart.
	failoverCtx, failoverCancel := context.WithTimeout(
		context.Background(),
		8*time.Second,
	)

	_, err = replacement.runtime.Propose(
		failoverCtx,
		Command{
			Type:  CommandSet,
			Key:   "after",
			Value: "failover",
		},
	)
	failoverCancel()

	if err != nil {
		t.Fatalf("replacement leader write: %v", err)
	}

	// Phase 6: Restart the original leader using its existing WAL.
	start(first)

	deadlineCatchup := time.Now().Add(8 * time.Second)

	for time.Now().Before(deadlineCatchup) {
		first.kvMu.Lock()
		got := first.kv["after"]
		first.kvMu.Unlock()

		if got == "failover" {
			break
		}

		time.Sleep(25 * time.Millisecond)
	}

	first.kvMu.Lock()
	caughtUp := first.kv["after"] == "failover"
	first.kvMu.Unlock()

	if !caughtUp {
		t.Fatal("restarted previous leader failed to replicate new leader's SET")
	}

	// The restarted node must learn the newer term and remain a follower.
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		if first.raft.Role() == Follower &&
			first.raft.CurrentTerm() >= replacement.raft.CurrentTerm() {
			break
		}

		time.Sleep(25 * time.Millisecond)
	}

	if first.raft.Role() != Follower {
		t.Fatalf(
			"restarted %s is %v, expected follower",
			first.id,
			first.raft.Role(),
		)
	}

	if first.raft.CurrentTerm() < replacement.raft.CurrentTerm() {
		t.Fatal("restarted node has stale term")
	}

	t.Logf(
		"restarted %s caught up to term %d",
		first.id,
		first.raft.CurrentTerm(),
	)
}
