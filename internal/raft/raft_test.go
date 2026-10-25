package raft

import (
	"sync"
	"testing"
)

func TestNewRaft(t *testing.T) {
	r := New("node1")

	if r == nil {
		t.Fatal("expected non-nil Raft instance")
	}

	if r.ID() != "node1" {
		t.Fatalf("expected node ID node1, got %q", r.ID())
	}

	if r.Role() != Follower {
		t.Fatalf("expected Follower role, got %v", r.Role())
	}

	if r.CurrentTerm() != 0 {
		t.Fatalf("expected initial term 0, got %d", r.CurrentTerm())
	}

	if r.votedFor != "" {
		t.Fatalf("expected no initial vote, got %q", r.votedFor)
	}

	if len(r.log) != 0 {
		t.Fatalf("expected empty log, got %d entries", len(r.log))
	}

	if r.commitIndex != 0 {
		t.Fatalf("expected commitIndex 0, got %d", r.commitIndex)
	}

	if r.lastApplied != 0 {
		t.Fatalf("expected lastApplied 0, got %d", r.lastApplied)
	}

	if r.nextIndex == nil {
		t.Fatal("nextIndex map must be initialized")
	}

	if r.matchIndex == nil {
		t.Fatal("matchIndex map must be initialized")
	}

	if len(r.nextIndex) != 0 {
		t.Fatalf("expected empty nextIndex map, got %d entries", len(r.nextIndex))
	}

	if len(r.matchIndex) != 0 {
		t.Fatalf("expected empty matchIndex map, got %d entries", len(r.matchIndex))
	}
}

func TestRaftInitialStateInvariant(t *testing.T) {
	r := New("node1")

	lastLogIndex := uint64(len(r.log))

	if r.lastApplied > r.commitIndex {
		t.Fatalf(
			"lastApplied (%d) exceeds commitIndex (%d)",
			r.lastApplied,
			r.commitIndex,
		)
	}

	if r.commitIndex > lastLogIndex {
		t.Fatalf(
			"commitIndex (%d) exceeds lastLogIndex (%d)",
			r.commitIndex,
			lastLogIndex,
		)
	}
}

func TestRaftRolesAreDistinct(t *testing.T) {
	roles := []Role{
		Follower,
		Candidate,
		Leader,
	}

	seen := make(map[Role]bool)

	for _, role := range roles {
		if seen[role] {
			t.Fatalf("duplicate role value: %d", role)
		}

		seen[role] = true
	}

	if len(seen) != 3 {
		t.Fatalf("expected 3 distinct roles, got %d", len(seen))
	}
}

func TestRaftCommandTypesAreDistinct(t *testing.T) {
	types := []CommandType{
		CommandNoop,
		CommandSet,
		CommandDelete,
	}

	seen := make(map[CommandType]bool)

	for _, commandType := range types {
		if seen[commandType] {
			t.Fatalf("duplicate command type: %d", commandType)
		}

		seen[commandType] = true
	}

	if len(seen) != 3 {
		t.Fatalf("expected 3 distinct command types, got %d", len(seen))
	}
}

func TestRaftAccessors(t *testing.T) {
	r := New("node1")

	if got := r.ID(); got != "node1" {
		t.Fatalf("expected node1, got %q", got)
	}

	if got := r.Role(); got != Follower {
		t.Fatalf("expected Follower, got %v", got)
	}

	if got := r.CurrentTerm(); got != 0 {
		t.Fatalf("expected term 0, got %d", got)
	}

	// Simulate state changes under the write lock.
	r.mu.Lock()
	r.role = Candidate
	r.currentTerm = 5
	r.mu.Unlock()

	if got := r.Role(); got != Candidate {
		t.Fatalf("expected Candidate, got %v", got)
	}

	if got := r.CurrentTerm(); got != 5 {
		t.Fatalf("expected term 5, got %d", got)
	}
}

func TestRaftConcurrentAccessors(t *testing.T) {
	r := New("node1")

	const readers = 100
	const iterations = 100

	var wg sync.WaitGroup

	// Concurrently update Raft state under the write lock.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 1; i <= iterations; i++ {
			r.mu.Lock()
			r.currentTerm = uint64(i)

			if i%2 == 0 {
				r.role = Candidate
			} else {
				r.role = Follower
			}

			r.mu.Unlock()
		}
	}()

	// Concurrently read state using public accessors.
	for i := 0; i < readers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				_ = r.ID()
				_ = r.Role()
				_ = r.CurrentTerm()
			}
		}()
	}

	wg.Wait()

	if got := r.CurrentTerm(); got != iterations {
		t.Fatalf(
			"expected final term %d, got %d",
			iterations,
			got,
		)
	}

	if got := r.Role(); got != Candidate {
		t.Fatalf("expected final role Candidate, got %v", got)
	}
}

// TestHeartbeatAckDoesNotTriggerAnotherHeartbeat prevents a response->request
// feedback loop that otherwise creates unbounded HTTP goroutines.
func TestHeartbeatAckDoesNotTriggerAnotherHeartbeat(t *testing.T) {
	r := New("node1")
	r.role = Leader
	r.currentTerm = 1
	r.log = []LogEntry{{Index: 1, Term: 1, Command: Command{Type: CommandNoop}}}
	r.commitIndex = 1
	r.matchIndex["node2"] = 1
	r.nextIndex["node2"] = 2

	effects, err := r.handleAppendEntriesResponse(Event{
		Type:                  EventAppendEntriesResponse,
		From:                  "node2",
		RequestID:             1,
		SentPrevLogIndex:      1,
		SentLastIndex:         1,
		AppendEntriesResponse: &AppendEntriesResponse{Term: 1, Success: true, MatchIndex: 1},
	}, []string{"node2", "node3"})
	if err != nil {
		t.Fatal(err)
	}
	if len(effects.Messages) != 0 {
		t.Fatalf("up-to-date follower caused %d redundant RPCs", len(effects.Messages))
	}
}
