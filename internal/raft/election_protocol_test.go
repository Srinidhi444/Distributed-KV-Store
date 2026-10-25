package raft

import (
	"path/filepath"
	"testing"
)

func newElectionTestRaft(t *testing.T) *Raft {
	t.Helper()

	path := filepath.Join(t.TempDir(), "raft.wal")

	storage, err := OpenWALStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := storage.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	})

	r, err := NewWithStorage("node1", storage)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestStartElectionPersistsSelfVote(t *testing.T) {
	r := newElectionTestRaft(t)

	effects, err := r.StartElection([]string{"node2", "node3"})
	if err != nil {
		t.Fatal(err)
	}

	if r.Role() != Candidate {
		t.Fatalf("expected Candidate, got %v", r.Role())
	}
	if r.CurrentTerm() != 1 {
		t.Fatalf("expected term 1, got %d", r.CurrentTerm())
	}
	if len(effects.Messages) != 2 {
		t.Fatalf("expected 2 vote requests, got %d", len(effects.Messages))
	}

	state, err := r.storage.Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState.CurrentTerm != 1 ||
		state.HardState.VotedFor != "node1" {
		t.Fatalf("self-vote not persisted: %+v", state.HardState)
	}
}

func TestElectionMajorityPromotesLeader(t *testing.T) {
	r := newElectionTestRaft(t)
	peers := []string{"node2", "node3"}

	if _, err := r.StartElection(peers); err != nil {
		t.Fatal(err)
	}

	_, err := r.HandleRequestVoteResponse(
		"node2",
		1,
		RequestVoteResponse{Term: 1, VoteGranted: true},
		peers,
	)
	if err != nil {
		t.Fatal(err)
	}

	if r.Role() != Leader {
		t.Fatalf("expected Leader, got %v", r.Role())
	}
}

func TestStaleVotesCannotElectLeader(t *testing.T) {
	r := newElectionTestRaft(t)
	peers := []string{"node2", "node3"}

	if _, err := r.StartElection(peers); err != nil {
		t.Fatal(err)
	}
	if _, err := r.StartElection(peers); err != nil {
		t.Fatal(err)
	}

	_, err := r.HandleRequestVoteResponse(
		"node2",
		1,
		RequestVoteResponse{Term: 1, VoteGranted: true},
		peers,
	)
	if err != nil {
		t.Fatal(err)
	}

	if r.Role() != Candidate {
		t.Fatalf("stale vote incorrectly elected a leader")
	}
	if r.CurrentTerm() != 2 {
		t.Fatalf("expected term 2, got %d", r.CurrentTerm())
	}
}

func TestHigherTermResponseStepsDown(t *testing.T) {
	r := newElectionTestRaft(t)
	peers := []string{"node2", "node3"}

	if _, err := r.StartElection(peers); err != nil {
		t.Fatal(err)
	}

	_, err := r.HandleRequestVoteResponse(
		"node2",
		1,
		RequestVoteResponse{Term: 5, VoteGranted: false},
		peers,
	)
	if err != nil {
		t.Fatal(err)
	}

	if r.Role() != Follower || r.CurrentTerm() != 5 {
		t.Fatalf(
			"expected follower at term 5, got role=%v term=%d",
			r.Role(),
			r.CurrentTerm(),
		)
	}

	state, err := r.storage.Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState.CurrentTerm != 5 {
		t.Fatalf("higher term not persisted")
	}
}
