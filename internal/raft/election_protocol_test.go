package raft

import (
	"errors"
	"fmt"
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

// TestMergeLogEntries exercises append, identical-prefix, conflict and
// committed-prefix safety using real entry payloads.
func TestMergeLogEntries(t *testing.T) {
	mk := func(index, term uint64) LogEntry {
		return LogEntry{Index: index, Term: term, Command: Command{Type: CommandSet, Key: "k", Value: fmt.Sprint(term)}}
	}
	initial := []LogEntry{mk(1, 1), mk(2, 1), mk(3, 2), mk(4, 2), mk(5, 2)}
	incoming := []LogEntry{mk(4, 3), mk(5, 3)}
	result, err := mergeLogEntries(initial, incoming, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 5 || result[3].Term != 3 || result[4].Term != 3 || initial[3].Term != 2 {
		t.Fatalf("bad logical suffix replacement: %v", result)
	}
	if _, err := mergeLogEntries(initial, []LogEntry{mk(3, 3)}, 3); !errors.Is(err, ErrCommittedEntry) {
		t.Fatalf("expected committed overwrite error, got %v", err)
	}
	same, err := mergeLogEntries(initial, []LogEntry{mk(3, 2), mk(4, 2)}, 2)
	if err != nil || len(same) != 5 || same[4] != initial[4] {
		t.Fatalf("identical prefix truncated trailing entries: %v, %v", same, err)
	}
	if _, err := mergeLogEntries(initial, []LogEntry{mk(7, 4)}, 2); err == nil {
		t.Fatal("accepted a gap")
	}
	altered := mk(4, 2)
	altered.Command.Value = "evil"
	if _, err := mergeLogEntries(initial, []LogEntry{altered}, 3); err == nil {
		t.Fatal("accepted different command for identical index and term")
	}
}

// TestAppendEntriesRejectsDivergentCommit prevents commitment of a follower's
// trailing entries that were not proven to match the leader.
func TestAppendEntriesRejectsDivergentCommit(t *testing.T) {
	current := raftState{role: Follower, currentTerm: 3, log: []LogEntry{
		{Index: 1, Term: 1, Command: Command{Type: CommandNoop}},
		{Index: 2, Term: 2, Command: Command{Type: CommandNoop}},
		{Index: 3, Term: 2, Command: Command{Type: CommandNoop}},
	}}
	next, resp, _, err := evaluateAppendEntries(current, AppendEntriesRequest{Term: 3, LeaderID: "node2", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 3})
	if err != nil || !resp.Success || next.commitIndex != 1 {
		t.Fatalf("advanced commit beyond proven prefix: %v, %+v, %v", next.commitIndex, resp, err)
	}
}

// TestCommitCurrentTermOnly verifies that prior-term entries are never
// committed by replica counting alone.
func TestCommitCurrentTermOnly(t *testing.T) {
	entries := []LogEntry{
		{Index: 1, Term: 1, Command: Command{Type: CommandNoop}},
		{Index: 2, Term: 2, Command: Command{Type: CommandNoop}},
	}
	s := raftState{role: Leader, currentTerm: 3, log: entries, matchIndex: map[string]uint64{"n2": 2, "n3": 1}}
	if advanceLeaderCommit(&s, []string{"n2", "n3"}) || s.commitIndex != 0 {
		t.Fatal("incorrectly committed prior-term entry")
	}
	s.log = append(s.log, LogEntry{Index: 3, Term: 3, Command: Command{Type: CommandNoop}})
	s.matchIndex["n2"] = 3
	if !advanceLeaderCommit(&s, []string{"n2", "n3"}) || s.commitIndex != 3 {
		t.Fatal("failed to commit current-term entry")
	}
}
