package raft

import (
	"errors"
	"fmt"
	"math"
)

var ErrNotLeader = errors.New("raft node is not the leader")

// mergeLogEntries retains identical entries, and replaces the first conflicting
// uncommitted suffix with the leader's entries. Indexes start at one (no snapshots).
func mergeLogEntries(local, incoming []LogEntry, commitIndex uint64) ([]LogEntry, error) {
	out := append([]LogEntry(nil), local...)
	if len(incoming) == 0 {
		return out, nil
	}
	if incoming[0].Index == 0 {
		return nil, fmt.Errorf("%w: log starts at zero", ErrInvalidRaftState)
	}
	if err := validateRaftEntries(incoming, incoming[0].Index); err != nil {
		return nil, err
	}
	for i, entry := range incoming {
		if entry.Index > uint64(len(out))+1 {
			return nil, fmt.Errorf("%w: gap at index %d", ErrInvalidRaftState, entry.Index)
		}
		if entry.Index <= uint64(len(out)) {
			previous := out[entry.Index-1]
			if previous == entry {
				continue
			}
			if entry.Index <= commitIndex {
				return nil, ErrCommittedEntry
			}
			// Equal (index, term) entries must have equal commands in Raft.
			if previous.Term == entry.Term {
				return nil, fmt.Errorf("%w: different commands at identical index/term", ErrInvalidRaftState)
			}
			out = append(out[:entry.Index-1], incoming[i:]...)
			return out, nil
		}
		out = append(out, incoming[i:]...)
		return out, nil
	}
	return out, nil
}

// evaluateAppendEntries implements follower log matching, replacement, and
// commit advancement. It returns only the suffix needing durable storage.
func evaluateAppendEntries(current raftState, request AppendEntriesRequest) (raftState, AppendEntriesResponse, []LogEntry, error) {
	next := cloneRaftState(current)
	reject := func() AppendEntriesResponse { return AppendEntriesResponse{Term: next.currentTerm} }
	if request.Term < current.currentTerm {
		return next, reject(), nil, nil
	}
	if request.Term > current.currentTerm {
		next.currentTerm = request.Term
		next.votedFor = ""
	}
	next.role = Follower
	next.electionVotes = nil

	last := uint64(len(next.log))
	if request.PrevLogIndex > last {
		res := reject()
		res.ConflictIndex = last + 1
		return next, res, nil, nil
	}
	if request.PrevLogIndex == 0 {
		if request.PrevLogTerm != 0 {
			return next, reject(), nil, nil
		}
	} else if next.log[request.PrevLogIndex-1].Term != request.PrevLogTerm {
		term := next.log[request.PrevLogIndex-1].Term
		first := request.PrevLogIndex
		for first > 1 && next.log[first-2].Term == term {
			first--
		}
		res := reject()
		res.ConflictTerm, res.ConflictIndex = term, first
		return next, res, nil, nil
	}
	if len(request.Entries) > 0 {
		if request.PrevLogIndex == math.MaxUint64 || request.Entries[0].Index != request.PrevLogIndex+1 {
			return next, reject(), nil, fmt.Errorf("%w: noncontiguous AppendEntries", ErrInvalidRaftState)
		}
		if err := validateRaftEntries(request.Entries, request.PrevLogIndex+1); err != nil {
			return next, reject(), nil, err
		}
	}
	merged, err := mergeLogEntries(next.log, request.Entries, next.commitIndex)
	if err != nil {
		if errors.Is(err, ErrCommittedEntry) {
			return next, reject(), nil, nil
		}
		return next, reject(), nil, err
	}
	var changed []LogEntry
	for i := range merged {
		if i >= len(next.log) || merged[i] != next.log[i] {
			changed = append([]LogEntry(nil), merged[i:]...)
			break
		}
	}
	next.log = merged
	match := request.PrevLogIndex + uint64(len(request.Entries))
	// A follower's unrelated trailing suffix cannot justify committing entries.
	if limit := min(request.LeaderCommit, match); limit > next.commitIndex {
		next.commitIndex = limit
	}
	return next, AppendEntriesResponse{Term: next.currentTerm, Success: true, MatchIndex: match}, changed, nil
}

// HandleAppendEntries durably saves a new term, log suffix and/or commit index
// before returning a successful RPC response.
func (r *Raft) HandleAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, Effects, error) {
	if req.LeaderID == "" {
		return AppendEntriesResponse{}, Effects{}, fmt.Errorf("%w: missing leader", ErrInvalidEvent)
	}
	var response AppendEntriesResponse
	effects, err := r.processTransition(func(current raftState) (Transition, error) {
		if current.role == Leader && req.Term == current.currentTerm && req.LeaderID != r.id {
			return Transition{}, fmt.Errorf("%w: competing leader in current term", ErrInvalidTransition)
		}
		next, reply, changed, err := evaluateAppendEntries(current, req)
		if err != nil {
			return Transition{}, err
		}
		response = reply
		t := Transition{next: next}
		if next.currentTerm != current.currentTerm || next.votedFor != current.votedFor || next.commitIndex != current.commitIndex || len(changed) > 0 {
			p := &PersistenceEffect{Entries: changed}
			if next.currentTerm != current.currentTerm || next.votedFor != current.votedFor || next.commitIndex != current.commitIndex {
				p.HardState = &HardState{CurrentTerm: next.currentTerm, VotedFor: next.votedFor, CommitIndex: next.commitIndex}
			}
			t.effects.Persist = p
		}
		if req.Term >= current.currentTerm && req.LeaderID != r.id {
			t.effects.ResetElectionTimer = true
		}
		return t, nil
	})
	if err != nil {
		return AppendEntriesResponse{}, Effects{}, err
	}
	return response, effects, nil
}

const maxEntriesPerRPC = 16

// appendForPeer builds an AppendEntries RPC from one immutable leader snapshot.
func appendForPeer(state raftState, peer, leaderID string) OutboundMessage {
	next := state.nextIndex[peer]
	if next == 0 {
		next = uint64(len(state.log)) + 1
	}
	if next > uint64(len(state.log))+1 {
		next = uint64(len(state.log)) + 1
	}
	prev := next - 1
	prevTerm := uint64(0)
	if prev > 0 {
		prevTerm = state.log[prev-1].Term
	}
	entries := append([]LogEntry(nil), state.log[prev:]...)
	if len(entries) > maxEntriesPerRPC {
		entries = entries[:maxEntriesPerRPC]
	}
	return OutboundMessage{
		To: peer, Type: MessageAppendEntries, RequestID: state.currentTerm,
		AppendEntries: &AppendEntriesRequest{
			Term: state.currentTerm, LeaderID: leaderID,
			PrevLogIndex: prev, PrevLogTerm: prevTerm,
			Entries: entries, LeaderCommit: state.commitIndex,
		},
	}
}

// replicateToPeers returns one bounded AppendEntries message per follower.
func replicateToPeers(state raftState, leaderID string, peers []string) Effects {
	var effects Effects
	for _, peer := range peers {
		effects.Messages = append(effects.Messages, appendForPeer(state, peer, leaderID))
	}
	return effects
}

// appendLeaderNoop appends the required current-term NOOP on leader promotion.
func appendLeaderNoop(next *raftState, self string, peers []string, result *Transition) {
	last := uint64(len(next.log))
	next.nextIndex = make(map[string]uint64, len(peers))
	next.matchIndex = make(map[string]uint64, len(peers))
	for _, peer := range peers {
		next.nextIndex[peer] = last + 1
	}
	entry := LogEntry{Index: last + 1, Term: next.currentTerm, Command: Command{Type: CommandNoop}}
	next.log = append(next.log, entry)
	if len(peers) == 0 {
		next.commitIndex = entry.Index
	}
	result.next = *next
	result.effects.Persist = &PersistenceEffect{
		HardState: &HardState{CurrentTerm: next.currentTerm, VotedFor: next.votedFor, CommitIndex: next.commitIndex},
		Entries:   []LogEntry{entry},
	}
	result.effects.ResetHeartbeatTimer = true
	result.effects.Messages = replicateToPeers(*next, self, peers).Messages
}

// appendClientCommand durably proposes a new command without reporting success
// until majority commitment (enforced by Runtime.Propose).
func (r *Raft) appendClientCommand(cmd Command, peers []string) (uint64, uint64, Effects, error) {
	if len(cmd.Key)+len(cmd.Value) > 64<<10 {
		return 0, 0, Effects{}, fmt.Errorf("%w: oversized command", ErrInvalidEvent)
	}
	if err := validateRaftCommand(cmd); err != nil {
		return 0, 0, Effects{}, err
	}
	var index, term uint64
	effects, err := r.processTransition(func(current raftState) (Transition, error) {
		if current.role != Leader {
			return Transition{}, ErrNotLeader
		}
		next := cloneRaftState(current)
		entry := LogEntry{Index: uint64(len(next.log)) + 1, Term: next.currentTerm, Command: cmd}
		next.log = append(next.log, entry)
		if len(peers) == 0 {
			next.commitIndex = entry.Index
		}
		index, term = entry.Index, entry.Term
		t := Transition{next: next}
		t.effects.Persist = &PersistenceEffect{Entries: []LogEntry{entry}}
		if len(peers) == 0 {
			t.effects.Persist.HardState = &HardState{CurrentTerm: next.currentTerm, VotedFor: next.votedFor, CommitIndex: next.commitIndex}
		}
		t.effects.Messages = replicateToPeers(next, r.id, peers).Messages
		return t, nil
	})
	return index, term, effects, err
}

// advanceLeaderCommit advances only current-term entries replicated by a majority.
func advanceLeaderCommit(next *raftState, peers []string) bool {
	if next.role != Leader {
		return false
	}
	for n := uint64(len(next.log)); n > next.commitIndex; n-- {
		if next.log[n-1].Term != next.currentTerm {
			continue
		}
		votes := 1 // leader itself
		for _, peer := range peers {
			if next.matchIndex[peer] >= n {
				votes++
			}
		}
		if votes > (len(peers)+1)/2 {
			next.commitIndex = n
			return true
		}
	}
	return false
}

// handleAppendEntriesResponse handles stale acknowledgments, conflict retries,
// and quorum-commit advancement. A request's sent prev/last index are supplied
// by the local transport, never the untrusted remote response.

func (r *Raft) handleAppendEntriesResponse(event Event, peers []string) (Effects, error) {
	if !isElectionPeer(event.From, peers) {
		return Effects{}, fmt.Errorf("%w: unknown peer", ErrInvalidEvent)
	}

	res := *event.AppendEntriesResponse

	return r.processTransition(func(current raftState) (Transition, error) {
		next := cloneRaftState(current)
		t := Transition{next: next}

		if res.Term > current.currentTerm {
			next.currentTerm, next.role, next.votedFor = res.Term, Follower, ""
			next.electionVotes = nil
			t.next = next
			t.effects.Persist = &PersistenceEffect{
				HardState: &HardState{
					CurrentTerm: next.currentTerm,
					VotedFor:    "",
					CommitIndex: next.commitIndex,
				},
			}
			t.effects.ResetElectionTimer = true
			return t, nil
		}

		if current.role != Leader ||
			event.RequestID != current.currentTerm ||
			res.Term != current.currentTerm {
			return t, nil
		}

		if event.SentPrevLogIndex > uint64(len(next.log)) ||
			event.SentLastIndex > uint64(len(next.log)) ||
			event.SentLastIndex < event.SentPrevLogIndex {
			return t, nil
		}

		if res.Success {
			if res.MatchIndex != event.SentLastIndex {
				return t, nil
			}

			if res.MatchIndex > next.matchIndex[event.From] {
				next.matchIndex[event.From] = res.MatchIndex
			}

			if next.nextIndex[event.From] < res.MatchIndex+1 {
				next.nextIndex[event.From] = res.MatchIndex + 1
			}

			if advanceLeaderCommit(&next, peers) {
				t.effects.Persist = &PersistenceEffect{
					HardState: &HardState{
						CurrentTerm: next.currentTerm,
						VotedFor:    next.votedFor,
						CommitIndex: next.commitIndex,
					},
				}
			}
		} else {
			// A delayed rejection must not undo newer acknowledged progress.
			if next.nextIndex[event.From] != event.SentPrevLogIndex+1 ||
				next.matchIndex[event.From] >= event.SentPrevLogIndex {
				return t, nil
			}

			target := res.ConflictIndex

			if res.ConflictTerm != 0 {
				// Find the leader's last entry of the conflicting term.
				found := false

				for i := len(next.log) - 1; i >= 0; i-- {
					if next.log[i].Term == res.ConflictTerm {
						target = uint64(i) + 2
						found = true
						break
					}
				}

				if !found {
					target = res.ConflictIndex
				}
			}

			if target == 0 || target >= next.nextIndex[event.From] {
				target = max(uint64(1), next.nextIndex[event.From]-1)
			}

			next.nextIndex[event.From] = target
		}

		t.next = next

		// FIX: Do not immediately send another heartbeat after
		// a successful acknowledgment from an up-to-date follower.
		//
		// Retry rejected requests or continue sending entries to
		// lagging followers. Regular heartbeats are timer-driven.
		if !res.Success ||
			next.nextIndex[event.From] <= uint64(len(next.log)) {
			t.effects.Messages = append(
				t.effects.Messages,
				appendForPeer(next, event.From, r.id),
			)
		}

		return t, nil
	})
}
