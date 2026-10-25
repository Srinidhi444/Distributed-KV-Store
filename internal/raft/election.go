package raft

import "fmt"

// isCandidateLogUpToDate reports whether a candidate's
// log is at least as up-to-date as the local log.
//
// Raft compares the last log term first.
// The index is used only when the terms are equal.
func isCandidateLogUpToDate(
	candidateLastTerm uint64,
	candidateLastIndex uint64,
	localLastTerm uint64,
	localLastIndex uint64,
) bool {
	if candidateLastTerm != localLastTerm {
		return candidateLastTerm > localLastTerm
	}

	return candidateLastIndex >= localLastIndex
}

// lastLogInfo returns the index and term of the
// final entry in the log.
//
// An empty log has index 0 and term 0.
func lastLogInfo(entries []LogEntry) (index, term uint64) {
	// Implement this.
	length := len(entries)
	if length == 0 {
		return 0, 0
	}
	latestindex := entries[length-1].Index
	latestterm := entries[length-1].Term
	return latestindex, latestterm

}

func evaluateRequestVote(
	current raftState,
	request RequestVoteRequest,
) (raftState, RequestVoteResponse) {
	next := cloneRaftState(current)

	if request.Term < next.currentTerm {
		return next, RequestVoteResponse{
			Term:        next.currentTerm,
			VoteGranted: false,
		}
	}

	if request.Term > next.currentTerm {
		next.currentTerm = request.Term
		next.role = Follower
		next.votedFor = ""
		next.electionVotes = nil
	}

	index, term := lastLogInfo(next.log)

	upToDate := isCandidateLogUpToDate(
		request.LastLogTerm,
		request.LastLogIndex,
		term,
		index,
	)

	canVote := next.votedFor == "" ||
		next.votedFor == request.CandidateID

	if upToDate && canVote {
		next.votedFor = request.CandidateID
		return next, RequestVoteResponse{
			Term:        next.currentTerm,
			VoteGranted: true,
		}
	}

	return next, RequestVoteResponse{
		Term:        next.currentTerm,
		VoteGranted: false,
	}
}

// evaluateHeartbeat processes an empty AppendEntries request.
//
// It validates the leader's term, checks log consistency,
// and advances commitIndex when appropriate.
//
// This function only computes the next state.
// Persistence and election timer resets are handled
// by the Raft transition engine.
func evaluateHeartbeat(
	current raftState,
	request AppendEntriesRequest,
) (raftState, AppendEntriesResponse) {
	next := cloneRaftState(current)

	// 1. Reject requests from older terms.
	if request.Term < current.currentTerm {
		return next, AppendEntriesResponse{
			Term:    next.currentTerm,
			Success: false,
		}
	}

	// 2. Recognize a newer term.
	if request.Term > current.currentTerm {
		next.currentTerm = request.Term
		next.votedFor = ""
		next.electionVotes = nil
	}

	// 3. A valid leader's AppendEntries causes the
	// receiver to become a follower.
	next.role = Follower
	next.electionVotes = nil

	// 4. Check whether the previous log entry matches.
	if request.PrevLogIndex == 0 {
		if request.PrevLogTerm != 0 {
			return next, AppendEntriesResponse{
				Term:    next.currentTerm,
				Success: false,
			}
		}
	} else {
		// Our current log starts at index 1 and has
		// no snapshots or compacted entries.
		if request.PrevLogIndex > uint64(len(next.log)) {
			return next, AppendEntriesResponse{
				Term:          next.currentTerm,
				Success:       false,
				ConflictIndex: uint64(len(next.log)) + 1,
			}
		}

		previous := next.log[request.PrevLogIndex-1]

		if previous.Term != request.PrevLogTerm {
			return next, AppendEntriesResponse{
				Term:          next.currentTerm,
				Success:       false,
				ConflictTerm:  previous.Term,
				ConflictIndex: previous.Index,
			}
		}
	}

	// 5. Advance the commit index, but never beyond
	// the entries actually present on this follower.
	lastIndex, _ := lastLogInfo(next.log)

	if request.LeaderCommit > next.commitIndex {
		next.commitIndex = min(
			request.LeaderCommit,
			lastIndex,
		)
	}

	return next, AppendEntriesResponse{
		Term:       next.currentTerm,
		Success:    true,
		MatchIndex: request.PrevLogIndex,
	}
}

// HandleHeartbeat processes an empty AppendEntries RPC.
//
// It persists any term, vote, or commit-index changes
// before returning the response to the caller.
func (r *Raft) HandleHeartbeat(
	request AppendEntriesRequest,
) (AppendEntriesResponse, Effects, error) {
	if request.LeaderID == "" || len(request.Entries) != 0 {
		return AppendEntriesResponse{}, Effects{},
			fmt.Errorf("%w: invalid heartbeat", ErrInvalidEvent)
	}

	var response AppendEntriesResponse

	effects, err := r.processTransition(
		func(current raftState) (Transition, error) {
			next, result := evaluateHeartbeat(current, request)

			// Two legitimate leaders cannot exist in the
			// same term. Do not step down for a competing
			// same-term leader claim.
			if current.role == Leader &&
				request.Term == current.currentTerm &&
				request.LeaderID != r.id {
				return Transition{}, fmt.Errorf(
					"%w: competing leader in term %d",
					ErrInvalidTransition,
					current.currentTerm,
				)
			}

			response = result
			t := Transition{next: next}

			if next.currentTerm != current.currentTerm ||
				next.votedFor != current.votedFor ||
				next.commitIndex != current.commitIndex {
				t.effects.Persist = &PersistenceEffect{
					HardState: &HardState{
						CurrentTerm: next.currentTerm,
						VotedFor:    next.votedFor,
						CommitIndex: next.commitIndex,
					},
				}
			}

			// A recognized leader can reset our timer
			// even when the log consistency check fails.
			if request.Term >= current.currentTerm {
				t.effects.ResetElectionTimer = true
			}

			return t, nil
		},
	)

	if err != nil {
		return AppendEntriesResponse{}, Effects{}, err
	}

	return response, effects, nil
}
