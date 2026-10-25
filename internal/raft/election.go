package raft

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
