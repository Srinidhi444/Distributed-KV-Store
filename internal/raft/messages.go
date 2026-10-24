package raft

// RequestVoteRequest is sent by a candidate
// requesting a vote during leader election.
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteResponse contains the receiving
// node's term and voting decision.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesRequest is used both for log
// replication and leader heartbeats.
type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// AppendEntriesResponse reports whether
// the follower accepted the request.
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`

	// Useful for identifying how far a follower
	// has replicated the leader's log.
	MatchIndex uint64 `json:"match_index"`

	// Optional conflict hints for efficient
	// replication retries.
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
}
