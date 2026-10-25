package raft

type EventType uint8

const (
	EventElectionTimeout EventType = iota
	EventHeartbeatTimeout
	EventRequestVote
	EventRequestVoteResponse
	EventAppendEntries
	EventAppendEntriesResponse
	EventClientProposal
)

// Event is one input to the Raft state machine.
//
// From identifies the authenticated/validated peer that
// supplied an RPC or response.
//
// RequestID correlates a response with an outstanding
// request in the local runtime.
type Event struct {
	Type EventType

	From      string
	RequestID uint64
	// Local correlation data for AppendEntries responses.
	SentPrevLogIndex uint64
	SentLastIndex    uint64

	RequestVote         *RequestVoteRequest
	RequestVoteResponse *RequestVoteResponse

	AppendEntries         *AppendEntriesRequest
	AppendEntriesResponse *AppendEntriesResponse

	Proposal *Command
}
