package raft

// EventType identifies the kind of event received
// by the Raft consensus engine.
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

// Event represents one input to the Raft engine.
//
// Exactly one payload should be populated for events
// that require additional data.
type Event struct {
	Type EventType

	RequestVote         *RequestVoteRequest
	RequestVoteResponse *RequestVoteResponse

	AppendEntries         *AppendEntriesRequest
	AppendEntriesResponse *AppendEntriesResponse

	Proposal *Command
}
