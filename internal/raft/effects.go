package raft

// MessageType identifies an outgoing Raft RPC.
type MessageType uint8

const (
	MessageRequestVote MessageType = iota
	MessageAppendEntries
)

// OutboundMessage represents a message to send after
// the required durable state has been persisted.
//
// Exactly one request or response payload must match Type.
type OutboundMessage struct {
	To   string
	Type MessageType

	RequestVote         *RequestVoteRequest
	RequestVoteResponse *RequestVoteResponse

	AppendEntries         *AppendEntriesRequest
	AppendEntriesResponse *AppendEntriesResponse

	// Correlation metadata, assigned by the runtime.
	RequestID uint64
}

// PersistenceEffect describes one coordinated durable update.
//
// Nil HardState means that the stored HardState is unchanged.
// An empty Entries slice means that the stored log is unchanged.
type PersistenceEffect struct {
	HardState *HardState
	Entries   []LogEntry
}

// ApplyEffect identifies committed log entries that become
// eligible for ordered application to the KV state machine.
type ApplyEffect struct {
	FromIndex uint64
	ToIndex   uint64
}

// Effects contains the externally visible work requested
// by one successfully planned Raft transition.
//
// The executor must persist the batch before releasing
// messages that depend on the new durable state.
type Effects struct {
	Persist  *PersistenceEffect
	Messages []OutboundMessage
	Apply    []ApplyEffect

	ResetElectionTimer  bool
	ResetHeartbeatTimer bool
}
