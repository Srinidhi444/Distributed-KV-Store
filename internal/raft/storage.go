package raft

import "errors"

var (
	ErrStorageClosed = errors.New("raft storage closed")
	ErrStorageFailed = errors.New("raft storage failed")
	ErrLogNotFound   = errors.New("raft log entry not found")
)

// HardState contains Raft metadata that must survive crashes.
type HardState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	CommitIndex uint64 `json:"commit_index"`
}

// PersistentState represents recovered durable Raft state.
type PersistentState struct {
	HardState HardState
	Entries   []LogEntry

	// Reserved for future snapshot-aware indexing.
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

// Storage defines the durable operations required by Raft.
type Storage interface {
	// Load returns an independent snapshot of recovered state.
	Load() (PersistentState, error)

	// Save persists optional HardState and entries together.
	//
	// Entries beginning at an existing index logically replace
	// the previous suffix starting at that index.
	//
	// Successful return means the batch is durable.
	// An uncertain I/O failure puts storage into a failed state.
	Save(hardState *HardState, entries []LogEntry) error

	// Close releases the underlying resources.
	Close() error
}
