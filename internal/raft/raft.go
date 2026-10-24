package raft

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type Raft struct {
	mu sync.RWMutex

	// Identity
	id string

	// Volatile role
	role Role

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         []LogEntry
	commitIndex uint64

	// Volatile state
	lastApplied uint64

	// Leader-specific volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Durable Raft storage.
	// Nil only for instances created with New().
	storage Storage
}

// New creates an in-memory Raft instance.
//
// It is retained for existing tests and must not be used
// for a node that requires durable Raft persistence.
func New(id string) *Raft {
	return &Raft{
		id:          id,
		role:        Follower,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
	}
}

// NewWithStorage initializes Raft from previously persisted state.
//
// The node always starts as a follower, regardless of
// its role before the previous shutdown.
//
// Storage ownership remains with the caller.
func NewWithStorage(id string, storage Storage) (*Raft, error) {
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("raft node ID cannot be empty")
	}

	if isNilStorage(storage) {
		return nil, fmt.Errorf("raft storage cannot be nil")
	}

	// Step 1: Load durable state.
	state, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("load persistent raft state: %w", err)
	}

	// Step 2: Verify the recovered state.
	if err := validatePersistentState(state); err != nil {
		return nil, fmt.Errorf(
			"validate recovered raft state: %w",
			err,
		)
	}

	// Step 3: Independently copy the recovered log.
	entries := append([]LogEntry(nil), state.Entries...)

	// Step 4: Restore durable fields and initialize
	// volatile fields from scratch.
	r := &Raft{
		id:          id,
		role:        Follower,
		currentTerm: state.HardState.CurrentTerm,
		votedFor:    state.HardState.VotedFor,
		log:         entries,
		commitIndex: state.HardState.CommitIndex,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		storage:     storage,
	}

	return r, nil
}

// isNilStorage detects nil interfaces and typed-nil
// implementations of Storage.
func isNilStorage(storage Storage) bool {
	if storage == nil {
		return true
	}

	value := reflect.ValueOf(storage)

	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Ptr,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (r *Raft) ID() string {
	return r.id
}

func (r *Raft) Role() Role {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.role
}

func (r *Raft) CurrentTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.currentTerm
}
