package raft

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type transitionPlanner func(raftState) (Transition, error)
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
	storage       Storage
	electionVotes map[string]bool
	failed        bool
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

// snapshotStateLocked captures the live Raft state.
//
// Caller must hold r.mu.
func (r *Raft) snapshotStateLocked() raftState {
	return cloneRaftState(raftState{
		role:          r.role,
		currentTerm:   r.currentTerm,
		votedFor:      r.votedFor,
		log:           r.log,
		commitIndex:   r.commitIndex,
		lastApplied:   r.lastApplied,
		nextIndex:     r.nextIndex,
		matchIndex:    r.matchIndex,
		electionVotes: r.electionVotes,
	})
}

// publishStateLocked replaces live state after persistence.
//
// Caller must hold r.mu.
func (r *Raft) publishStateLocked(state raftState) {
	// Copy scalar and value fields.
	r.role = state.role
	r.currentTerm = state.currentTerm
	r.votedFor = state.votedFor
	r.commitIndex = state.commitIndex
	r.lastApplied = state.lastApplied
	r.electionVotes = make(map[string]bool, len(state.electionVotes))

	for peer, granted := range state.electionVotes {
		r.electionVotes[peer] = granted
	}

	// Deep copy the log slice so it's not shared with external or recovered state.
	if state.log != nil {
		r.log = append([]LogEntry(nil), state.log...)
	} else {
		r.log = make([]LogEntry, 0)
	}

	// Deep copy the nextIndex map.
	if state.nextIndex != nil {
		r.nextIndex = make(map[string]uint64, len(state.nextIndex))
		for k, v := range state.nextIndex {
			r.nextIndex[k] = v
		}
	} else {
		r.nextIndex = make(map[string]uint64)
	}

	// Deep copy the matchIndex map.
	if state.matchIndex != nil {
		r.matchIndex = make(map[string]uint64, len(state.matchIndex))
		for k, v := range state.matchIndex {
			r.matchIndex[k] = v
		}
	} else {
		r.matchIndex = make(map[string]uint64)
	}
}

func (r *Raft) processTransition(
	planner transitionPlanner,
) (Effects, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. Reject transitions after a fatal storage failure.
	if r.failed {
		return Effects{}, ErrStorageFailed
	}

	if planner == nil {
		return Effects{}, fmt.Errorf(
			"%w: nil transition planner",
			ErrInvalidTransition,
		)
	}

	// 2. Capture an independent copy of the current state.
	current := r.snapshotStateLocked()

	// 3. Give the planner a separate copy so it cannot
	// accidentally mutate the state used for validation.
	planningState := cloneRaftState(current)

	transition, err := planner(planningState)
	if err != nil {
		return Effects{}, fmt.Errorf(
			"plan raft transition: %w",
			err,
		)
	}

	// 4. Verify that the proposed state and persistence
	// effects are consistent.
	if err := validateTransition(current, transition); err != nil {
		return Effects{}, fmt.Errorf(
			"validate raft transition: %w",
			err,
		)
	}

	// 5. Persist required changes before publication.
	if transition.effects.Persist != nil {
		if r.storage == nil {
			return Effects{}, fmt.Errorf(
				"%w: persistent transition requires storage",
				ErrInvalidTransition,
			)
		}

		effect := transition.effects.Persist

		err := r.storage.Save(
			effect.HardState,
			effect.Entries,
		)
		if err != nil {
			// A persistence failure may have an uncertain
			// outcome. Stop processing further transitions
			// until the node is recovered.
			r.failed = true

			return Effects{}, fmt.Errorf(
				"persist raft transition: %w",
				err,
			)
		}
	}

	// 6. Publish only after successful persistence.
	r.publishStateLocked(transition.next)

	// 7. Return effects for the runtime to execute.
	//
	// The persistence effect has already been handled.
	// The runtime must not execute it a second time.
	effects := transition.effects
	effects.Persist = nil

	return effects, nil
}
