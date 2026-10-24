package raft

import "sync"

type Raft struct {
	mu sync.RWMutex

	// Identity
	id string

	// Volatile role
	role Role

	// Persistent state (stored durably in Task 26)
	currentTerm uint64
	votedFor    string
	log         []LogEntry

	// Volatile state
	commitIndex uint64
	lastApplied uint64

	// Leader-specific volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
}

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
