package raft

// raftState represents the consensus state that can change
// as a result of processing an event.
//
// This is separate from the live Raft instance so a
// transition can be planned without publishing mutations.
type raftState struct {
	role Role

	currentTerm uint64
	votedFor    string
	log         []LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64
}

// Transition describes one proposed Raft state change
// and the effects required to make it externally visible.
//
// It must not be published until its required persistence
// has completed successfully.
type Transition struct {
	next    raftState
	effects Effects
}
