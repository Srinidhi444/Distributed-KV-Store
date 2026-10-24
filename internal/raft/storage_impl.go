package raft

import (
	"errors"
	"fmt"
	"sync"
)

// WALStorage coordinates logical Raft state with durable WAL writes.
//
// The mutex protects the entire sequence:
// validate -> persist -> publish.
type WALStorage struct {
	mu sync.Mutex

	wal   *RaftWAL
	state PersistentState
}

// OpenWALStorage opens Raft storage and reconstructs
// the previously persisted state before returning.
func OpenWALStorage(path string) (*WALStorage, error) {
	wal, err := OpenRaftWAL(path)
	if err != nil {
		return nil, fmt.Errorf("open raft WAL: %w", err)
	}

	state, err := wal.replay()
	if err != nil {
		closeErr := wal.Close()

		return nil, errors.Join(
			fmt.Errorf("recover raft WAL: %w", err),
			closeErr,
		)
	}

	return &WALStorage{
		wal:   wal,
		state: state,
	}, nil
}

// Load returns an independent copy of the current
// persistent Raft state.
func (s *WALStorage) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.checkState(); err != nil {
		return PersistentState{}, err
	}

	copied := s.state

	copied.Entries = append(
		[]LogEntry(nil),
		s.state.Entries...,
	)

	return copied, nil
}

// Save validates, durably persists, and publishes
// one logical Raft state update.
//
// No in-memory changes are published before a
// successful WAL synchronization.
func (s *WALStorage) Save(
	hardState *HardState,
	entries []LogEntry,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.checkState(); err != nil {
		return err
	}

	// An empty update does not require a WAL write.
	if hardState == nil && len(entries) == 0 {
		return nil
	}

	batch := raftBatch{
		HardState: hardState,
		Entries:   entries,
	}

	// Step 1: Validate the update and calculate
	// the resulting logical Raft state.
	next, err := applyRaftBatch(s.state, batch)
	if err != nil {
		return fmt.Errorf("validate raft batch: %w", err)
	}

	// Step 2: Persist the batch.
	//
	// appendBatch returns successfully only after
	// the WAL record has been written and synced.
	if err := s.wal.appendBatch(batch); err != nil {
		return fmt.Errorf("persist raft batch: %w", err)
	}

	// Step 3: Publish the new in-memory state
	// only after durable persistence succeeds.
	s.state = next

	return nil
}

// Close closes the WAL and releases its exclusive file lock.
//
// The underlying RaftWAL.Close() is idempotent.
func (s *WALStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.wal.Close()
}

// Compile-time verification that WALStorage
// implements the Storage interface.
var _ Storage = (*WALStorage)(nil)
