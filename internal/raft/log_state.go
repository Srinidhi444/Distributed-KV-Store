package raft

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidRaftState = errors.New("invalid raft persistent state")
	ErrCommittedEntry   = errors.New("cannot overwrite committed raft entry")
)

// validateRaftCommand verifies that a command has a valid
// type and payload.
func validateRaftCommand(cmd Command) error {
	switch cmd.Type {
	case CommandNoop:
		if cmd.Key != "" || cmd.Value != "" {
			return fmt.Errorf(
				"%w: NOOP command cannot contain key or value",
				ErrInvalidRaftState,
			)
		}

	case CommandSet:
		if strings.TrimSpace(cmd.Key) == "" {
			return fmt.Errorf(
				"%w: SET command requires a nonempty key",
				ErrInvalidRaftState,
			)
		}

	case CommandDelete:
		if strings.TrimSpace(cmd.Key) == "" {
			return fmt.Errorf(
				"%w: DELETE command requires a nonempty key",
				ErrInvalidRaftState,
			)
		}

		if cmd.Value != "" {
			return fmt.Errorf(
				"%w: DELETE command cannot contain a value",
				ErrInvalidRaftState,
			)
		}

	default:
		return fmt.Errorf(
			"%w: unknown command type %d",
			ErrInvalidRaftState,
			cmd.Type,
		)
	}

	return nil
}

// validateRaftEntries checks that entries:
//   - Begin at the expected index
//   - Have contiguous indices
//   - Have nonzero, nondecreasing terms
//   - Contain valid commands
//
// An empty slice is valid.
func validateRaftEntries(
	entries []LogEntry,
	firstIndex uint64,
) error {
	for i, entry := range entries {
		expectedIndex := firstIndex + uint64(i)

		if entry.Index != expectedIndex {
			return fmt.Errorf(
				"%w: expected log index %d, got %d",
				ErrInvalidRaftState,
				expectedIndex,
				entry.Index,
			)
		}

		if entry.Term == 0 {
			return fmt.Errorf(
				"%w: zero term at log index %d",
				ErrInvalidRaftState,
				entry.Index,
			)
		}

		if i > 0 && entry.Term < entries[i-1].Term {
			return fmt.Errorf(
				"%w: term decreased at log index %d",
				ErrInvalidRaftState,
				entry.Index,
			)
		}

		if err := validateRaftCommand(entry.Command); err != nil {
			return fmt.Errorf(
				"invalid command at log index %d: %w",
				entry.Index,
				err,
			)
		}
	}

	return nil
}

// validatePersistentState checks the invariants of a complete
// recovered or proposed persistent Raft state.
func validatePersistentState(state PersistentState) error {
	// Snapshot persistence is not implemented until Phase 6.
	if state.LastIncludedIndex != 0 ||
		state.LastIncludedTerm != 0 {
		return fmt.Errorf(
			"%w: nonzero snapshot boundary is not supported",
			ErrInvalidRaftState,
		)
	}

	if err := validateRaftEntries(state.Entries, 1); err != nil {
		return err
	}

	lastLogIndex := uint64(len(state.Entries))

	if state.HardState.CommitIndex > lastLogIndex {
		return fmt.Errorf(
			"%w: commit index %d exceeds last log index %d",
			ErrInvalidRaftState,
			state.HardState.CommitIndex,
			lastLogIndex,
		)
	}

	if len(state.Entries) > 0 {
		lastLogTerm := state.Entries[len(state.Entries)-1].Term

		if lastLogTerm > state.HardState.CurrentTerm {
			return fmt.Errorf(
				"%w: last log term %d exceeds current term %d",
				ErrInvalidRaftState,
				lastLogTerm,
				state.HardState.CurrentTerm,
			)
		}
	}

	return nil
}

// applyRaftBatch validates a proposed durable Raft update
// and returns the resulting persistent state.
//
// It never mutates current, batch, or their backing slices.
//
// The same function is used by:
//   - WALStorage.Save() before persistence
//   - WAL recovery when replaying durable batches
func applyRaftBatch(
	current PersistentState,
	batch raftBatch,
) (PersistentState, error) {
	// Step 1: Reject logically empty batches.
	if batch.HardState == nil && len(batch.Entries) == 0 {
		return PersistentState{}, fmt.Errorf(
			"%w: empty batch",
			ErrInvalidRaftState,
		)
	}

	// Step 2: Validate the existing state.
	if err := validatePersistentState(current); err != nil {
		return PersistentState{}, fmt.Errorf(
			"invalid current raft state: %w",
			err,
		)
	}

	// Step 3: Build an independent copy.
	next := current
	next.Entries = append([]LogEntry(nil), current.Entries...)

	// Step 4: Validate and apply incoming entries.
	if len(batch.Entries) > 0 {
		firstIndex := batch.Entries[0].Index

		if firstIndex == 0 {
			return PersistentState{}, fmt.Errorf(
				"%w: log indices must start at 1",
				ErrInvalidRaftState,
			)
		}

		if err := validateRaftEntries(
			batch.Entries,
			firstIndex,
		); err != nil {
			return PersistentState{}, err
		}

		lastExistingIndex := uint64(len(current.Entries))

		// Incoming entries may append at lastIndex+1 or
		// replace an existing uncommitted suffix.
		//
		// Starting beyond lastIndex+1 would create a gap.
		if firstIndex > lastExistingIndex+1 {
			return PersistentState{}, fmt.Errorf(
				"%w: log gap: incoming index %d, last existing index %d",
				ErrInvalidRaftState,
				firstIndex,
				lastExistingIndex,
			)
		}

		// Committed history must never be replaced.
		if firstIndex <= current.HardState.CommitIndex {
			return PersistentState{}, fmt.Errorf(
				"%w: replacement starts at index %d, commit index is %d",
				ErrCommittedEntry,
				firstIndex,
				current.HardState.CommitIndex,
			)
		}

		// Snapshot boundary is currently zero, so Raft
		// index N corresponds to slice position N-1.
		prefixLength := int(firstIndex - 1)

		// Verify term ordering at the boundary between
		// the retained prefix and incoming entries.
		if prefixLength > 0 {
			previousTerm := current.Entries[prefixLength-1].Term
			incomingTerm := batch.Entries[0].Term

			if incomingTerm < previousTerm {
				return PersistentState{}, fmt.Errorf(
					"%w: incoming term %d is lower than retained prefix term %d",
					ErrInvalidRaftState,
					incomingTerm,
					previousTerm,
				)
			}
		}

		// Build a new logical log:
		// retained prefix + incoming entries.
		//
		// No backing-array aliasing with current or batch.
		updated := make(
			[]LogEntry,
			0,
			prefixLength+len(batch.Entries),
		)

		updated = append(
			updated,
			current.Entries[:prefixLength]...,
		)

		updated = append(
			updated,
			batch.Entries...,
		)

		next.Entries = updated
	}

	// Step 5: Validate and apply HardState.
	if batch.HardState != nil {
		hs := *batch.HardState

		// A node must never return to an older term.
		if hs.CurrentTerm < current.HardState.CurrentTerm {
			return PersistentState{}, fmt.Errorf(
				"%w: term decreased from %d to %d",
				ErrInvalidRaftState,
				current.HardState.CurrentTerm,
				hs.CurrentTerm,
			)
		}

		// A node that voted in the current term cannot
		// change or erase that vote.
		if hs.CurrentTerm == current.HardState.CurrentTerm &&
			current.HardState.VotedFor != "" &&
			hs.VotedFor != current.HardState.VotedFor {
			return PersistentState{}, fmt.Errorf(
				"%w: cannot change or clear vote for %q in term %d",
				ErrInvalidRaftState,
				current.HardState.VotedFor,
				current.HardState.CurrentTerm,
			)
		}

		// A new term may reset VotedFor to an empty string.
		// Alternatively, the node may vote for a candidate
		// in that new term.

		if hs.CommitIndex < current.HardState.CommitIndex {
			return PersistentState{}, fmt.Errorf(
				"%w: commit index decreased from %d to %d",
				ErrInvalidRaftState,
				current.HardState.CommitIndex,
				hs.CommitIndex,
			)
		}

		next.HardState = hs
	}

	// Step 6: Validate the complete resulting state.
	if err := validatePersistentState(next); err != nil {
		return PersistentState{}, fmt.Errorf(
			"invalid resulting raft state: %w",
			err,
		)
	}

	return next, nil
}
