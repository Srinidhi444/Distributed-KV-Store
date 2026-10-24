package raft

import (
	"errors"
	"fmt"
	"io"
)

// replay reconstructs the durable Raft state by reading
// and applying every WAL batch in order.
//
// It never modifies the WAL contents.
//
// The same applyRaftBatch function is used during normal
// writes and recovery, ensuring consistent validation rules.
//
// On failure, the WAL is marked unusable for further writes.
func (w *RaftWAL) replay() (PersistentState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return PersistentState{}, ErrStorageClosed
	}

	if w.failed {
		return PersistentState{}, ErrStorageFailed
	}

	// Recovery begins with Raft's initial persistent state.
	state := PersistentState{}

	// Start at the beginning of the WAL.
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		w.failed = true
		return PersistentState{}, fmt.Errorf(
			"seek raft WAL start: %w",
			err,
		)
	}

	// Validate file identity and format version.
	if err := ValidateRaftWALHeader(w.file); err != nil {
		w.failed = true
		return PersistentState{}, fmt.Errorf(
			"validate raft WAL header: %w",
			err,
		)
	}

	for {
		// Record the beginning of the next batch.
		startOffset, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			w.failed = true
			return PersistentState{}, fmt.Errorf(
				"get raft WAL offset: %w",
				err,
			)
		}

		// Decode one physical batch record.
		batch, err := decodeRaftBatch(w.file)

		if err != nil {
			endOffset, seekErr := w.file.Seek(
				0,
				io.SeekCurrent,
			)
			if seekErr != nil {
				w.failed = true
				return PersistentState{}, fmt.Errorf(
					"get raft WAL end offset: %w",
					seekErr,
				)
			}

			// Clean EOF is allowed only when no bytes
			// of the next record were consumed.
			if errors.Is(err, io.EOF) &&
				startOffset == endOffset {
				break
			}

			w.failed = true
			return PersistentState{}, fmt.Errorf(
				"decode raft WAL record at offset %d: %w",
				startOffset,
				err,
			)
		}

		// Validate and apply the logical Raft update.
		next, err := applyRaftBatch(state, batch)
		if err != nil {
			w.failed = true
			return PersistentState{}, fmt.Errorf(
				"apply raft WAL batch at offset %d: %w",
				startOffset,
				err,
			)
		}

		state = next
	}

	// Validate the reconstructed state one final time.
	if err := validatePersistentState(state); err != nil {
		w.failed = true
		return PersistentState{}, fmt.Errorf(
			"invalid recovered raft state: %w",
			err,
		)
	}

	// Restore the file position to the end so subsequent
	// append operations begin after existing records.
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		w.failed = true
		return PersistentState{}, fmt.Errorf(
			"seek raft WAL end after recovery: %w",
			err,
		)
	}

	return state, nil
}
