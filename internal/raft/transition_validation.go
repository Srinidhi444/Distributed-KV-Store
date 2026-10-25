package raft

import (
	"errors"
	"fmt"
	"reflect"
)

var ErrInvalidTransition = errors.New("invalid raft transition")

func validateTransition(
	current raftState,
	transition Transition,
) error {
	next := transition.next
	effects := transition.effects

	// 1. Validate the proposed role.
	switch next.role {
	case Follower, Candidate, Leader:
		// Valid.
	default:
		return fmt.Errorf(
			"%w: invalid role %d",
			ErrInvalidTransition,
			next.role,
		)
	}

	// 2. Commit index must never decrease.
	if next.commitIndex < current.commitIndex {
		return fmt.Errorf(
			"%w: commit index decreased from %d to %d",
			ErrInvalidTransition,
			current.commitIndex,
			next.commitIndex,
		)
	}

	// 3. Applied index must never decrease.
	if next.lastApplied < current.lastApplied {
		return fmt.Errorf(
			"%w: lastApplied decreased from %d to %d",
			ErrInvalidTransition,
			current.lastApplied,
			next.lastApplied,
		)
	}

	// 4. Applied entries must already be committed.
	if next.lastApplied > next.commitIndex {
		return fmt.Errorf(
			"%w: lastApplied %d exceeds commitIndex %d",
			ErrInvalidTransition,
			next.lastApplied,
			next.commitIndex,
		)
	}

	// Task 34 will introduce explicit application
	// acknowledgments. Until then, a transition cannot
	// directly advance lastApplied.
	if next.lastApplied > current.lastApplied {
		return fmt.Errorf(
			"%w: lastApplied cannot advance without application confirmation",
			ErrInvalidTransition,
		)
	}

	// 5. Reconstruct the current durable state.
	currentPersistent := PersistentState{
		HardState: HardState{
			CurrentTerm: current.currentTerm,
			VotedFor:    current.votedFor,
			CommitIndex: current.commitIndex,
		},
		Entries: append([]LogEntry(nil), current.log...),
	}

	if err := validatePersistentState(currentPersistent); err != nil {
		return fmt.Errorf(
			"%w: invalid current persistent state: %v",
			ErrInvalidTransition,
			err,
		)
	}

	// 6. Calculate what the durable state should become.
	expected := currentPersistent

	if effects.Persist != nil {
		batch := raftBatch{
			HardState: effects.Persist.HardState,
			Entries:   effects.Persist.Entries,
		}

		// A persistence effect must contain an actual update.
		if batch.HardState == nil && len(batch.Entries) == 0 {
			return fmt.Errorf(
				"%w: empty persistence effect",
				ErrInvalidTransition,
			)
		}

		var err error
		expected, err = applyRaftBatch(currentPersistent, batch)
		if err != nil {
			return fmt.Errorf(
				"%w: invalid persistence effect: %v",
				ErrInvalidTransition,
				err,
			)
		}
	}

	// 7. Compare expected durable state against proposed state.
	if next.currentTerm != expected.HardState.CurrentTerm {
		return fmt.Errorf(
			"%w: proposed term does not match persisted term",
			ErrInvalidTransition,
		)
	}

	if next.votedFor != expected.HardState.VotedFor {
		return fmt.Errorf(
			"%w: proposed vote does not match persisted vote",
			ErrInvalidTransition,
		)
	}

	if next.commitIndex != expected.HardState.CommitIndex {
		return fmt.Errorf(
			"%w: proposed commit index does not match persisted commit index",
			ErrInvalidTransition,
		)
	}

	if !reflect.DeepEqual(next.log, expected.Entries) {
		return fmt.Errorf(
			"%w: proposed log does not match persisted log",
			ErrInvalidTransition,
		)
	}

	// 8. Validate application ranges.
	for _, apply := range effects.Apply {
		if apply.FromIndex == 0 ||
			apply.ToIndex < apply.FromIndex {
			return fmt.Errorf(
				"%w: invalid application range [%d, %d]",
				ErrInvalidTransition,
				apply.FromIndex,
				apply.ToIndex,
			)
		}

		if apply.ToIndex > next.commitIndex {
			return fmt.Errorf(
				"%w: application range exceeds commit index",
				ErrInvalidTransition,
			)
		}
	}

	return nil
}
