package raft

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidEvent = errors.New("invalid raft event")

// validateEvent verifies that an event contains exactly
// the payload required by its event type.
//
// This function validates event structure and basic identities.
// Raft protocol decisions such as term comparisons, voting,
// and log matching belong to their respective handlers.
func validateEvent(event Event) error {
	hasExtraPayloads := func(allowed EventType) bool {
		if allowed != EventRequestVote && event.RequestVote != nil {
			return true
		}
		if allowed != EventRequestVoteResponse &&
			event.RequestVoteResponse != nil {
			return true
		}
		if allowed != EventAppendEntries &&
			event.AppendEntries != nil {
			return true
		}
		if allowed != EventAppendEntriesResponse &&
			event.AppendEntriesResponse != nil {
			return true
		}
		if allowed != EventClientProposal && event.Proposal != nil {
			return true
		}
		return false
	}

	switch event.Type {
	case EventElectionTimeout, EventHeartbeatTimeout:
		if hasExtraPayloads(event.Type) {
			return fmt.Errorf(
				"%w: timeout event cannot contain RPC or proposal payloads",
				ErrInvalidEvent,
			)
		}

		if event.From != "" || event.RequestID != 0 {
			return fmt.Errorf(
				"%w: timeout event cannot contain peer metadata",
				ErrInvalidEvent,
			)
		}

	case EventRequestVote:
		if event.RequestVote != nil && event.RequestVote.Term == 0 {
			return fmt.Errorf("%w: zero RequestVote term", ErrInvalidEvent)
		}
		if event.RequestVote == nil {
			return fmt.Errorf(
				"%w: missing RequestVote payload",
				ErrInvalidEvent,
			)
		}

		if hasExtraPayloads(EventRequestVote) {
			return fmt.Errorf(
				"%w: unexpected payload in RequestVote event",
				ErrInvalidEvent,
			)
		}

		if strings.TrimSpace(event.From) == "" {
			return fmt.Errorf(
				"%w: RequestVote requires a sender",
				ErrInvalidEvent,
			)
		}

		if strings.TrimSpace(event.RequestVote.CandidateID) == "" {
			return fmt.Errorf(
				"%w: RequestVote requires a candidate ID",
				ErrInvalidEvent,
			)
		}

		if event.From != event.RequestVote.CandidateID {
			return fmt.Errorf(
				"%w: sender and candidate ID do not match",
				ErrInvalidEvent,
			)
		}

	case EventRequestVoteResponse:
		if event.RequestVoteResponse == nil {
			return fmt.Errorf(
				"%w: missing RequestVoteResponse payload",
				ErrInvalidEvent,
			)
		}

		if hasExtraPayloads(EventRequestVoteResponse) {
			return fmt.Errorf(
				"%w: unexpected payload in RequestVoteResponse event",
				ErrInvalidEvent,
			)
		}

		if strings.TrimSpace(event.From) == "" {
			return fmt.Errorf(
				"%w: RequestVoteResponse requires a sender",
				ErrInvalidEvent,
			)
		}

	case EventAppendEntries:
		if event.AppendEntries != nil && event.AppendEntries.Term == 0 {
			return fmt.Errorf("%w: zero AppendEntries term", ErrInvalidEvent)
		}
		if event.AppendEntries == nil {
			return fmt.Errorf(
				"%w: missing AppendEntries payload",
				ErrInvalidEvent,
			)
		}

		if hasExtraPayloads(EventAppendEntries) {
			return fmt.Errorf(
				"%w: unexpected payload in AppendEntries event",
				ErrInvalidEvent,
			)
		}

		if strings.TrimSpace(event.From) == "" {
			return fmt.Errorf(
				"%w: AppendEntries requires a sender",
				ErrInvalidEvent,
			)
		}

		if strings.TrimSpace(event.AppendEntries.LeaderID) == "" {
			return fmt.Errorf(
				"%w: AppendEntries requires a leader ID",
				ErrInvalidEvent,
			)
		}

		if event.From != event.AppendEntries.LeaderID {
			return fmt.Errorf(
				"%w: sender and leader ID do not match",
				ErrInvalidEvent,
			)
		}

	case EventAppendEntriesResponse:
		if event.AppendEntriesResponse == nil {
			return fmt.Errorf(
				"%w: missing AppendEntriesResponse payload",
				ErrInvalidEvent,
			)
		}

		if hasExtraPayloads(EventAppendEntriesResponse) {
			return fmt.Errorf(
				"%w: unexpected payload in AppendEntriesResponse event",
				ErrInvalidEvent,
			)
		}

		if strings.TrimSpace(event.From) == "" {
			return fmt.Errorf(
				"%w: AppendEntriesResponse requires a sender",
				ErrInvalidEvent,
			)
		}

	case EventClientProposal:
		if event.Proposal == nil {
			return fmt.Errorf(
				"%w: missing client proposal",
				ErrInvalidEvent,
			)
		}

		if hasExtraPayloads(EventClientProposal) {
			return fmt.Errorf(
				"%w: unexpected payload in client proposal",
				ErrInvalidEvent,
			)
		}

		if event.From != "" || event.RequestID != 0 {
			return fmt.Errorf(
				"%w: client proposal cannot contain peer metadata",
				ErrInvalidEvent,
			)
		}

		if err := validateRaftCommand(*event.Proposal); err != nil {
			return fmt.Errorf(
				"%w: invalid client proposal: %v",
				ErrInvalidEvent,
				err,
			)
		}

	default:
		return fmt.Errorf(
			"%w: unknown event type %d",
			ErrInvalidEvent,
			event.Type,
		)
	}

	return nil
}
