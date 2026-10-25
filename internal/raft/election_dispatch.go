package raft

import "fmt"

// ElectionOutcome separates the reply owed to the caller
// from effects that must be executed by the runtime.
type ElectionOutcome struct {
	Effects        Effects
	AppendResponse *AppendEntriesResponse
	VoteResponse   *RequestVoteResponse
	ProposalIndex  uint64
	ProposalTerm   uint64
}

// dispatchElectionEvent processes an election-related
// event through the Raft transition engine.
//
// peers must be the authoritative set of other voting
// members, excluding the local node.
func (r *Raft) dispatchElectionEvent(
	event Event,
	peers []string,
) (ElectionOutcome, error) {
	if err := validateEvent(event); err != nil {
		return ElectionOutcome{}, err
	}

	switch event.Type {
	case EventElectionTimeout:
		effects, err := r.StartElection(peers)
		if err != nil {
			return ElectionOutcome{}, err
		}

		return ElectionOutcome{
			Effects: effects,
		}, nil

	case EventRequestVote:
		if !isElectionPeer(event.From, peers) {
			return ElectionOutcome{}, fmt.Errorf("%w: unconfigured candidate", ErrInvalidEvent)
		}
		response, effects, err := r.HandleRequestVote(
			*event.RequestVote,
		)
		if err != nil {
			return ElectionOutcome{}, err
		}

		return ElectionOutcome{
			Effects:      effects,
			VoteResponse: &response,
		}, nil

	case EventAppendEntries:
		if !isElectionPeer(event.From, peers) {
			return ElectionOutcome{}, fmt.Errorf("%w: unconfigured leader", ErrInvalidEvent)
		}
		response, effects, err := r.HandleAppendEntries(*event.AppendEntries)
		if err != nil {
			return ElectionOutcome{}, err
		}
		return ElectionOutcome{Effects: effects, AppendResponse: &response}, nil

	case EventRequestVoteResponse:
		// For RequestVote messages, the RequestID
		// currently carries the election term.
		//
		// The transport must populate this from the
		// original outgoing request, not from an
		// untrusted response body.
		effects, err := r.HandleRequestVoteResponse(
			event.From,
			event.RequestID,
			*event.RequestVoteResponse,
			peers,
		)
		if err != nil {
			return ElectionOutcome{}, err
		}

		return ElectionOutcome{
			Effects: effects,
		}, nil

	case EventHeartbeatTimeout:
		effects, err := r.processTransition(func(current raftState) (Transition, error) {
			transition := Transition{next: cloneRaftState(current)}
			if current.role == Leader {
				transition.effects = replicateToPeers(current, r.id, peers)
			}
			return transition, nil
		})
		return ElectionOutcome{Effects: effects}, err

	case EventAppendEntriesResponse:
		effects, err := r.handleAppendEntriesResponse(event, peers)
		return ElectionOutcome{Effects: effects}, err

	case EventClientProposal:
		index, term, effects, err := r.appendClientCommand(*event.Proposal, peers)
		return ElectionOutcome{Effects: effects, ProposalIndex: index, ProposalTerm: term}, err

	default:
		return ElectionOutcome{}, fmt.Errorf(
			"%w: event type %d",
			ErrUnsupportedEvent,
			event.Type,
		)
	}
}
