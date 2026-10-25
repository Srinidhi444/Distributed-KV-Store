package raft

import "fmt"

// ElectionOutcome separates the reply owed to the caller
// from effects that must be executed by the runtime.
type ElectionOutcome struct {
	Effects        Effects
	AppendResponse *AppendEntriesResponse
	VoteResponse   *RequestVoteResponse
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
		response, effects, err := r.HandleHeartbeat(
			*event.AppendEntries,
		)
		if err != nil {
			return ElectionOutcome{}, err
		}

		return ElectionOutcome{
			Effects:        effects,
			AppendResponse: &response,
		}, nil

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
		if r.Role() != Leader {
			return ElectionOutcome{}, nil
		}

		// Empty heartbeats only; replication comes next.
		effects := Effects{}
		for _, peer := range peers {
			effects.Messages = append(
				effects.Messages,
				OutboundMessage{
					To:   peer,
					Type: MessageAppendEntries,
					AppendEntries: &AppendEntriesRequest{
						Term:         r.CurrentTerm(),
						LeaderID:     r.ID(),
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						LeaderCommit: 0,
					},
				},
			)
		}

		return ElectionOutcome{Effects: effects}, nil

	case EventAppendEntriesResponse:
		response := event.AppendEntriesResponse

		if !isElectionPeer(event.From, peers) {
			return ElectionOutcome{},
				fmt.Errorf("%w: unknown peer", ErrInvalidEvent)
		}

		// Only a higher term affects state at this stage.
		_, err := r.processTransition(
			func(current raftState) (Transition, error) {
				next := cloneRaftState(current)
				t := Transition{next: next}

				if response.Term > current.currentTerm {
					next.currentTerm = response.Term
					next.role = Follower
					next.votedFor = ""
					next.electionVotes = nil

					t.next = next
					t.effects.Persist = &PersistenceEffect{
						HardState: &HardState{
							CurrentTerm: next.currentTerm,
							VotedFor:    "",
							CommitIndex: next.commitIndex,
						},
					}
					t.effects.ResetElectionTimer = true
				}

				return t, nil
			},
		)

		return ElectionOutcome{}, err

	default:
		return ElectionOutcome{}, fmt.Errorf(
			"%w: event type %d",
			ErrUnsupportedEvent,
			event.Type,
		)
	}
}
