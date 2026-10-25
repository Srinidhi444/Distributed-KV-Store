package raft

import "fmt"

// HandleRequestVote evaluates a vote request and persists
// any changes to the current term or votedFor.
//
// The returned response is safe to send only after this
// function returns successfully.
//
// The runtime must execute the returned non-persistence
// effects, particularly ResetElectionTimer.
func (r *Raft) HandleRequestVote(
	request RequestVoteRequest,
) (RequestVoteResponse, Effects, error) {
	if request.CandidateID == "" {
		return RequestVoteResponse{}, Effects{},
			fmt.Errorf("%w: empty candidate ID", ErrInvalidEvent)
	}

	var response RequestVoteResponse

	effects, err := r.processTransition(
		func(current raftState) (Transition, error) {
			// Core algorithm implemented by you.
			next, voteResponse := evaluateRequestVote(
				current,
				request,
			)

			response = voteResponse

			result := Transition{
				next: next,
			}

			// Persist whenever term or vote changes.
			termChanged := next.currentTerm != current.currentTerm
			voteChanged := next.votedFor != current.votedFor

			if termChanged || voteChanged {
				result.effects.Persist = &PersistenceEffect{
					HardState: &HardState{
						CurrentTerm: next.currentTerm,
						VotedFor:    next.votedFor,
						CommitIndex: next.commitIndex,
					},
				}
			}

			// Granting a vote postpones the next election.
			result.effects.ResetElectionTimer = voteResponse.VoteGranted

			return result, nil
		},
	)

	if err != nil {
		return RequestVoteResponse{}, Effects{},
			fmt.Errorf("handle request vote: %w", err)
	}

	return response, effects, nil
}
