package raft

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// ErrNotCandidate means an election cannot be initiated
// from the node's current role.
var ErrNotCandidate = errors.New("node cannot start an election")

// validateElectionMembers checks a fixed voting membership.
// Dynamic membership changes are not supported yet.
func validateElectionMembers(
	self string,
	peers []string,
) error {
	if strings.TrimSpace(self) == "" {
		return fmt.Errorf("%w: empty local node ID", ErrInvalidEvent)
	}

	seen := map[string]bool{self: true}

	for _, peer := range peers {
		if strings.TrimSpace(peer) == "" {
			return fmt.Errorf("%w: empty peer ID", ErrInvalidEvent)
		}
		if seen[peer] {
			return fmt.Errorf(
				"%w: duplicate or local peer %q",
				ErrInvalidEvent,
				peer,
			)
		}
		seen[peer] = true
	}
	return nil
}

func isElectionPeer(peer string, peers []string) bool {
	for _, id := range peers {
		if peer == id {
			return true
		}
	}
	return false
}

// StartElection begins a new election and returns effects
// containing RequestVote messages for the other members.
//
// The outgoing messages must be dispatched by the runtime
// only after this function returns successfully.
func (r *Raft) StartElection(
	peers []string,
) (Effects, error) {
	if err := validateElectionMembers(r.id, peers); err != nil {
		return Effects{}, err
	}

	return r.processTransition(func(current raftState) (Transition, error) {
		if current.role == Leader {
			return Transition{}, ErrNotCandidate
		}

		if current.currentTerm == math.MaxUint64 {
			return Transition{}, fmt.Errorf(
				"%w: raft term overflow",
				ErrInvalidTransition,
			)
		}

		// Your core election algorithm.
		next := startElection(current, r.id)

		next.electionVotes = map[string]bool{
			r.id: true,
		}

		result := Transition{next: next}

		// The new term and self-vote must be durable
		// before we send RequestVote messages.
		result.effects.Persist = &PersistenceEffect{
			HardState: &HardState{
				CurrentTerm: next.currentTerm,
				VotedFor:    next.votedFor,
				CommitIndex: next.commitIndex,
			},
		}

		result.effects.ResetElectionTimer = true

		lastIndex, lastTerm := lastLogInfo(next.log)

		// A single-member cluster already has a quorum.
		if hasElectionMajority(next.electionVotes, len(peers)+1) {
			next.role = Leader
			appendLeaderNoop(&next, r.id, peers, &result)
			return result, nil
		}

		// Generate one RequestVote request per peer.
		for _, peer := range peers {
			request := &RequestVoteRequest{
				Term:         next.currentTerm,
				CandidateID:  r.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}

			result.effects.Messages = append(
				result.effects.Messages,
				OutboundMessage{
					To:          peer,
					Type:        MessageRequestVote,
					RequestVote: request,
					RequestID:   next.currentTerm,
				},
			)
		}

		return result, nil
	})
}

// HandleRequestVoteResponse processes one peer's response.
//
// requestTerm is the term of the RequestVote RPC that
// generated this response. It lets us discard delayed
// responses from old elections.
//
// peers is the fixed, validated set of other voters.
func (r *Raft) HandleRequestVoteResponse(
	from string,
	requestTerm uint64,
	response RequestVoteResponse,
	peers []string,
) (Effects, error) {
	if err := validateElectionMembers(r.id, peers); err != nil {
		return Effects{}, err
	}

	if !isElectionPeer(from, peers) {
		return Effects{}, fmt.Errorf(
			"%w: response from unknown peer %q",
			ErrInvalidEvent,
			from,
		)
	}

	return r.processTransition(func(current raftState) (Transition, error) {
		next := cloneRaftState(current)
		result := Transition{next: next}

		// A higher term always takes precedence,
		// even if this is a delayed RPC response.
		if response.Term > current.currentTerm {
			next.currentTerm = response.Term
			next.role = Follower
			next.votedFor = ""
			next.electionVotes = nil

			result.next = next
			result.effects.Persist = &PersistenceEffect{
				HardState: &HardState{
					CurrentTerm: next.currentTerm,
					VotedFor:    "",
					CommitIndex: next.commitIndex,
				},
			}
			result.effects.ResetElectionTimer = true

			return result, nil
		}

		// Only active candidates can count votes.
		if current.role != Candidate {
			return result, nil
		}

		// Ignore responses from old or mismatched terms.
		if requestTerm != current.currentTerm ||
			response.Term != current.currentTerm {
			return result, nil
		}

		if !response.VoteGranted {
			return result, nil
		}

		// Copy the vote set. A map prevents duplicate
		// responses from being counted multiple times.
		next.electionVotes = make(
			map[string]bool,
			len(current.electionVotes)+1,
		)
		for peer, granted := range current.electionVotes {
			next.electionVotes[peer] = granted
		}
		next.electionVotes[from] = true

		// The voter set includes this node itself.
		clusterSize := len(peers) + 1

		if hasElectionMajority(next.electionVotes, clusterSize) {
			next.role = Leader

			appendLeaderNoop(&next, r.id, peers, &result)
		}

		result.next = next
		return result, nil
	})
}
