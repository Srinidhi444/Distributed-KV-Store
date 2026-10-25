package raft

// startElection prepares the local state for a new election.
//
// This function computes state only. Persistence,
// network requests, and timer management are handled
// by the transition engine and runtime.
func startElection(current raftState, nodeID string) raftState {
	next := cloneRaftState(current)
	next.currentTerm++
	next.role = Candidate
	next.votedFor = nodeID
	// Vote for this node.

	return next
}

func hasElectionMajority(
	grantedVotes map[string]bool,
	clusterSize int,
) bool {
	// Count how many members actually granted their vote (value is true)
	count := 0
	for _, granted := range grantedVotes {
		if granted {
			count++
		}
	}

	// A majority is strictly greater than half the cluster size
	return count > clusterSize/2
}
