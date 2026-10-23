package transport

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

var ErrPeerNotFound = errors.New("peer not found")

type PeerManager struct {
	localID string
	peers   map[string]string
	client  *Client
}

func NewPeerManager(
	localID string,
	peers map[string]string,
	client *Client,
) *PeerManager {
	// Create an independent copy of the peer registry.
	dup := make(map[string]string, len(peers))

	for id, address := range peers {
		dup[id] = address
	}

	return &PeerManager{
		localID: localID,
		peers:   dup,
		client:  client,
	}
}

func (pm *PeerManager) GetPeer(peerID string) (string, error) {
	address, exists := pm.peers[peerID]
	if !exists {
		return "", fmt.Errorf("%w: %s", ErrPeerNotFound, peerID)
	}

	return address, nil
}

func (pm *PeerManager) ListPeers() []string {
	ids := make([]string, 0, len(pm.peers))

	for id := range pm.peers {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}

func (pm *PeerManager) PingPeer(
	ctx context.Context,
	peerID string,
) (PingResponse, error) {
	// Resolve the peer's address.
	address, err := pm.GetPeer(peerID)
	if err != nil {
		return PingResponse{}, err
	}

	// Build the request using our local node identity.
	req := PingRequest{
		From: pm.localID,
	}

	// Send the RPC using the reusable HTTP client.
	resp, err := pm.client.Ping(ctx, address, req)
	if err != nil {
		return PingResponse{}, fmt.Errorf(
			"ping peer %q at %s: %w",
			peerID,
			address,
			err,
		)
	}

	// Verify that the response came from the expected node ID.
	if resp.NodeID != peerID {
		return PingResponse{}, fmt.Errorf(
			"peer identity mismatch: expected %q, got %q",
			peerID,
			resp.NodeID,
		)
	}

	return resp, nil
}
