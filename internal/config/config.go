package config

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	NodeID  string
	Address string
	DataDir string
	Peers   map[string]string
}

// validateAddress checks whether an address contains
// a valid host and TCP port.
func validateAddress(address string) error {
	if strings.TrimSpace(address) == "" {
		return fmt.Errorf("address cannot be empty")
	}

	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid host:port format: %w", err)
	}

	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("host cannot be empty")
	}

	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf(
			"invalid port %q: must be between 1 and 65535",
			portStr,
		)
	}

	return nil
}

// Validate verifies the node's identity, network address,
// storage directory, and peer configuration.
func (c Config) Validate() error {
	// Step 1: Validate node identity.
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	if c.NodeID != strings.TrimSpace(c.NodeID) {
		return fmt.Errorf("node ID cannot contain leading or trailing whitespace")
	}

	// Step 2: Validate local network address.
	if err := validateAddress(c.Address); err != nil {
		return fmt.Errorf("invalid node address: %w", err)
	}

	// Step 3: Validate storage directory.
	if strings.TrimSpace(c.DataDir) == "" {
		return fmt.Errorf("data directory cannot be empty")
	}

	// Step 4: Track addresses to reject duplicates.
	seenAddresses := map[string]bool{
		c.Address: true,
	}

	// Step 5: Validate every configured peer.
	for peerID, peerAddress := range c.Peers {
		if strings.TrimSpace(peerID) == "" {
			return fmt.Errorf("peer ID cannot be empty")
		}

		if peerID != strings.TrimSpace(peerID) {
			return fmt.Errorf(
				"peer ID %q contains leading or trailing whitespace",
				peerID,
			)
		}

		if peerID == c.NodeID {
			return fmt.Errorf(
				"node %q cannot be its own peer",
				c.NodeID,
			)
		}

		if err := validateAddress(peerAddress); err != nil {
			return fmt.Errorf(
				"invalid address for peer %q: %w",
				peerID,
				err,
			)
		}

		if seenAddresses[peerAddress] {
			return fmt.Errorf(
				"duplicate peer address %q",
				peerAddress,
			)
		}

		seenAddresses[peerAddress] = true
	}

	return nil
}

// WALPath returns the location of the node's WAL file.
func (c Config) WALPath() string {
	return filepath.Join(c.DataDir, "store.wal")
}
