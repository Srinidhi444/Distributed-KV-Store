package config

import (
	"path/filepath"
	"strings"
	"testing"
)

func validConfig() Config {
	return Config{
		NodeID:  "node1",
		Address: "127.0.0.1:8001",
		DataDir: "data/node1",
		Peers: map[string]string{
			"node2": "127.0.0.1:8002",
			"node3": "127.0.0.1:8003",
		},
	}
}

func TestValidConfig(t *testing.T) {
	cfg := validConfig()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid configuration, got: %v", err)
	}
}

func TestValidSingleNodeConfig(t *testing.T) {
	cfg := Config{
		NodeID:  "node1",
		Address: "127.0.0.1:8001",
		DataDir: "data/node1",
		Peers:   nil,
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected single-node configuration to be valid: %v", err)
	}
}

func TestInvalidNodeID(t *testing.T) {
	tests := []struct {
		name   string
		nodeID string
	}{
		{"empty ID", ""},
		{"whitespace-only ID", "   "},
		{"leading whitespace", " node1"},
		{"trailing whitespace", "node1 "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.NodeID = tt.nodeID

			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected invalid node ID %q to fail", tt.nodeID)
			}
		})
	}
}

func TestInvalidDataDirectory(t *testing.T) {
	tests := []string{
		"",
		" ",
		"   ",
		"\t",
	}

	for _, dataDir := range tests {
		t.Run("dir="+dataDir, func(t *testing.T) {
			cfg := validConfig()
			cfg.DataDir = dataDir

			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected invalid data directory %q", dataDir)
			}
		})
	}
}

func TestInvalidNodeAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{"empty address", ""},
		{"missing port", "127.0.0.1"},
		{"missing host", ":8001"},
		{"empty port", "127.0.0.1:"},
		{"port zero", "127.0.0.1:0"},
		{"negative port", "127.0.0.1:-1"},
		{"port too large", "127.0.0.1:65536"},
		{"non-numeric port", "127.0.0.1:abc"},
		{"unbracketed IPv6", "::1:8001"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Address = tt.address

			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected address %q to be rejected", tt.address)
			}
		})
	}
}

func TestValidAddressFormats(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{"IPv4", "127.0.0.1:8001"},
		{"hostname", "localhost:8001"},
		{"IPv6", "[::1]:8001"},
		{"minimum port", "localhost:1"},
		{"maximum port", "localhost:65535"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Address = tt.address

			if err := cfg.Validate(); err != nil {
				t.Fatalf(
					"expected address %q to be valid: %v",
					tt.address,
					err,
				)
			}
		})
	}
}

func TestInvalidPeerID(t *testing.T) {
	tests := []struct {
		name   string
		peerID string
	}{
		{"empty peer ID", ""},
		{"whitespace-only peer ID", "   "},
		{"leading whitespace", " node2"},
		{"trailing whitespace", "node2 "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()

			cfg.Peers = map[string]string{
				tt.peerID: "127.0.0.1:8002",
			}

			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected invalid peer ID %q", tt.peerID)
			}
		})
	}
}

func TestRejectSelfAsPeer(t *testing.T) {
	cfg := validConfig()

	cfg.Peers = map[string]string{
		"node1": "127.0.0.1:8002",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected node to reject itself as a peer")
	}
}

func TestRejectDuplicatePeerAddresses(t *testing.T) {
	cfg := validConfig()

	cfg.Peers = map[string]string{
		"node2": "127.0.0.1:8002",
		"node3": "127.0.0.1:8002",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected duplicate peer addresses to be rejected")
	}
}

func TestRejectPeerUsingLocalAddress(t *testing.T) {
	cfg := validConfig()

	cfg.Peers = map[string]string{
		"node2": "127.0.0.1:8001",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected peer using local address to be rejected")
	}
}

func TestInvalidPeerAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{"empty address", ""},
		{"missing port", "localhost"},
		{"missing host", ":8002"},
		{"invalid port", "localhost:abc"},
		{"port zero", "localhost:0"},
		{"port too large", "localhost:65536"},
		{"invalid IPv6", "::1:8002"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()

			cfg.Peers = map[string]string{
				"node2": tt.address,
			}

			if err := cfg.Validate(); err == nil {
				t.Fatalf(
					"expected invalid peer address %q to fail",
					tt.address,
				)
			}
		})
	}
}

func TestValidIPv6Peers(t *testing.T) {
	cfg := Config{
		NodeID:  "node1",
		Address: "[::1]:8001",
		DataDir: "data/node1",
		Peers: map[string]string{
			"node2": "[::1]:8002",
			"node3": "[::1]:8003",
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid IPv6 configuration: %v", err)
	}
}

func TestWALPath(t *testing.T) {
	cfg := validConfig()

	expected := filepath.Join("data", "node1", "store.wal")
	actual := cfg.WALPath()

	if actual != expected {
		t.Fatalf(
			"expected WAL path %q, got %q",
			expected,
			actual,
		)
	}
}

func TestWALPathCustomDirectory(t *testing.T) {
	cfg := validConfig()

	cfg.DataDir = filepath.Join(
		"custom",
		"storage",
		"node1",
	)

	expected := filepath.Join(
		"custom",
		"storage",
		"node1",
		"store.wal",
	)

	if actual := cfg.WALPath(); actual != expected {
		t.Fatalf(
			"expected WAL path %q, got %q",
			expected,
			actual,
		)
	}
}

func TestValidationErrorMessages(t *testing.T) {
	cfg := validConfig()
	cfg.NodeID = ""

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}

	if !strings.Contains(err.Error(), "node ID") {
		t.Fatalf("expected node ID context in error, got %v", err)
	}
}
