package config

import (
	"flag"
	"fmt"
	"strings"
)

// ParseFlags converts command-line arguments into node configuration.
func ParseFlags(args []string) (Config, error) {
	fs := flag.NewFlagSet("distributed-kv", flag.ContinueOnError)

	nodeID := fs.String("id", "", "Unique node ID")
	address := fs.String("addr", "", "Node listening address")
	dataDir := fs.String("data-dir", "", "Node data directory")
	peersRaw := fs.String("peers", "", "Comma-separated peer IDs and addresses")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if fs.NArg() != 0 {
		return Config{}, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}

	peers := make(map[string]string)

	if strings.TrimSpace(*peersRaw) != "" {
		for _, entry := range strings.Split(*peersRaw, ",") {
			parts := strings.SplitN(entry, "=", 2)

			if len(parts) != 2 {
				return Config{}, fmt.Errorf("invalid peer entry %q: expected id=address", entry)
			}

			id := strings.TrimSpace(parts[0])
			addr := strings.TrimSpace(parts[1])

			if id == "" || addr == "" {
				return Config{}, fmt.Errorf("invalid peer entry %q", entry)
			}

			if _, exists := peers[id]; exists {
				return Config{}, fmt.Errorf("duplicate peer ID %q", id)
			}

			peers[id] = addr
		}
	}

	cfg := Config{
		NodeID:  *nodeID,
		Address: *address,
		DataDir: *dataDir,
		Peers:   peers,
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}
