package persistence

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type WAL struct {
	file   *os.File
	mu     sync.Mutex
	failed bool
	closed bool
}

const (
	WALMagic   = "DKV1"
	WALVersion = uint32(1)

	MaxPayloadSize = 1024 * 1024
)

func EncodeWALHeader() []byte {
	header := make([]byte, 8)

	copy(header[0:4], WALMagic)

	binary.BigEndian.PutUint32(header[4:8], WALVersion)

	return header
}

func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(
		path,
		os.O_RDWR|os.O_CREATE|os.O_EXCL,
		0600,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Close the file if initialization fails.
	success := false
	defer func() {
		if !success {
			file.Close()
		}
	}()

	// Encode the WAL header.
	header := EncodeWALHeader()

	// Write the header to disk.
	n, err := file.Write(header)
	if err != nil {
		return nil, fmt.Errorf("failed to write WAL header: %w", err)
	}

	// Ensure the complete header was written.
	if n != len(header) {
		return nil, fmt.Errorf(
			"incomplete WAL header write: expected %d bytes, wrote %d",
			len(header),
			n,
		)
	}

	// Synchronize the header to disk.
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync WAL header: %w", err)
	}

	success = true

	return &WAL{
		file: file,
	}, nil
}
