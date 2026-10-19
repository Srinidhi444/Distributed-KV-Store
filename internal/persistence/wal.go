package persistence

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

var (
	ErrWALClosed = errors.New("WAL is closed")
	ErrWALFailed = errors.New("WAL is in a failed state")
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

func (w *WAL) Append(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}
	if w.failed {
		return ErrWALFailed
	}

	record, err := EncodeRecord(entry)
	if err != nil {
		return fmt.Errorf("encode wal record:%w", err)
	}
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		w.failed = true
		return fmt.Errorf("seek wal end:%w", err)
	}
	n, err := w.file.Write(record)
	if n != len(record) {
		w.failed = true
		return fmt.Errorf(
			"short WAL write: wrote %d of %d bytes",
			n,
			len(record),
		)
	}
	// Request synchronization with persistent storage.
	if err := w.file.Sync(); err != nil {
		w.failed = true
		return fmt.Errorf("sync WAL record: %w", err)
	}

	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	// Mark closed before releasing the underlying file.
	w.closed = true

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}

	return nil
}
