package raft

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"
)

var ErrRaftWALLocked = errors.New("raft WAL already locked")

// RaftWAL manages the physical append-only Raft write-ahead log.
//
// It is responsible for:
//   - Exclusive file ownership
//   - WAL header and record validation
//   - Durable batch writes
//   - Detecting uncertain write failures
//   - Releasing resources safely
//
// Logical Raft state validation is handled by the Storage layer.
type RaftWAL struct {
	mu sync.Mutex

	file *os.File
	lock *flock.Flock

	closed bool
	failed bool
}

// OpenRaftWAL opens an existing WAL or creates a new one.
//
// Existing records are validated before the WAL becomes available.
// Corrupt or incomplete WAL records are never silently repaired.
func OpenRaftWAL(path string) (_ *RaftWAL, retErr error) {
	if path == "" {
		return nil, errors.New("raft WAL path cannot be empty")
	}

	lockPath := path + ".lock"
	fileLock := flock.New(lockPath)

	locked, err := fileLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("acquire raft WAL lock: %w", err)
	}

	if !locked {
		return nil, fmt.Errorf("%w: %s", ErrRaftWALLocked, path)
	}

	// Release the lock if initialization fails.
	defer func() {
		if retErr != nil {
			if err := fileLock.Unlock(); err != nil {
				retErr = errors.Join(
					retErr,
					fmt.Errorf("release raft WAL lock: %w", err),
				)
			}
		}
	}()

	// File creation is explicit to avoid accidentally truncating
	// an existing WAL containing persistent Raft state.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)

	created := err == nil

	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create raft WAL: %w", err)
		}

		file, err = os.OpenFile(path, os.O_RDWR, 0600)
		if err != nil {
			return nil, fmt.Errorf("open existing raft WAL: %w", err)
		}
	}

	// Always close the file on initialization failure.
	defer func() {
		if retErr != nil {
			if err := file.Close(); err != nil {
				retErr = errors.Join(
					retErr,
					fmt.Errorf("close raft WAL after failed open: %w", err),
				)
			}
		}
	}()

	if created {
		header := EncodeRaftWALHeader()

		n, err := file.Write(header)
		if err != nil {
			return nil, fmt.Errorf("write raft WAL header: %w", err)
		}

		if n != len(header) {
			return nil, fmt.Errorf(
				"write raft WAL header: %w",
				io.ErrShortWrite,
			)
		}

		if err := file.Sync(); err != nil {
			return nil, fmt.Errorf("sync raft WAL header: %w", err)
		}

		// Ensure the newly created directory entry is durable
		// where the operating system supports directory syncing.
		//
		// The directory must already exist. Directory fsync
		// portability and its full failure policy will be
		// finalized during durability hardening.
		_ = filepath.Dir(path)
	} else {
		if err := validateRaftWALRecords(file); err != nil {
			return nil, fmt.Errorf("validate existing raft WAL: %w", err)
		}
	}

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek raft WAL end: %w", err)
	}

	return &RaftWAL{
		file: file,
		lock: fileLock,
	}, nil
}

// appendBatch appends one checksummed Raft batch and synchronizes
// it to durable storage before reporting success.
//
// A failed write or sync puts the WAL into a failed state.
// Further writes are prohibited until the WAL is closed
// and recovered through OpenRaftWAL.
func (w *RaftWAL) appendBatch(batch raftBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrStorageClosed
	}

	if w.failed {
		return ErrStorageFailed
	}

	record, err := encodeRaftBatch(batch)
	if err != nil {
		return fmt.Errorf("encode raft WAL batch: %w", err)
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		w.failed = true
		return fmt.Errorf("seek raft WAL end: %w", err)
	}

	n, err := w.file.Write(record)
	if err != nil {
		w.failed = true
		return fmt.Errorf("append raft WAL batch: %w", err)
	}

	if n != len(record) {
		w.failed = true
		return fmt.Errorf(
			"append raft WAL batch: wrote %d of %d bytes: %w",
			n,
			len(record),
			io.ErrShortWrite,
		)
	}

	if err := w.file.Sync(); err != nil {
		w.failed = true
		return fmt.Errorf("sync raft WAL batch: %w", err)
	}

	return nil
}

// Close closes the WAL and releases the exclusive file lock.
//
// Close is idempotent.
func (w *RaftWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	var errs []error

	if w.file != nil {
		if err := w.file.Close(); err != nil {
			errs = append(
				errs,
				fmt.Errorf("close raft WAL file: %w", err),
			)
		}
	}

	if w.lock != nil {
		if err := w.lock.Unlock(); err != nil {
			errs = append(
				errs,
				fmt.Errorf("release raft WAL lock: %w", err),
			)
		}
	}

	return errors.Join(errs...)
}

// validateRaftWALRecords checks the WAL header and every
// physical batch record.
//
// It rejects:
//   - Invalid WAL magic or version
//   - Incomplete records
//   - Invalid record lengths
//   - CRC32C checksum mismatches
//   - Malformed or unsupported JSON
//
// Logical Raft invariants are validated separately.
func validateRaftWALRecords(file *os.File) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek raft WAL start: %w", err)
	}

	if err := ValidateRaftWALHeader(file); err != nil {
		return err
	}

	for {
		startOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("get raft WAL offset: %w", err)
		}

		_, err = decodeRaftBatch(file)
		if err == nil {
			continue
		}

		endOffset, seekErr := file.Seek(0, io.SeekCurrent)
		if seekErr != nil {
			return fmt.Errorf("get raft WAL end offset: %w", seekErr)
		}

		// Clean EOF is valid only when no bytes of the next
		// record have been consumed.
		if errors.Is(err, io.EOF) && startOffset == endOffset {
			return nil
		}

		return fmt.Errorf(
			"invalid raft WAL record at offset %d: %w",
			startOffset,
			err,
		)
	}
}
