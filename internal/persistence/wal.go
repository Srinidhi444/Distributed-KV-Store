package persistence

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/gofrs/flock"
)

type WAL struct {
	file   *os.File
	mu     sync.Mutex
	failed bool
	closed bool

	lock *flock.Flock
}

const (
	WALMagic   = "DKV1"
	WALVersion = uint32(1)

	MaxPayloadSize = 1024 * 1024
)

var (
	ErrWALClosed = errors.New("WAL is closed")
	ErrWALFailed = errors.New("WAL is in a failed state")
	ErrWALLocked = errors.New("WAL IS LOCKED")
)

func EncodeWALHeader() []byte {
	header := make([]byte, 8)

	copy(header[0:4], WALMagic)

	binary.BigEndian.PutUint32(header[4:8], WALVersion)

	return header
}

func OpenWAL(path string) (*WAL, error) {
	fileLock := flock.New(path + ".lock")

	locked, err := fileLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("acquire WAL lock: %w", err)
	}

	if !locked {
		return nil, fmt.Errorf("%w: %s", ErrWALLocked, path)
	}

	success := false

	defer func() {
		if !success {
			_ = fileLock.Unlock()
		}
	}()

	// Try to create a brand-new WAL file.
	file, err := os.OpenFile(
		path,
		os.O_RDWR|os.O_CREATE|os.O_EXCL,
		0600,
	)

	newFile := err == nil

	if errors.Is(err, os.ErrExist) {
		// Existing WAL: open without truncating it.
		file, err = os.OpenFile(path, os.O_RDWR, 0600)
	}

	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	defer func() {
		if !success {
			_ = file.Close()
		}
	}()

	if newFile {
		header := EncodeWALHeader()

		n, err := file.Write(header)
		if err != nil {
			return nil, fmt.Errorf("write WAL header: %w", err)
		}

		if n != len(header) {
			return nil, fmt.Errorf(
				"short WAL header write: wrote %d of %d bytes",
				n,
				len(header),
			)
		}

		if err := file.Sync(); err != nil {
			return nil, fmt.Errorf("sync WAL header: %w", err)
		}
	} else {
		// Existing WAL must be fully valid before new appends.
		if err := validateExistingWAL(file); err != nil {
			return nil, fmt.Errorf("open existing WAL: %w", err)
		}
	}

	// Position the cursor for future appends.
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek WAL end: %w", err)
	}

	success = true

	return &WAL{
		file: file,
		lock: fileLock,
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

	if err != nil {
		w.failed = true
		return fmt.Errorf("write WAL record: %w", err)
	}

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

	w.closed = true

	fileErr := w.file.Close()

	var lockErr error
	if w.lock != nil {
		lockErr = w.lock.Unlock()
	}

	return errors.Join(fileErr, lockErr)
}

func ValidateWALHeader(r io.Reader) error {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return fmt.Errorf("read WAL header:%w", err)
	}
	magic := string(header[0:4])
	if magic != WALMagic {
		return fmt.Errorf(
			"invalid wal magic:expected %q,got %q",
			WALMagic,
			magic,
		)
	}
	version := binary.BigEndian.Uint32(header[4:8])

	if version != WALVersion {
		return fmt.Errorf(
			"unsupported Wal version: %d",
			version,
		)
	}
	return nil
}

func validateExistingWAL(file *os.File) error {
	// Step 1: Move to the beginning of the WAL.
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek WAL start: %w", err)
	}

	// Step 2: Validate the existing 8-byte WAL header.
	if err := ValidateWALHeader(file); err != nil {
		return fmt.Errorf("validate WAL header: %w", err)
	}

	// Step 3: Validate every record.
	for {
		startOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("get WAL offset: %w", err)
		}

		// DecodeRecord also verifies CRC32C and record validity.
		_, err = DecodeRecord(file)

		if err == nil {
			continue
		}

		// Step 4: Distinguish clean EOF from truncated data.
		if errors.Is(err, io.EOF) {
			endOffset, seekErr := file.Seek(0, io.SeekCurrent)
			if seekErr != nil {
				return fmt.Errorf("get WAL end offset: %w", seekErr)
			}

			// EOF without consuming bytes means normal end of file.
			if startOffset == endOffset {
				return nil
			}
		}

		// Any other error means the WAL is invalid.
		if isIncompleteRecordError(err) {
			return &WALRecoveryError{
				Offset:         startOffset,
				LastGoodOffset: startOffset,
				Cause: fmt.Errorf(
					"%w: %w",
					ErrIncompleteWALRecord,
					err,
				),
			}
		}

		return &WALRecoveryError{
			Offset:         startOffset,
			LastGoodOffset: startOffset,
			Cause:          err,
		}
	}
}
