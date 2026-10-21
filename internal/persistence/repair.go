package persistence

import (
	"errors"
	"fmt"
	"os"

	"github.com/gofrs/flock"
)

func RepairWALTail(path string) (retErr error) {
	// Step 1: Acquire exclusive ownership of the WAL.
	fileLock := flock.New(path + ".lock")

	locked, err := fileLock.TryLock()
	if err != nil {
		return fmt.Errorf("acquire WAL lock: %w", err)
	}

	if !locked {
		return fmt.Errorf("%w: %s", ErrWALLocked, path)
	}

	// Always release the lock, even on success.
	defer func() {
		retErr = errors.Join(retErr, fileLock.Unlock())
	}()

	// Step 2: Open the existing WAL without creating it.
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("open existing WAL: %w", err)
	}

	// Always close the file before releasing the lock.
	defer func() {
		retErr = errors.Join(retErr, file.Close())
	}()

	// Step 3: Validate the entire WAL.
	err = validateExistingWAL(file)

	// A valid WAL requires no repair.
	if err == nil {
		return nil
	}

	// Step 4: Extract the structured recovery error.
	var recoveryErr *WALRecoveryError

	if !errors.As(err, &recoveryErr) {
		return fmt.Errorf("WAL cannot be repaired: %w", err)
	}

	// Step 5: Only incomplete records are repair candidates.
	if !errors.Is(err, ErrIncompleteWALRecord) {
		return fmt.Errorf("WAL corruption is not repairable: %w", err)
	}

	// Step 6: Validate the truncation offset.
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat WAL: %w", err)
	}

	offset := recoveryErr.LastGoodOffset

	if offset < int64(len(EncodeWALHeader())) ||
		offset >= info.Size() {
		return fmt.Errorf(
			"invalid WAL repair offset %d for file size %d",
			offset,
			info.Size(),
		)
	}

	// Step 7: Remove the incomplete final record.
	if err := file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate WAL: %w", err)
	}

	// Step 8: Synchronize the repaired WAL.
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync repaired WAL: %w", err)
	}

	return nil
}
