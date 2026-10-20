package persistence

import (
	"errors"
	"fmt"
	"io"
)

func (w *WAL) Replay(apply func(LogEntry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Step 1: Check WAL state
	if w.closed {
		return ErrWALClosed
	}

	if w.failed {
		return ErrWALFailed
	}

	if apply == nil {
		return fmt.Errorf("replay callback cannot be nil")
	}

	// Step 2: Move to the beginning of the file
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek WAL start: %w", err)
	}

	// Step 3: Validate the WAL file header
	if err := ValidateWALHeader(w.file); err != nil {
		return fmt.Errorf("invalid WAL header: %w", err)
	}

	// Step 4: Read and apply records one by one
	for {
		// Remember where the next record starts
		startOffset, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("get WAL offset: %w", err)
		}

		// Decode the next record
		entry, err := DecodeRecord(w.file)

		if err != nil {
			// EOF is valid only if no bytes were consumed
			if errors.Is(err, io.EOF) {
				endOffset, seekErr := w.file.Seek(0, io.SeekCurrent)
				if seekErr != nil {
					return fmt.Errorf("get WAL end offset: %w", seekErr)
				}

				if startOffset == endOffset {
					return nil
				}
			}

			return fmt.Errorf(
				"decode WAL record at offset %d: %w",
				startOffset,
				err,
			)
		}

		// Step 5: Apply the recovered operation
		if err := apply(entry); err != nil {
			return fmt.Errorf(
				"apply WAL record at offset %d: %w",
				startOffset,
				err,
			)
		}
	}
}
