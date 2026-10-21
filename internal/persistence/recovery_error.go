package persistence

import (
	"errors"
	"fmt"
	"io"
)

var ErrIncompleteWALRecord = errors.New("incomplete WAL record")

type WALRecoveryError struct {
	Offset         int64
	LastGoodOffset int64
	Cause          error
}

func (e *WALRecoveryError) Error() string {
	return fmt.Sprintf(
		"WAL recovery failed at offset %d (last good offset %d): %v",
		e.Offset,
		e.LastGoodOffset,
		e.Cause,
	)
}

func (e *WALRecoveryError) Unwrap() error {
	return e.Cause
}

func isIncompleteRecordError(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}
