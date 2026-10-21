package persistence

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWALIncompleteTailReportsOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := wal.Append(NewSetEntry("name", "Alex")); err != nil {
		t.Fatal(err)
	}

	info, err := wal.file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	lastGoodOffset := info.Size()

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate an interrupted append.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}

	_, writeErr := file.Write([]byte{0, 0, 0, 20})
	closeErr := file.Close()

	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatal(err)
	}

	// Reopening must reject the incomplete WAL.
	_, err = OpenWAL(path)

	if !errors.Is(err, ErrIncompleteWALRecord) {
		t.Fatalf("expected incomplete record error, got %v", err)
	}

	var recoveryErr *WALRecoveryError
	if !errors.As(err, &recoveryErr) {
		t.Fatalf("expected WALRecoveryError, got %v", err)
	}

	if recoveryErr.LastGoodOffset != lastGoodOffset {
		t.Fatalf(
			"expected last good offset %d, got %d",
			lastGoodOffset,
			recoveryErr.LastGoodOffset,
		)
	}

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected underlying unexpected EOF, got %v", err)
	}
}
