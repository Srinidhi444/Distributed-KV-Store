package persistence

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestWALAppendAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	if err := wal.Append(
		NewSetEntry("language", "Go"),
	); err != nil {
		t.Fatalf("append SET: %v", err)
	}

	if err := wal.Append(
		NewDeleteEntry("language"),
	); err != nil {
		t.Fatalf("append DELETE: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	// Appending after closure must fail.
	err = wal.Append(NewSetEntry("name", "Alex"))

	if !errors.Is(err, ErrWALClosed) {
		t.Fatalf(
			"expected ErrWALClosed, got %v",
			err,
		)
	}
}
