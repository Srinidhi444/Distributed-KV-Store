package persistence

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestWALWriteFailurePoisonsWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	writeErr := errors.New("simulated disk write failure")

	wal.writeRecord = func(data []byte) (int, error) {
		return 0, writeErr
	}

	err = wal.Append(NewSetEntry("key", "value"))

	if !errors.Is(err, writeErr) {
		t.Fatalf("expected write failure, got %v", err)
	}

	if !wal.failed {
		t.Fatal("WAL must enter failed state")
	}

	err = wal.Append(NewSetEntry("another", "value"))

	if !errors.Is(err, ErrWALFailed) {
		t.Fatalf("expected ErrWALFailed, got %v", err)
	}
}

func TestWALShortWritePoisonsWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	wal.writeRecord = func(data []byte) (int, error) {
		return wal.file.Write(data[:len(data)/2])
	}

	err = wal.Append(NewSetEntry("key", "value"))

	if err == nil || !strings.Contains(err.Error(), "short WAL write") {
		t.Fatalf("expected short write error, got %v", err)
	}

	if !wal.failed {
		t.Fatal("WAL must enter failed state after short write")
	}

	err = wal.Append(NewSetEntry("another", "value"))

	if !errors.Is(err, ErrWALFailed) {
		t.Fatalf("expected ErrWALFailed, got %v", err)
	}
}

func TestWALSyncFailurePoisonsWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	syncErr := errors.New("simulated sync failure")

	wal.syncRecord = func() error {
		return syncErr
	}

	err = wal.Append(NewSetEntry("key", "value"))

	if !errors.Is(err, syncErr) {
		t.Fatalf("expected sync failure, got %v", err)
	}

	if !wal.failed {
		t.Fatal("WAL must enter failed state")
	}

	err = wal.Append(NewDeleteEntry("key"))

	if !errors.Is(err, ErrWALFailed) {
		t.Fatalf("expected ErrWALFailed, got %v", err)
	}
}

func TestPersistentStoreWriteFailurePreservesMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	db, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set("key", "original"); err != nil {
		t.Fatal(err)
	}

	writeErr := fmt.Errorf("simulated I/O error")

	db.wal.writeRecord = func([]byte) (int, error) {
		return 0, writeErr
	}

	err = db.Set("key", "changed")

	if !errors.Is(err, writeErr) {
		t.Fatalf("expected write error, got %v", err)
	}

	value, exists := db.Get("key")
	if !exists || value != "original" {
		t.Fatalf(
			"failed SET changed memory: value=%q, exists=%v",
			value,
			exists,
		)
	}

	err = db.Delete("key")

	if !errors.Is(err, ErrWALFailed) {
		t.Fatalf("expected ErrWALFailed, got %v", err)
	}
}
