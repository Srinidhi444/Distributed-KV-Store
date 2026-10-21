package persistence

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/store"
)

// Replays WAL operations into a fresh in-memory store.
func replayIntoStore(wal *WAL, db *store.Store) error {
	return wal.Replay(func(entry LogEntry) error {
		switch entry.Operation {
		case "set":
			db.Set(entry.Key, entry.Value)
		case "delete":
			db.Delete(entry.Key)
		default:
			return errors.New("unknown WAL operation")
		}
		return nil
	})
}

// Test 1: SET operations must survive a restart.
func TestPersistenceRecovery(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "store.wal")

	wal1, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	db1 := store.NewStore()
	persistentDB := newPersistentStore(db1, wal1)

	if err := persistentDB.Set("language", "Go"); err != nil {
		t.Fatal(err)
	}

	if err := persistentDB.Set("name", "Alex"); err != nil {
		t.Fatal(err)
	}

	if err := wal1.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a restart.
	wal2, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()

	db2 := store.NewStore()

	if err := replayIntoStore(wal2, db2); err != nil {
		t.Fatalf("recover database: %v", err)
	}

	value, exists := db2.Get("language")
	if !exists || value != "Go" {
		t.Fatalf("expected language=Go, got %q, exists=%v", value, exists)
	}

	name, exists := db2.Get("name")
	if !exists || name != "Alex" {
		t.Fatalf("expected name=Alex, got %q, exists=%v", name, exists)
	}
}

// Test 2: Deleted keys must remain deleted after restart.
func TestDeleteRecovery(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "store.wal")

	wal1, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	db1 := store.NewStore()
	persistentDB := newPersistentStore(db1, wal1)

	if err := persistentDB.Set("language", "Go"); err != nil {
		t.Fatal(err)
	}

	if err := persistentDB.Delete("language"); err != nil {
		t.Fatal(err)
	}

	if err := wal1.Close(); err != nil {
		t.Fatal(err)
	}

	wal2, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	db2 := store.NewStore()

	if err := replayIntoStore(wal2, db2); err != nil {
		t.Fatal(err)
	}

	if _, exists := db2.Get("language"); exists {
		t.Fatal("expected language to remain deleted")
	}
}

// Test 3: Updates must replay in the correct order.
func TestUpdateRecovery(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "store.wal")

	wal1, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	db1 := store.NewStore()
	persistentDB := newPersistentStore(db1, wal1)

	if err := persistentDB.Set("language", "Go"); err != nil {
		t.Fatal(err)
	}

	if err := persistentDB.Set("language", "Rust"); err != nil {
		t.Fatal(err)
	}

	if err := wal1.Close(); err != nil {
		t.Fatal(err)
	}

	wal2, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	db2 := store.NewStore()

	if err := replayIntoStore(wal2, db2); err != nil {
		t.Fatal(err)
	}

	value, exists := db2.Get("language")
	if !exists || value != "Rust" {
		t.Fatalf("expected language=Rust, got %q, exists=%v", value, exists)
	}
}

// Test 4: Opening a nonexistent WAL should create a valid empty WAL.
func TestMissingLogFile(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "missing.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("create missing WAL: %v", err)
	}
	defer wal.Close()

	db := store.NewStore()

	if err := replayIntoStore(wal, db); err != nil {
		t.Fatalf("replay empty WAL: %v", err)
	}

	if _, exists := db.Get("language"); exists {
		t.Fatal("expected database to remain empty")
	}

	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("expected WAL file to exist: %v", err)
	}
}

// Test 5: Corrupted WAL records must cause recovery to fail.
func TestInvalidLogEntry(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := wal.Append(NewSetEntry("language", "Go")); err != nil {
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte in the first record's checksum.
	file, err := os.OpenFile(walPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	checksumByte := []byte{0}

	if _, err := file.ReadAt(checksumByte, 12); err != nil {
		file.Close()
		t.Fatal(err)
	}

	checksumByte[0] ^= 0xFF

	if _, err := file.WriteAt(checksumByte, 12); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	// Opening must reject the corrupted WAL.
	if _, err := OpenWAL(walPath); err == nil {
		t.Fatal("expected error for corrupted WAL")
	}
}

// Test 6: Failed persistence must not modify the in-memory store.
func TestAppendFailure(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	db := store.NewStore()
	persistentDB := newPersistentStore(db, wal)

	// Closing the WAL forces subsequent appends to fail.
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	err = persistentDB.Set("language", "Go")
	if !errors.Is(err, ErrWALClosed) {
		t.Fatalf("expected ErrWALClosed, got %v", err)
	}

	if _, exists := db.Get("language"); exists {
		t.Fatal("failed append must not modify in-memory store")
	}
}
