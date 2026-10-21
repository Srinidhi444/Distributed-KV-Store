package persistence

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDatabaseRejectsCorruptedChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	db, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Set("name", "Alex"); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte in the record's checksum.
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	// 8-byte WAL header + 4-byte record length.
	const checksumOffset = 12

	b := []byte{0}
	if _, err := file.ReadAt(b, checksumOffset); err != nil {
		file.Close()
		t.Fatal(err)
	}

	b[0] ^= 0xFF

	_, writeErr := file.WriteAt(b, checksumOffset)
	closeErr := file.Close()

	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenPersistentStore(path)
	if err == nil {
		recovered.Close()
		t.Fatal("expected corrupted WAL to prevent startup")
	}
}

func TestDatabaseRejectsIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	db, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Set("language", "Go"); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash during the next record header.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}

	_, writeErr := file.Write([]byte{0, 0, 0, 25})
	closeErr := file.Close()

	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenPersistentStore(path)
	if err == nil {
		recovered.Close()
		t.Fatal("expected incomplete WAL to prevent startup")
	}

	if !errors.Is(err, ErrIncompleteWALRecord) {
		t.Fatalf("expected ErrIncompleteWALRecord, got %v", err)
	}

	// Recovery failure must release the WAL lock.
	if err := RepairWALTail(path); err != nil {
		t.Fatalf("expected explicit repair to succeed: %v", err)
	}

	// Previously durable records must survive repair.
	recovered, err = OpenPersistentStore(path)
	if err != nil {
		t.Fatalf("open repaired database: %v", err)
	}
	defer recovered.Close()

	value, exists := recovered.Get("language")
	if !exists || value != "Go" {
		t.Fatalf("expected language=Go after repair, got %q", value)
	}
}

func TestDatabaseRejectsInvalidMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	db, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	_, writeErr := file.WriteAt([]byte("BAD!"), 0)
	closeErr := file.Close()

	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenPersistentStore(path)
	if err == nil {
		recovered.Close()
		t.Fatal("expected invalid WAL magic to prevent startup")
	}
}

func TestDatabaseRejectsUnsupportedVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	db, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	version := make([]byte, 4)
	binary.BigEndian.PutUint32(version, 999)

	_, writeErr := file.WriteAt(version, 4)
	closeErr := file.Close()

	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenPersistentStore(path)
	if err == nil {
		recovered.Close()
		t.Fatal("expected unsupported WAL version to prevent startup")
	}
}

func TestDatabaseExclusiveOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	db1, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatal(err)
	}

	// Two databases must not own the same WAL simultaneously.
	db2, err := OpenPersistentStore(path)
	if err == nil {
		db2.Close()
		t.Fatal("expected WAL lock conflict")
	}

	if !errors.Is(err, ErrWALLocked) {
		t.Fatalf("expected ErrWALLocked, got %v", err)
	}

	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	// Closing the first database releases ownership.
	db3, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatalf("expected reopen after close: %v", err)
	}
	defer db3.Close()
}

func TestDatabaseRejectsWritesAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	db, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Set("key", "original"); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Set("key", "changed"); !errors.Is(err, ErrWALClosed) {
		t.Fatalf("expected ErrWALClosed for SET, got %v", err)
	}

	if err := db.Delete("key"); !errors.Is(err, ErrWALClosed) {
		t.Fatalf("expected ErrWALClosed for DELETE, got %v", err)
	}

	value, exists := db.Get("key")
	if !exists || value != "original" {
		t.Fatalf("failed write modified memory: %q", value)
	}
}

func TestInvalidEntryDoesNotModifyWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	// An invalid operation must fail before writing bytes.
	err = wal.Append(LogEntry{
		Operation: "unknown",
		Key:       "language",
		Value:     "Go",
	})

	if err == nil {
		t.Fatal("expected invalid operation to fail")
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if after.Size() != before.Size() {
		t.Fatal("invalid operation changed WAL file size")
	}

	// An encoding error must not poison a healthy WAL.
	if err := wal.Append(NewSetEntry("language", "Go")); err != nil {
		t.Fatalf("valid append after invalid entry failed: %v", err)
	}
}
