package persistence

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Creates a WAL with valid records and closes it.
func createTestWAL(t *testing.T, path string, entries []LogEntry) int64 {
	t.Helper()

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	for _, entry := range entries {
		if err := wal.Append(entry); err != nil {
			wal.Close()
			t.Fatalf("append record: %v", err)
		}
	}

	info, err := wal.file.Stat()
	if err != nil {
		wal.Close()
		t.Fatalf("stat WAL: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	return info.Size()
}

// Appends raw bytes to simulate an interrupted write.
func appendRawBytes(t *testing.T, path string, data []byte) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("open WAL for corruption: %v", err)
	}

	n, writeErr := file.Write(data)
	closeErr := file.Close()

	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatalf("append raw bytes: %v", err)
	}

	if n != len(data) {
		t.Fatalf("short raw write: %d of %d", n, len(data))
	}
}

func TestRepairWALTruncatedHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	entries := []LogEntry{
		NewSetEntry("language", "Go"),
		NewSetEntry("name", "Alex"),
	}

	lastGoodOffset := createTestWAL(t, path, entries)

	// Simulate an incomplete record header.
	appendRawBytes(t, path, []byte{0, 0, 0, 20})

	// Normal opening must reject the incomplete WAL.
	_, err := OpenWAL(path)
	if !errors.Is(err, ErrIncompleteWALRecord) {
		t.Fatalf("expected incomplete WAL error, got %v", err)
	}

	// Explicitly repair the incomplete tail.
	if err := RepairWALTail(path); err != nil {
		t.Fatalf("repair WAL: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() != lastGoodOffset {
		t.Fatalf(
			"expected repaired size %d, got %d",
			lastGoodOffset,
			info.Size(),
		)
	}

	// The repaired WAL must reopen and retain both records.
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen repaired WAL: %v", err)
	}
	defer wal.Close()

	var recovered []LogEntry

	err = wal.Replay(func(entry LogEntry) error {
		recovered = append(recovered, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("replay repaired WAL: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Fatalf(
			"expected %d records, got %d",
			len(entries),
			len(recovered),
		)
	}

	for i := range entries {
		if recovered[i] != entries[i] {
			t.Fatalf(
				"record %d: expected %+v, got %+v",
				i,
				entries[i],
				recovered[i],
			)
		}
	}
}

func TestRepairWALTruncatedPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	lastGoodOffset := createTestWAL(t, path, []LogEntry{
		NewSetEntry("key", "value"),
	})

	record, err := EncodeRecord(NewSetEntry("incomplete", "data"))
	if err != nil {
		t.Fatal(err)
	}

	// Complete 9-byte record header + partial payload.
	appendRawBytes(t, path, record[:recordHeaderSize+3])

	if err := RepairWALTail(path); err != nil {
		t.Fatalf("repair truncated payload: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() != lastGoodOffset {
		t.Fatalf(
			"expected size %d, got %d",
			lastGoodOffset,
			info.Size(),
		)
	}

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal.Close()

	count := 0

	err = wal.Replay(func(entry LogEntry) error {
		count++

		if entry != NewSetEntry("key", "value") {
			t.Errorf("unexpected recovered entry: %+v", entry)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected one valid record, got %d", count)
	}
}

func TestRepairWALChecksumCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	createTestWAL(t, path, []LogEntry{
		NewSetEntry("key", "value"),
	})

	// Corrupt the final byte of a complete record.
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatal(err)
	}

	lastByte := make([]byte, 1)

	if _, err := file.ReadAt(lastByte, info.Size()-1); err != nil {
		file.Close()
		t.Fatal(err)
	}

	lastByte[0] ^= 0x01

	writeErr := error(nil)
	_, writeErr = file.WriteAt(lastByte, info.Size()-1)
	closeErr := file.Close()

	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// A complete record with a bad checksum is not repairable.
	err = RepairWALTail(path)
	if err == nil {
		t.Fatal("expected checksum corruption error")
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(before, after) {
		t.Fatal("repair modified WAL containing checksum corruption")
	}
}

func TestRepairWALInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	createTestWAL(t, path, nil)

	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	_, writeErr := file.WriteAt([]byte("BAD!"), 0)
	closeErr := file.Close()

	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := RepairWALTail(path); err == nil {
		t.Fatal("expected invalid WAL header error")
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(before, after) {
		t.Fatal("repair modified WAL with invalid header")
	}
}

func TestRepairWALExclusiveLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	// An active WAL instance owns the file.
	err = RepairWALTail(path)

	if !errors.Is(err, ErrWALLocked) {
		t.Fatalf("expected ErrWALLocked, got %v", err)
	}
}

func TestRepairWALAlreadyValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	createTestWAL(t, path, []LogEntry{
		NewSetEntry("language", "Go"),
		NewSetEntry("name", "Alex"),
	})

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Repairing a valid WAL should be a no-op.
	if err := RepairWALTail(path); err != nil {
		t.Fatalf("repair valid WAL: %v", err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(before, after) {
		t.Fatal("repair modified an already valid WAL")
	}
}

func TestRepairWALMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.wal")

	err := RepairWALTail(path)

	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("repair unexpectedly created a WAL file")
	}
}
