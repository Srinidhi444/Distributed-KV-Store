package persistence

import (
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func TestWALReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	entries := []LogEntry{
		NewSetEntry("language", "Go"),
		NewSetEntry("name", "Alex"),
		NewSetEntry("language", "Rust"),
		NewDeleteEntry("name"),
	}

	for _, entry := range entries {
		if err := wal.Append(entry); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	var recovered []LogEntry

	err = wal.Replay(func(entry LogEntry) error {
		recovered = append(recovered, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Fatalf(
			"expected %d entries, got %d",
			len(entries),
			len(recovered),
		)
	}

	for i := range entries {
		if recovered[i] != entries[i] {
			t.Errorf(
				"entry %d: expected %+v, got %+v",
				i,
				entries[i],
				recovered[i],
			)
		}
	}
}

func TestWALReplayEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	count := 0

	err = wal.Replay(func(entry LogEntry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("replay empty WAL: %v", err)
	}

	if count != 0 {
		t.Fatalf("expected zero records, got %d", count)
	}
}

func TestWALReplayTruncatedHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	// Simulate a crash halfway through the next record header.
	if _, err := wal.file.Write([]byte{0, 0, 0, 10}); err != nil {
		t.Fatal(err)
	}

	err = wal.Replay(func(LogEntry) error {
		return nil
	})

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestWALReplayTruncatedPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	record, err := EncodeRecord(NewSetEntry("key", "value"))
	if err != nil {
		t.Fatal(err)
	}

	// Write the complete record header, but only part of the payload.
	if _, err := wal.file.Write(record[:12]); err != nil {
		t.Fatal(err)
	}

	err = wal.Replay(func(LogEntry) error {
		return nil
	})

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestWALReplayMissingPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	record, err := EncodeRecord(NewSetEntry("key", "value"))
	if err != nil {
		t.Fatal(err)
	}

	// Write all nine record-header bytes, but none of the payload.
	if _, err := wal.file.Write(record[:recordHeaderSize]); err != nil {
		t.Fatal(err)
	}

	err = wal.Replay(func(LogEntry) error {
		return nil
	})

	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected wrapped EOF, got %v", err)
	}

	if err == nil {
		t.Fatal("missing payload must not be accepted as clean EOF")
	}
}

func TestWALReplayChecksumCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	record, err := EncodeRecord(NewSetEntry("key", "value"))
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the final payload byte without updating the checksum.
	record[len(record)-1] ^= 0x01

	if _, err := wal.file.Write(record); err != nil {
		t.Fatal(err)
	}

	err = wal.Replay(func(LogEntry) error {
		return nil
	})

	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}
}

func TestWALReplayCallbackError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	if err := wal.Append(NewSetEntry("key", "value")); err != nil {
		t.Fatal(err)
	}

	expected := errors.New("apply failed")

	err = wal.Replay(func(LogEntry) error {
		return expected
	})

	if !errors.Is(err, expected) {
		t.Fatalf("expected callback error, got %v", err)
	}
}

func TestWALReplayClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	err = wal.Replay(func(LogEntry) error {
		return nil
	})

	if !errors.Is(err, ErrWALClosed) {
		t.Fatalf("expected ErrWALClosed, got %v", err)
	}
}

func TestWALReplayInvalidMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	// Overwrite the magic bytes.
	if _, err := wal.file.WriteAt([]byte("BAD!"), 0); err != nil {
		t.Fatal(err)
	}

	err = wal.Replay(func(LogEntry) error {
		return nil
	})

	if err == nil {
		t.Fatal("expected invalid WAL header error")
	}
}

func TestWALReopenAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	// First database session.
	wal1, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	entries := []LogEntry{
		NewSetEntry("language", "Go"),
		NewSetEntry("name", "Alex"),
		NewDeleteEntry("language"),
	}

	for _, entry := range entries {
		if err := wal1.Append(entry); err != nil {
			t.Fatal(err)
		}
	}

	if err := wal1.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate restarting the database.
	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()

	var recovered []LogEntry

	err = wal2.Replay(func(entry LogEntry) error {
		recovered = append(recovered, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("replay reopened WAL: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Fatalf(
			"expected %d recovered records, got %d",
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
