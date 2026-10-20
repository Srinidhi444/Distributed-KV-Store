package persistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
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

func TestValidateWALHeader(t *testing.T) {
	header := EncodeWALHeader()

	err := ValidateWALHeader(bytes.NewReader(header))
	if err != nil {
		t.Fatalf("expected valid header: %v", err)
	}
}

func TestInvalidWALMagic(t *testing.T) {
	header := EncodeWALHeader()
	copy(header[:4], "BAD!")

	err := ValidateWALHeader(bytes.NewReader(header))
	if err == nil {
		t.Fatal("expected invalid magic error")
	}
}

func TestUnsupportedWALVersion(t *testing.T) {
	header := EncodeWALHeader()

	binary.BigEndian.PutUint32(header[4:8], 999)

	err := ValidateWALHeader(bytes.NewReader(header))
	if err == nil {
		t.Fatal("expected unsupported version error")
	}
}

func TestTruncatedWALHeader(t *testing.T) {
	header := EncodeWALHeader()

	err := ValidateWALHeader(bytes.NewReader(header[:5]))
	if err == nil {
		t.Fatal("expected truncated header error")
	}
}

func TestWALFileHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, EncodeWALHeader()) {
		t.Fatalf("unexpected WAL header: %x", data)
	}
}

func TestWALExclusiveLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	// First instance acquires ownership.
	wal1, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("first OpenWAL failed: %v", err)
	}

	// Second instance must not acquire the same lock.
	_, err = OpenWAL(path)
	if err == nil {
		t.Fatal("expected second OpenWAL to fail")
	}

	// Release ownership.
	if err := wal1.Close(); err != nil {
		t.Fatalf("close first WAL: %v", err)
	}

	// Retry after releasing the lock.
	// Opening still fails because O_EXCL rejects existing files.
	// Here we're testing that the error is no longer a lock conflict.
	_, err = OpenWAL(path)
	if err == nil {
		t.Fatal("expected existing-file error")
	}

	if !strings.Contains(err.Error(), "file exists") {
		t.Fatalf("expected existing-file error, got: %v", err)
	}
}
