package persistence

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/store"
)

// Test 1: Verify SET operations survive recovery.
func TestPersistenceRecovery(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "store.log")

	db1 := store.NewStore()
	persistentDB := NewPersistentStore(db1, logPath)

	if err := persistentDB.Set("language", "Go"); err != nil {
		t.Fatalf("failed to set language: %v", err)
	}

	if err := persistentDB.Set("name", "Alex"); err != nil {
		t.Fatalf("failed to set name: %v", err)
	}

	// Simulate a restart using a new in-memory store.
	db2 := store.NewStore()

	if err := ReplayLog(logPath, db2); err != nil {
		t.Fatalf("failed to replay log: %v", err)
	}

	value, exists := db2.Get("language")
	if !exists {
		t.Fatal("expected language to exist after recovery")
	}

	if value != "Go" {
		t.Errorf("expected Go, got %q", value)
	}

	name, exists := db2.Get("name")
	if !exists {
		t.Fatal("expected name to exist after recovery")
	}

	if name != "Alex" {
		t.Errorf("expected Alex, got %q", name)
	}
}

// Test 2: Verify deleted keys remain deleted after recovery.
func TestDeleteRecovery(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "store.log")

	db1 := store.NewStore()
	persistentDB := NewPersistentStore(db1, logPath)

	if err := persistentDB.Set("language", "Go"); err != nil {
		t.Fatalf("failed to set language: %v", err)
	}

	if err := persistentDB.Delete("language"); err != nil {
		t.Fatalf("failed to delete language: %v", err)
	}

	db2 := store.NewStore()

	if err := ReplayLog(logPath, db2); err != nil {
		t.Fatalf("failed to replay log: %v", err)
	}

	_, exists := db2.Get("language")
	if exists {
		t.Error("expected language to remain deleted after recovery")
	}
}

// Test 3: Verify updates are replayed in the correct order.
func TestUpdateRecovery(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "store.log")

	db1 := store.NewStore()
	persistentDB := NewPersistentStore(db1, logPath)

	if err := persistentDB.Set("language", "Go"); err != nil {
		t.Fatalf("failed to set initial value: %v", err)
	}

	if err := persistentDB.Set("language", "Rust"); err != nil {
		t.Fatalf("failed to update value: %v", err)
	}

	db2 := store.NewStore()

	if err := ReplayLog(logPath, db2); err != nil {
		t.Fatalf("failed to replay log: %v", err)
	}

	value, exists := db2.Get("language")
	if !exists {
		t.Fatal("expected language to exist after recovery")
	}

	if value != "Rust" {
		t.Errorf("expected Rust, got %q", value)
	}
}

// Test 4: A missing log file should not cause recovery to fail.
func TestMissingLogFile(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "missing.log")

	db := store.NewStore()

	if err := ReplayLog(logPath, db); err != nil {
		t.Fatalf("expected no error for missing log, got %v", err)
	}

	_, exists := db.Get("language")
	if exists {
		t.Error("expected database to remain empty")
	}
}

// Test 5: Invalid log operations should cause recovery to fail.
func TestInvalidLogEntry(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "store.log")

	data := []byte(
		"{\"op\":\"unknown\",\"key\":\"language\",\"value\":\"Go\"}\n",
	)

	if err := os.WriteFile(logPath, data, 0644); err != nil {
		t.Fatalf("failed to create log file: %v", err)
	}

	db := store.NewStore()

	if err := ReplayLog(logPath, db); err == nil {
		t.Fatal("expected error for unknown log operation")
	}
}

// Test 6: Failed appends must not modify the in-memory database.
func TestAppendFailure(t *testing.T) {
	logPath := filepath.Join(
		t.TempDir(),
		"nonexistent-directory",
		"store.log",
	)

	db := store.NewStore()
	persistentDB := NewPersistentStore(db, logPath)

	err := persistentDB.Set("language", "Go")
	if err == nil {
		t.Fatal("expected append operation to fail")
	}

	_, exists := db.Get("language")
	if exists {
		t.Error("failed append should not modify in-memory store")
	}
}
