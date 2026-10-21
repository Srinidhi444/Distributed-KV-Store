package persistence

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// Verifies that concurrent writes survive a database restart.
func TestConcurrentWritesAndRecovery(t *testing.T) {
	const numWrites = 100

	path := filepath.Join(t.TempDir(), "store.wal")

	db, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	var wg sync.WaitGroup
	errCh := make(chan error, numWrites)

	for i := 0; i < numWrites; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)

			if err := db.Set(key, value); err != nil {
				errCh <- fmt.Errorf("set %s: %w", key, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent write failed: %v", err)
	}
	if t.Failed() {
		return
	}

	// Verify values before restarting.
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("value-%d", i)

		value, exists := db.Get(key)
		if !exists || value != expected {
			t.Fatalf(
				"before restart: key=%s, expected=%s, got=%s, exists=%v",
				key, expected, value, exists,
			)
		}
	}

	// Simulate a database restart.
	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	recoveredDB, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	defer func() {
		if err := recoveredDB.Close(); err != nil {
			t.Errorf("close recovered database: %v", err)
		}
	}()

	// Verify every write survived recovery.
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("value-%d", i)

		value, exists := recoveredDB.Get(key)
		if !exists || value != expected {
			t.Errorf(
				"after restart: key=%s, expected=%s, got=%s, exists=%v",
				key, expected, value, exists,
			)
		}
	}
}

// Verifies that concurrent updates to the same key
// recover to the final in-memory value.
func TestConcurrentUpdatesRecovery(t *testing.T) {
	const numWrites = 100

	path := filepath.Join(t.TempDir(), "store.wal")

	db, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	var wg sync.WaitGroup
	errCh := make(chan error, numWrites)

	for i := 0; i < numWrites; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			value := fmt.Sprintf("value-%d", i)

			if err := db.Set("shared-key", value); err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent update failed: %v", err)
	}
	if t.Failed() {
		return
	}

	// Capture the final value after all writes finish.
	expected, exists := db.Get("shared-key")
	if !exists {
		t.Fatal("shared-key does not exist")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	recoveredDB, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	defer func() {
		if err := recoveredDB.Close(); err != nil {
			t.Errorf("close recovered database: %v", err)
		}
	}()

	actual, exists := recoveredDB.Get("shared-key")
	if !exists {
		t.Fatal("shared-key missing after recovery")
	}

	if actual != expected {
		t.Fatalf(
			"recovery mismatch: expected %q, got %q",
			expected, actual,
		)
	}
}

// Verifies that concurrent SET and DELETE operations
// preserve the final database state across restarts.
func TestConcurrentSetDeleteRecovery(t *testing.T) {
	const numKeys = 100

	path := filepath.Join(t.TempDir(), "store.wal")

	db, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	var wg sync.WaitGroup
	errCh := make(chan error, numKeys)

	// First create all keys concurrently.
	for i := 0; i < numKeys; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", i)

			if err := db.Set(key, "value"); err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()

	// Delete all even-numbered keys concurrently.
	for i := 0; i < numKeys; i += 2 {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", i)

			if err := db.Delete(key); err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("operation failed: %v", err)
	}
	if t.Failed() {
		return
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	recoveredDB, err := OpenPersistentStore(path)
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	defer func() {
		if err := recoveredDB.Close(); err != nil {
			t.Errorf("close recovered database: %v", err)
		}
	}()

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		value, exists := recoveredDB.Get(key)

		if i%2 == 0 {
			if exists {
				t.Errorf("expected %s to remain deleted", key)
			}
		} else {
			if !exists || value != "value" {
				t.Errorf(
					"expected %s=value, got %q, exists=%v",
					key, value, exists,
				)
			}
		}
	}
}
