package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestSetAndGet(t *testing.T) {
	db := NewStore()

	db.Set("language", "Go")

	value, exists := db.Get("language")

	if !exists {
		t.Fatal("expected key to exist")
	}

	if value != "Go" {
		t.Errorf("expected Go, got %s", value)
	}
}

func TestDelete(t *testing.T) {
	db := NewStore()

	db.Set("name", "Alex")
	db.Delete("name")

	_, exists := db.Get("name")

	if exists {
		t.Error("expected key to be deleted, but it still exists")
	}
}

func TestGetNonExistentKey(t *testing.T) {
	db := NewStore()

	value, exists := db.Get("unknown")

	if exists {
		t.Error("expected key 'unknown' to not exist")
	}

	if value != "" {
		t.Errorf("expected empty string, got %q", value)
	}
}

func TestConcurrentAccess(t *testing.T) {
	db := NewStore()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", id)
			value := fmt.Sprintf("value-%d", id)

			// Write to the store
			db.Set(key, value)

			// Read from the store
			got, exists := db.Get(key)

			if !exists {
				t.Errorf("key %s does not exist", key)
				return
			}

			if got != value {
				t.Errorf("expected %s, got %s", value, got)
			}
		}(i)
	}

	wg.Wait()
}
