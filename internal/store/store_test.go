package store

import "testing"

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
