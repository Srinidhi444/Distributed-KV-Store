package persistence

import (
	"fmt"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/store"
)

func OpenPersistentStore(walPath string) (*PersistentStore, error) {
	// Step 1: Open or create the WAL.
	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	// Step 2: Close the WAL if initialization fails.
	success := false
	defer func() {
		if !success {
			_ = wal.Close()
		}
	}()

	// Step 3: Create a fresh in-memory store.
	db := store.NewStore()

	// Step 4: Replay all persisted operations.
	err = wal.Replay(func(entry LogEntry) error {
		switch entry.Operation {
		case "set":
			db.Set(entry.Key, entry.Value)

		case "delete":
			db.Delete(entry.Key)

		default:
			return fmt.Errorf(
				"unknown WAL operation: %q",
				entry.Operation,
			)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("recover database from WAL: %w", err)
	}

	// Step 5: Construct the persistent database.
	persistentDB := newPersistentStore(db, wal)

	success = true

	return persistentDB, nil
}
