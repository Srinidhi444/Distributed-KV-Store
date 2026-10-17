package persistence

import (
	"fmt"
	"sync"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/store"
)

type PersistentStore struct {
	db      *store.Store
	logPath string
	mu      sync.Mutex
}

func NewPersistentStore(db *store.Store, logPath string) *PersistentStore {
	return &PersistentStore{
		db:      db,
		logPath: logPath,
	}
}

func (p *PersistentStore) Set(key string, value string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	entry := NewSetEntry(key, value)

	if err := AppendEntry(p.logPath, entry); err != nil {
		return fmt.Errorf("failed to append SET operation: %w", err)
	}

	p.db.Set(key, value)

	return nil
}

func (p *PersistentStore) Get(key string) (string, bool) {
	return p.db.Get(key)
}

func (p *PersistentStore) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	entry := NewDeleteEntry(key)

	if err := AppendEntry(p.logPath, entry); err != nil {
		return fmt.Errorf("failed to append DELETE operation: %w", err)
	}

	p.db.Delete(key)

	return nil
}
