package persistence

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Srinidhi444/Distributed-KV-Store/internal/store"
)

type LogEntry struct {
	Operation string `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
}

func NewSetEntry(key string, value string) LogEntry {
	entry := LogEntry{
		Operation: "set",
		Key:       key,
		Value:     value,
	}
	return entry
}

func NewDeleteEntry(key string) LogEntry {
	return LogEntry{
		Operation: "delete",
		Key:       key,
	}
}

func AppendEntry(path string, entry LogEntry) error {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
	}

	defer file.Close()

	encoder := json.NewEncoder(file)

	if err := encoder.Encode(entry); err != nil {
		return err
	}

	return file.Sync()
}

func ReplayLog(path string, db *store.Store) error {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	for {
		var entry LogEntry

		err := decoder.Decode(&entry)

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		if entry.Key == "" {
			return fmt.Errorf("log entry contains empty key")
		}

		switch entry.Operation {
		case "set":
			db.Set(entry.Key, entry.Value)

		case "delete":
			db.Delete(entry.Key)

		default:
			return fmt.Errorf(
				"unknown log operation: %s",
				entry.Operation,
			)
		}
	}

	return nil
}
