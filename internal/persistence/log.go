package persistence

import (
	"encoding/json"
	"os"
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
