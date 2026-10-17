package persistence

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
