package persistence

import (
	"bytes"
	"testing"
)

func TestRecordRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		entry LogEntry
	}{
		{
			name:  "SET operation",
			entry: NewSetEntry("language", "Go"),
		},
		{
			name:  "DELETE operation",
			entry: NewDeleteEntry("language"),
		},
		{
			name:  "empty value",
			entry: NewSetEntry("empty", ""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeRecord(tt.entry)
			if err != nil {
				t.Fatalf("encoding failed: %v", err)
			}

			decoded, err := DecodeRecord(
				bytes.NewReader(encoded),
			)
			if err != nil {
				t.Fatalf("decoding failed: %v", err)
			}

			if decoded != tt.entry {
				t.Errorf(
					"expected %+v, got %+v",
					tt.entry,
					decoded,
				)
			}
		})
	}
}
