package persistence

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
)

const (
	RecordTypeSet    byte = 1
	RecordTypeDelete byte = 2

	recordHeaderSize = 9
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func EncodeRecord(entry LogEntry) ([]byte, error) {
	var recordType byte

	// Step 1: Determine the operation type
	switch entry.Operation {
	case "set":
		recordType = RecordTypeSet
	case "delete":
		recordType = RecordTypeDelete
	default:
		return nil, fmt.Errorf(
			"unknown log operation: %s",
			entry.Operation,
		)
	}

	// Step 2: Validate the key
	if entry.Key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// Step 3: Convert the log entry into JSON bytes
	payload, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal log entry: %w", err)
	}

	// Step 4: Validate payload size
	if len(payload) > MaxPayloadSize {
		return nil, fmt.Errorf(
			"payload exceeds maximum size of %d bytes",
			MaxPayloadSize,
		)
	}

	// Step 5: Allocate memory for the complete record
	record := make([]byte, recordHeaderSize+len(payload))

	// Step 6: Write payload length into bytes 0-3
	binary.BigEndian.PutUint32(
		record[0:4],
		uint32(len(payload)),
	)

	// Step 7: Write record type into byte 8
	record[8] = recordType

	// Step 8: Copy JSON payload after the header
	copy(record[9:], payload)

	// Step 9: Calculate checksum over type + payload
	checksum := crc32.Checksum(record[8:], crcTable)

	// Step 10: Store checksum in bytes 4-7
	binary.BigEndian.PutUint32(record[4:8], checksum)

	return record, nil
}
