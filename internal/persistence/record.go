package persistence

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
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

func DecodeRecord(r io.Reader) (LogEntry, error) {
	var entry LogEntry

	// Step 1: Read the complete 9-byte record header
	header := make([]byte, recordHeaderSize)

	_, err := io.ReadFull(r, header)
	if err != nil {
		return entry, fmt.Errorf("read record header: %w", err)
	}

	// Step 2: Extract payload length and checksum
	length := binary.BigEndian.Uint32(header[0:4])
	expectedCRC := binary.BigEndian.Uint32(header[4:8])
	recordType := header[8]

	// Step 3: Validate payload length
	if length > MaxPayloadSize {
		return entry, fmt.Errorf(
			"record payload exceeds maximum size: %d",
			length,
		)
	}

	// Step 4: Read exactly length bytes
	payload := make([]byte, int(length))

	if _, err := io.ReadFull(r, payload); err != nil {
		return entry, fmt.Errorf("read record payload: %w", err)
	}

	// Step 5: Verify CRC32C
	checksum := crc32.Update(
		0,
		crcTable,
		[]byte{recordType},
	)
	checksum = crc32.Update(checksum, crcTable, payload)

	if checksum != expectedCRC {
		return entry, fmt.Errorf(
			"checksum mismatch: expected %08x, got %08x",
			expectedCRC,
			checksum,
		)
	}

	// Step 6: Decode the JSON payload
	if err := json.Unmarshal(payload, &entry); err != nil {
		return entry, fmt.Errorf("decode record JSON: %w", err)
	}

	// Step 7: Validate the key
	if entry.Key == "" {
		return entry, fmt.Errorf("record contains empty key")
	}

	// Step 8: Validate record type and operation
	switch recordType {
	case RecordTypeSet:
		if entry.Operation != "set" {
			return entry, fmt.Errorf(
				"record type SET does not match operation %q",
				entry.Operation,
			)
		}

	case RecordTypeDelete:
		if entry.Operation != "delete" {
			return entry, fmt.Errorf(
				"record type DELETE does not match operation %q",
				entry.Operation,
			)
		}

	default:
		return entry, fmt.Errorf(
			"unknown record type: %d",
			recordType,
		)
	}

	return entry, nil
}
