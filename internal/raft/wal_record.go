package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	RaftWALMagic   = "RFT1"
	RaftWALVersion = uint32(1)

	raftWALHeaderSize    = 8
	raftRecordHeaderSize = 8

	MaxRaftBatchSize = 4 * 1024 * 1024
)

var raftCRCTable = crc32.MakeTable(crc32.Castagnoli)

type raftBatch struct {
	HardState *HardState `json:"hard_state,omitempty"`
	Entries   []LogEntry `json:"entries,omitempty"`
}

// EncodeRaftWALHeader creates the versioned 8-byte WAL header.
func EncodeRaftWALHeader() []byte {
	buf := make([]byte, raftWALHeaderSize)

	copy(buf[:4], RaftWALMagic)
	binary.BigEndian.PutUint32(buf[4:8], RaftWALVersion)

	return buf
}

// ValidateRaftWALHeader validates the WAL magic and version.
func ValidateRaftWALHeader(r io.Reader) error {
	header := make([]byte, raftWALHeaderSize)

	if _, err := io.ReadFull(r, header); err != nil {
		return fmt.Errorf("read raft WAL header: %w", err)
	}

	if string(header[:4]) != RaftWALMagic {
		return fmt.Errorf(
			"invalid raft WAL magic: %q",
			string(header[:4]),
		)
	}

	version := binary.BigEndian.Uint32(header[4:8])

	if version != RaftWALVersion {
		return fmt.Errorf(
			"unsupported raft WAL version: %d (expected %d)",
			version,
			RaftWALVersion,
		)
	}

	return nil
}

// encodeRaftBatch converts a logical batch into a checksummed,
// length-prefixed binary WAL record.
func encodeRaftBatch(batch raftBatch) ([]byte, error) {
	if batch.HardState == nil && len(batch.Entries) == 0 {
		return nil, errors.New("raft batch cannot be empty")
	}

	payload, err := json.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("marshal raft batch: %w", err)
	}

	if len(payload) == 0 || len(payload) > MaxRaftBatchSize {
		return nil, fmt.Errorf(
			"invalid raft batch payload size: %d",
			len(payload),
		)
	}

	frame := make([]byte, raftRecordHeaderSize+len(payload))

	binary.BigEndian.PutUint32(
		frame[0:4],
		uint32(len(payload)),
	)

	checksum := crc32.Checksum(payload, raftCRCTable)

	binary.BigEndian.PutUint32(
		frame[4:8],
		checksum,
	)

	copy(frame[raftRecordHeaderSize:], payload)

	return frame, nil
}

// decodeRaftBatch reads exactly one framed WAL record.
//
// A clean EOF is returned only when no bytes of the next
// record header have been consumed.
func decodeRaftBatch(r io.Reader) (raftBatch, error) {
	header := make([]byte, raftRecordHeaderSize)

	if _, err := io.ReadFull(r, header); err != nil {
		return raftBatch{}, fmt.Errorf(
			"read raft record header: %w",
			err,
		)
	}

	length := binary.BigEndian.Uint32(header[0:4])
	expectedChecksum := binary.BigEndian.Uint32(header[4:8])

	if length == 0 || length > MaxRaftBatchSize {
		return raftBatch{}, fmt.Errorf(
			"invalid raft batch payload length: %d",
			length,
		)
	}

	payload := make([]byte, int(length))

	if _, err := io.ReadFull(r, payload); err != nil {
		// The record header has already been consumed.
		// Therefore an absent payload is an incomplete
		// record, not a clean end of the WAL.
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}

		return raftBatch{}, fmt.Errorf(
			"read raft batch payload: %w",
			err,
		)
	}

	actualChecksum := crc32.Checksum(payload, raftCRCTable)

	if actualChecksum != expectedChecksum {
		return raftBatch{}, fmt.Errorf(
			"raft batch checksum mismatch: expected %08x, got %08x",
			expectedChecksum,
			actualChecksum,
		)
	}

	// Reject JSON null explicitly.
	if bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
		return raftBatch{}, errors.New(
			"raft batch cannot be JSON null",
		)
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()

	var batch raftBatch

	if err := decoder.Decode(&batch); err != nil {
		return raftBatch{}, fmt.Errorf(
			"decode raft batch JSON: %w",
			err,
		)
	}

	// Reject additional JSON objects or trailing garbage.
	var extra json.RawMessage

	if err := decoder.Decode(&extra); err != io.EOF {
		return raftBatch{}, errors.New(
			"raft batch must contain exactly one JSON object",
		)
	}

	if batch.HardState == nil && len(batch.Entries) == 0 {
		return raftBatch{}, errors.New(
			"decoded raft batch cannot be empty",
		)
	}

	return batch, nil
}
