package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"reflect"
	"strings"
	"testing"
)

func testRaftBatch() raftBatch {
	return raftBatch{
		HardState: &HardState{
			CurrentTerm: 3,
			VotedFor:    "node2",
			CommitIndex: 2,
		},
		Entries: []LogEntry{
			{
				Index: 1,
				Term:  1,
				Command: Command{
					Type:  CommandSet,
					Key:   "language",
					Value: "Go",
				},
			},
			{
				Index: 2,
				Term:  2,
				Command: Command{
					Type: CommandDelete,
					Key:  "language",
				},
			},
		},
	}
}

// rawRaftRecord creates a framed record without calling
// encodeRaftBatch, allowing tests to inject malformed JSON.
func rawRaftRecord(payload []byte) []byte {
	record := make([]byte, raftRecordHeaderSize+len(payload))

	binary.BigEndian.PutUint32(
		record[:4],
		uint32(len(payload)),
	)

	binary.BigEndian.PutUint32(
		record[4:8],
		crc32.Checksum(payload, raftCRCTable),
	)

	copy(record[raftRecordHeaderSize:], payload)

	return record
}

func TestRaftWALHeaderEncoding(t *testing.T) {
	header := EncodeRaftWALHeader()

	if len(header) != raftWALHeaderSize {
		t.Fatalf(
			"expected %d header bytes, got %d",
			raftWALHeaderSize,
			len(header),
		)
	}

	if string(header[:4]) != RaftWALMagic {
		t.Fatalf("unexpected magic: %q", header[:4])
	}

	version := binary.BigEndian.Uint32(header[4:8])

	if version != RaftWALVersion {
		t.Fatalf(
			"expected version %d, got %d",
			RaftWALVersion,
			version,
		)
	}

	if err := ValidateRaftWALHeader(bytes.NewReader(header)); err != nil {
		t.Fatalf("valid header rejected: %v", err)
	}
}

func TestRaftWALHeaderRejectsInvalidInputs(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
	}{
		{
			name:   "empty",
			header: nil,
		},
		{
			name:   "partial magic",
			header: []byte("RF"),
		},
		{
			name:   "missing version",
			header: []byte("RFT1"),
		},
		{
			name: "invalid magic",
			header: func() []byte {
				h := EncodeRaftWALHeader()
				copy(h[:4], "BAD!")
				return h
			}(),
		},
		{
			name: "unsupported version",
			header: func() []byte {
				h := EncodeRaftWALHeader()
				binary.BigEndian.PutUint32(h[4:8], 999)
				return h
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRaftWALHeader(
				bytes.NewReader(tt.header),
			)

			if err == nil {
				t.Fatal("expected invalid WAL header to fail")
			}
		})
	}
}

func TestRaftBatchRoundTrip(t *testing.T) {
	original := testRaftBatch()

	record, err := encodeRaftBatch(original)
	if err != nil {
		t.Fatalf("encode batch: %v", err)
	}

	decoded, err := decodeRaftBatch(bytes.NewReader(record))
	if err != nil {
		t.Fatalf("decode batch: %v", err)
	}

	if !reflect.DeepEqual(decoded, original) {
		t.Fatalf(
			"round trip mismatch:\nexpected: %+v\ngot: %+v",
			original,
			decoded,
		)
	}
}

func TestRaftBatchHardStateOnly(t *testing.T) {
	original := raftBatch{
		HardState: &HardState{
			CurrentTerm: 7,
			VotedFor:    "node3",
			CommitIndex: 0,
		},
	}

	record, err := encodeRaftBatch(original)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := decodeRaftBatch(bytes.NewReader(record))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(decoded, original) {
		t.Fatalf("unexpected decoded batch: %+v", decoded)
	}
}

func TestRaftBatchEntriesOnly(t *testing.T) {
	original := raftBatch{
		Entries: []LogEntry{
			{
				Index: 1,
				Term:  1,
				Command: Command{
					Type:  CommandSet,
					Key:   "x",
					Value: "10",
				},
			},
		},
	}

	record, err := encodeRaftBatch(original)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := decodeRaftBatch(bytes.NewReader(record))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(decoded, original) {
		t.Fatalf("unexpected decoded batch: %+v", decoded)
	}
}

func TestRaftBatchRejectsEmptyBatch(t *testing.T) {
	_, err := encodeRaftBatch(raftBatch{})

	if err == nil {
		t.Fatal("expected empty batch to be rejected")
	}
}

func TestRaftBatchRecordLayout(t *testing.T) {
	batch := testRaftBatch()

	record, err := encodeRaftBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	if len(record) < raftRecordHeaderSize {
		t.Fatal("record shorter than header")
	}

	length := binary.BigEndian.Uint32(record[:4])
	checksum := binary.BigEndian.Uint32(record[4:8])
	payload := record[raftRecordHeaderSize:]

	if int(length) != len(payload) {
		t.Fatalf(
			"encoded length %d does not match payload %d",
			length,
			len(payload),
		)
	}

	expectedCRC := crc32.Checksum(payload, raftCRCTable)

	if checksum != expectedCRC {
		t.Fatalf(
			"checksum mismatch: expected %08x, got %08x",
			expectedCRC,
			checksum,
		)
	}

	var decodedJSON map[string]json.RawMessage

	if err := json.Unmarshal(payload, &decodedJSON); err != nil {
		t.Fatal(err)
	}

	if _, exists := decodedJSON["hard_state"]; !exists {
		t.Fatal("expected hard_state JSON field")
	}

	if _, exists := decodedJSON["entries"]; !exists {
		t.Fatal("expected entries JSON field")
	}
}

func TestRaftBatchDetectsChecksumCorruption(t *testing.T) {
	record, err := encodeRaftBatch(testRaftBatch())
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the payload without updating its checksum.
	record[len(record)-1] ^= 0xFF

	_, err = decodeRaftBatch(bytes.NewReader(record))

	if err == nil {
		t.Fatal("expected checksum corruption to be rejected")
	}

	if !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("expected checksum error, got %v", err)
	}
}

func TestRaftBatchRejectsPartialRecordHeader(t *testing.T) {
	record, err := encodeRaftBatch(testRaftBatch())
	if err != nil {
		t.Fatal(err)
	}

	for n := 1; n < raftRecordHeaderSize; n++ {
		t.Run(
			string(rune('0'+n)),
			func(t *testing.T) {
				_, err := decodeRaftBatch(
					bytes.NewReader(record[:n]),
				)

				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf(
						"expected io.ErrUnexpectedEOF for %d bytes, got %v",
						n,
						err,
					)
				}
			},
		)
	}
}

func TestRaftBatchCleanEOF(t *testing.T) {
	_, err := decodeRaftBatch(bytes.NewReader(nil))

	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected clean io.EOF, got %v", err)
	}

	if errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("clean EOF must not be unexpected EOF: %v", err)
	}
}

func TestRaftBatchRejectsIncompletePayload(t *testing.T) {
	record, err := encodeRaftBatch(testRaftBatch())
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		size int
	}{
		{
			name: "no payload",
			size: raftRecordHeaderSize,
		},
		{
			name: "one payload byte",
			size: raftRecordHeaderSize + 1,
		},
		{
			name: "half payload",
			size: raftRecordHeaderSize +
				(len(record)-raftRecordHeaderSize)/2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeRaftBatch(
				bytes.NewReader(record[:tt.size]),
			)

			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf(
					"expected incomplete payload error, got %v",
					err,
				)
			}
		})
	}
}

func TestRaftBatchRejectsInvalidLengths(t *testing.T) {
	tests := []struct {
		name   string
		length uint32
	}{
		{"zero", 0},
		{"above maximum", MaxRaftBatchSize + 1},
		{"maximum uint32", ^uint32(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := make([]byte, raftRecordHeaderSize)

			binary.BigEndian.PutUint32(
				record[:4],
				tt.length,
			)

			_, err := decodeRaftBatch(
				bytes.NewReader(record),
			)

			if err == nil {
				t.Fatal("expected invalid length rejection")
			}
		})
	}
}

func TestRaftBatchRejectsOversizedEncoding(t *testing.T) {
	batch := raftBatch{
		Entries: []LogEntry{
			{
				Index: 1,
				Term:  1,
				Command: Command{
					Type: CommandSet,
					Key:  "large",
					Value: strings.Repeat(
						"x",
						MaxRaftBatchSize,
					),
				},
			},
		},
	}

	_, err := encodeRaftBatch(batch)

	if err == nil {
		t.Fatal("expected oversized batch to be rejected")
	}
}

func TestRaftBatchRejectsMalformedJSON(t *testing.T) {
	payloads := []string{
		`{invalid}`,
		`{"hard_state":`,
		`not-json`,
	}

	for _, payload := range payloads {
		t.Run(payload, func(t *testing.T) {
			record := rawRaftRecord([]byte(payload))

			_, err := decodeRaftBatch(
				bytes.NewReader(record),
			)

			if err == nil {
				t.Fatal("expected malformed JSON rejection")
			}
		})
	}
}

func TestRaftBatchRejectsUnknownFields(t *testing.T) {
	payloads := []string{
		`{"unexpected":true}`,
		`{"hard_state":{"current_term":3,"unknown":1}}`,
		`{"entries":[{"index":1,"term":1,"unknown":5}]}`,
	}

	for _, payload := range payloads {
		t.Run(payload, func(t *testing.T) {
			record := rawRaftRecord([]byte(payload))

			_, err := decodeRaftBatch(
				bytes.NewReader(record),
			)

			if err == nil {
				t.Fatal("expected unknown JSON field rejection")
			}
		})
	}
}

func TestRaftBatchRejectsNullAndEmptyJSON(t *testing.T) {
	payloads := []string{
		`null`,
		`{}`,
		`{"hard_state":null}`,
		`{"entries":[]}`,
		`{"hard_state":null,"entries":[]}`,
	}

	for _, payload := range payloads {
		t.Run(payload, func(t *testing.T) {
			record := rawRaftRecord([]byte(payload))

			_, err := decodeRaftBatch(
				bytes.NewReader(record),
			)

			if err == nil {
				t.Fatal("expected empty or null batch rejection")
			}
		})
	}
}

func TestRaftBatchRejectsMultipleJSONValues(t *testing.T) {
	payloads := []string{
		`{"hard_state":{"current_term":1}}{"hard_state":{"current_term":2}}`,
		`{"hard_state":{"current_term":1}} garbage`,
	}

	for _, payload := range payloads {
		t.Run(payload, func(t *testing.T) {
			record := rawRaftRecord([]byte(payload))

			_, err := decodeRaftBatch(
				bytes.NewReader(record),
			)

			if err == nil {
				t.Fatal("expected trailing JSON rejection")
			}
		})
	}
}

func TestRaftBatchSequentialDecoding(t *testing.T) {
	first := raftBatch{
		HardState: &HardState{
			CurrentTerm: 1,
			VotedFor:    "node1",
		},
	}

	second := raftBatch{
		Entries: []LogEntry{
			{
				Index: 1,
				Term:  1,
				Command: Command{
					Type:  CommandSet,
					Key:   "x",
					Value: "10",
				},
			},
		},
	}

	firstRecord, err := encodeRaftBatch(first)
	if err != nil {
		t.Fatal(err)
	}

	secondRecord, err := encodeRaftBatch(second)
	if err != nil {
		t.Fatal(err)
	}

	data := append(firstRecord, secondRecord...)
	reader := bytes.NewReader(data)

	gotFirst, err := decodeRaftBatch(reader)
	if err != nil {
		t.Fatal(err)
	}

	gotSecond, err := decodeRaftBatch(reader)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(gotFirst, first) {
		t.Fatalf("first batch mismatch: %+v", gotFirst)
	}

	if !reflect.DeepEqual(gotSecond, second) {
		t.Fatalf("second batch mismatch: %+v", gotSecond)
	}

	_, err = decodeRaftBatch(reader)

	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected clean EOF after two records, got %v", err)
	}
}
