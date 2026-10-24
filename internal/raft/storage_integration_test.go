package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func testStoragePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "raft.wal")
}

func openTestWALStorage(t *testing.T, path string) *WALStorage {
	t.Helper()

	storage, err := OpenWALStorage(path)
	if err != nil {
		t.Fatalf("OpenWALStorage: %v", err)
	}

	return storage
}

func closeTestWALStorage(t *testing.T, storage *WALStorage) {
	t.Helper()

	if err := storage.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func testSetEntry(index, term uint64, key, value string) LogEntry {
	return LogEntry{
		Index: index,
		Term:  term,
		Command: Command{
			Type:  CommandSet,
			Key:   key,
			Value: value,
		},
	}
}

func testHardState(term uint64, vote string, commit uint64) *HardState {
	return &HardState{
		CurrentTerm: term,
		VotedFor:    vote,
		CommitIndex: commit,
	}
}

func TestWALStorageNewAndEmptyRecovery(t *testing.T) {
	path := testStoragePath(t)

	storage := openTestWALStorage(t, path)

	state, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}

	if state.HardState != (HardState{}) {
		t.Fatalf("expected empty HardState, got %+v", state.HardState)
	}

	if len(state.Entries) != 0 {
		t.Fatalf("expected empty log, got %+v", state.Entries)
	}

	closeTestWALStorage(t, storage)

	// Reopening an empty WAL must recover the same state.
	reopened := openTestWALStorage(t, path)
	defer reopened.Close()

	recovered, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(recovered, state) {
		t.Fatalf(
			"empty recovery mismatch:\nexpected: %+v\ngot: %+v",
			state,
			recovered,
		)
	}
}

func TestWALStorageHardStateSurvivesRestart(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)

	expected := testHardState(5, "node2", 0)

	if err := storage.Save(expected, nil); err != nil {
		t.Fatalf("Save HardState: %v", err)
	}

	closeTestWALStorage(t, storage)

	reopened := openTestWALStorage(t, path)
	defer reopened.Close()

	state, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}

	if state.HardState != *expected {
		t.Fatalf(
			"HardState recovery mismatch:\nexpected: %+v\ngot: %+v",
			*expected,
			state.HardState,
		)
	}
}

func TestWALStorageLogSurvivesRestart(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)

	if err := storage.Save(testHardState(2, "node1", 0), nil); err != nil {
		t.Fatal(err)
	}

	entries := []LogEntry{
		testSetEntry(1, 1, "x", "10"),
		testSetEntry(2, 1, "y", "20"),
		testSetEntry(3, 2, "z", "30"),
	}

	if err := storage.Save(nil, entries); err != nil {
		t.Fatalf("Save entries: %v", err)
	}

	closeTestWALStorage(t, storage)

	reopened := openTestWALStorage(t, path)
	defer reopened.Close()

	state, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(state.Entries, entries) {
		t.Fatalf(
			"log recovery mismatch:\nexpected: %+v\ngot: %+v",
			entries,
			state.Entries,
		)
	}

	if state.HardState.CurrentTerm != 2 {
		t.Fatalf("expected term 2, got %d", state.HardState.CurrentTerm)
	}
}

func TestWALStorageLogicalSuffixReplacement(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)

	if err := storage.Save(testHardState(2, "node1", 0), nil); err != nil {
		t.Fatal(err)
	}

	original := []LogEntry{
		testSetEntry(1, 1, "a", "10"),
		testSetEntry(2, 1, "b", "20"),
		testSetEntry(3, 2, "c", "30"),
		testSetEntry(4, 2, "d", "40"),
	}

	if err := storage.Save(nil, original); err != nil {
		t.Fatal(err)
	}

	// New term allows a conflicting uncommitted suffix.
	if err := storage.Save(testHardState(3, "node2", 2), nil); err != nil {
		t.Fatal(err)
	}

	replacement := []LogEntry{
		testSetEntry(3, 3, "c", "300"),
	}

	if err := storage.Save(nil, replacement); err != nil {
		t.Fatalf("replace suffix: %v", err)
	}

	closeTestWALStorage(t, storage)

	reopened := openTestWALStorage(t, path)
	defer reopened.Close()

	state, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}

	expected := []LogEntry{
		original[0],
		original[1],
		replacement[0],
	}

	if !reflect.DeepEqual(state.Entries, expected) {
		t.Fatalf(
			"incorrect recovered suffix:\nexpected: %+v\ngot: %+v",
			expected,
			state.Entries,
		)
	}

	if state.HardState.CommitIndex != 2 {
		t.Fatalf(
			"expected committed index 2, got %d",
			state.HardState.CommitIndex,
		)
	}
}

func TestWALStorageRejectsCommittedOverwrite(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)
	defer storage.Close()

	if err := storage.Save(testHardState(2, "node1", 0), nil); err != nil {
		t.Fatal(err)
	}

	entries := []LogEntry{
		testSetEntry(1, 1, "x", "10"),
		testSetEntry(2, 2, "y", "20"),
	}

	if err := storage.Save(nil, entries); err != nil {
		t.Fatal(err)
	}

	if err := storage.Save(testHardState(2, "node1", 2), nil); err != nil {
		t.Fatal(err)
	}

	before, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}

	infoBefore, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Save(nil, []LogEntry{
		testSetEntry(2, 2, "y", "999"),
	})

	if !errors.Is(err, ErrCommittedEntry) {
		t.Fatalf("expected ErrCommittedEntry, got %v", err)
	}

	after, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(before, after) {
		t.Fatal("invalid update modified in-memory state")
	}

	infoAfter, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if infoAfter.Size() != infoBefore.Size() {
		t.Fatal("invalid update unexpectedly modified WAL size")
	}
}

func TestWALStorageRejectsDoubleVoting(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)
	defer storage.Close()

	if err := storage.Save(testHardState(5, "node2", 0), nil); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		vote string
	}{
		{"different candidate", "node3"},
		{"cleared vote", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.Save(testHardState(5, tt.vote, 0), nil)

			if !errors.Is(err, ErrInvalidRaftState) {
				t.Fatalf("expected invalid state error, got %v", err)
			}
		})
	}

	state, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}

	if state.HardState.VotedFor != "node2" {
		t.Fatalf("recorded vote changed to %q", state.HardState.VotedFor)
	}

	// Moving to a newer term allows voting for another candidate.
	if err := storage.Save(testHardState(6, "node3", 0), nil); err != nil {
		t.Fatalf("new-term vote rejected: %v", err)
	}
}

func TestWALStorageRejectsTermAndCommitRegression(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)
	defer storage.Close()

	if err := storage.Save(testHardState(3, "node1", 0), nil); err != nil {
		t.Fatal(err)
	}

	if err := storage.Save(nil, []LogEntry{
		testSetEntry(1, 3, "x", "10"),
	}); err != nil {
		t.Fatal(err)
	}

	if err := storage.Save(testHardState(3, "node1", 1), nil); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		state HardState
	}{
		{
			name: "term regression",
			state: HardState{
				CurrentTerm: 2,
				VotedFor:    "node1",
				CommitIndex: 1,
			},
		},
		{
			name: "commit regression",
			state: HardState{
				CurrentTerm: 3,
				VotedFor:    "node1",
				CommitIndex: 0,
			},
		},
		{
			name: "commit beyond log",
			state: HardState{
				CurrentTerm: 3,
				VotedFor:    "node1",
				CommitIndex: 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.Save(&tt.state, nil)

			if !errors.Is(err, ErrInvalidRaftState) {
				t.Fatalf("expected invalid state error, got %v", err)
			}
		})
	}
}

func TestWALStorageRejectsLogGaps(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)
	defer storage.Close()

	if err := storage.Save(testHardState(2, "node1", 0), nil); err != nil {
		t.Fatal(err)
	}

	if err := storage.Save(nil, []LogEntry{
		testSetEntry(1, 1, "x", "10"),
	}); err != nil {
		t.Fatal(err)
	}

	err := storage.Save(nil, []LogEntry{
		testSetEntry(3, 2, "z", "30"),
	})

	if !errors.Is(err, ErrInvalidRaftState) {
		t.Fatalf("expected log gap rejection, got %v", err)
	}
}

func TestWALStorageLoadReturnsIndependentCopy(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)
	defer storage.Close()

	if err := storage.Save(testHardState(1, "node1", 0), nil); err != nil {
		t.Fatal(err)
	}

	if err := storage.Save(nil, []LogEntry{
		testSetEntry(1, 1, "x", "10"),
	}); err != nil {
		t.Fatal(err)
	}

	first, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}

	first.Entries[0].Command.Value = "CORRUPTED"
	first.Entries[0].Term = 999
	first.HardState.CurrentTerm = 999

	second, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}

	if second.Entries[0].Command.Value != "10" {
		t.Fatal("Load exposed internal log memory")
	}

	if second.Entries[0].Term != 1 {
		t.Fatal("Load exposed internal entry memory")
	}

	if second.HardState.CurrentTerm != 1 {
		t.Fatal("Load exposed internal HardState")
	}
}

func TestWALStorageSaveDoesNotModifyInputs(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)
	defer storage.Close()

	hs := testHardState(1, "node1", 0)
	entries := []LogEntry{
		testSetEntry(1, 1, "x", "10"),
	}

	originalHS := *hs
	originalEntries := append([]LogEntry(nil), entries...)

	if err := storage.Save(hs, entries); err != nil {
		t.Fatal(err)
	}

	if *hs != originalHS {
		t.Fatal("Save mutated the caller's HardState")
	}

	if !reflect.DeepEqual(entries, originalEntries) {
		t.Fatal("Save mutated the caller's entries")
	}
}

func TestWALStorageEmptySaveDoesNotGrowWAL(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)
	defer storage.Close()

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := storage.Save(nil, nil); err != nil {
		t.Fatal(err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if before.Size() != after.Size() {
		t.Fatalf(
			"empty Save changed WAL size from %d to %d",
			before.Size(),
			after.Size(),
		)
	}
}

func TestWALStorageExclusiveLock(t *testing.T) {
	path := testStoragePath(t)

	first := openTestWALStorage(t, path)

	_, err := OpenWALStorage(path)
	if !errors.Is(err, ErrRaftWALLocked) {
		t.Fatalf("expected WAL lock conflict, got %v", err)
	}

	closeTestWALStorage(t, first)

	second := openTestWALStorage(t, path)
	defer second.Close()
}

func TestWALStorageClosedOperations(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)

	closeTestWALStorage(t, storage)

	// Closing twice must be safe.
	if err := storage.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}

	if _, err := storage.Load(); !errors.Is(err, ErrStorageClosed) {
		t.Fatalf("expected ErrStorageClosed from Load, got %v", err)
	}

	if err := storage.Save(nil, nil); !errors.Is(err, ErrStorageClosed) {
		t.Fatalf("expected ErrStorageClosed from Save, got %v", err)
	}
}

func TestWALStorageRejectsChecksumCorruption(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)

	if err := storage.Save(testHardState(1, "node1", 0), nil); err != nil {
		t.Fatal(err)
	}

	closeTestWALStorage(t, storage)

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatal(err)
	}

	// Flip a byte in the final JSON payload.
	offset := info.Size() - 1
	buf := make([]byte, 1)

	if _, err := file.ReadAt(buf, offset); err != nil {
		file.Close()
		t.Fatal(err)
	}

	buf[0] ^= 0xFF

	if _, err := file.WriteAt(buf, offset); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = OpenWALStorage(path)
	if err == nil {
		t.Fatal("expected corrupted WAL to be rejected")
	}

	if !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("expected checksum-related error, got %v", err)
	}
}

func TestWALStorageRejectsIncompleteTail(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)

	if err := storage.Save(testHardState(1, "node1", 0), nil); err != nil {
		t.Fatal(err)
	}

	closeTestWALStorage(t, storage)

	// Simulate an interrupted append with an incomplete
	// record header at the end of an otherwise valid WAL.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.Write([]byte{0x00, 0x00, 0x01}); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = OpenWALStorage(path)
	if err == nil {
		t.Fatal("expected incomplete WAL tail to be rejected")
	}

	if !errors.Is(err, os.ErrNotExist) &&
		!strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("unexpected recovery error: %v", err)
	}
}

func TestWALStorageRejectsInvalidLogicalReplay(t *testing.T) {
	path := testStoragePath(t)

	// Write a structurally valid WAL record containing
	// an invalid Raft state transition.
	wal, err := OpenRaftWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	err = wal.appendBatch(raftBatch{
		HardState: testHardState(1, "node1", 0),
		Entries: []LogEntry{
			testSetEntry(5, 1, "x", "10"),
		},
	})

	if err != nil {
		wal.Close()
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// The CRC and JSON are valid, but index 5 creates
	// a gap in an otherwise empty Raft log.
	_, err = OpenWALStorage(path)

	if !errors.Is(err, ErrInvalidRaftState) {
		t.Fatalf(
			"expected logical recovery validation failure, got %v",
			err,
		)
	}

	// Recovery failure must release the file lock.
	wal, err = OpenRaftWAL(path)
	if err != nil {
		t.Fatalf("WAL lock remained held after recovery failure: %v", err)
	}
	defer wal.Close()
}

func TestWALStorageNewWithStorageRecovery(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)

	if err := storage.Save(testHardState(4, "node2", 0), nil); err != nil {
		t.Fatal(err)
	}

	entries := []LogEntry{
		testSetEntry(1, 2, "x", "10"),
		testSetEntry(2, 4, "y", "20"),
	}

	if err := storage.Save(nil, entries); err != nil {
		t.Fatal(err)
	}

	if err := storage.Save(testHardState(4, "node2", 1), nil); err != nil {
		t.Fatal(err)
	}

	closeTestWALStorage(t, storage)

	reopened := openTestWALStorage(t, path)
	defer reopened.Close()

	node, err := NewWithStorage("node1", reopened)
	if err != nil {
		t.Fatalf("NewWithStorage: %v", err)
	}

	if node.ID() != "node1" {
		t.Fatalf("unexpected node ID: %q", node.ID())
	}

	if node.Role() != Follower {
		t.Fatalf("restarted node must be follower, got %v", node.Role())
	}

	if node.CurrentTerm() != 4 {
		t.Fatalf("expected term 4, got %d", node.CurrentTerm())
	}

	if node.votedFor != "node2" {
		t.Fatalf("expected vote for node2, got %q", node.votedFor)
	}

	if node.commitIndex != 1 {
		t.Fatalf("expected commit index 1, got %d", node.commitIndex)
	}

	if node.lastApplied != 0 {
		t.Fatalf("expected lastApplied 0, got %d", node.lastApplied)
	}

	if !reflect.DeepEqual(node.log, entries) {
		t.Fatalf(
			"recovered Raft log mismatch:\nexpected: %+v\ngot: %+v",
			entries,
			node.log,
		)
	}

	if node.nextIndex == nil || node.matchIndex == nil {
		t.Fatal("leader replication maps must be initialized")
	}

	if node.storage == nil {
		t.Fatal("Raft storage dependency was not assigned")
	}
}

func TestNewWithStorageRejectsInvalidInputs(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)
	defer storage.Close()

	tests := []struct {
		name    string
		id      string
		storage Storage
	}{
		{
			name:    "empty ID",
			id:      "",
			storage: storage,
		},
		{
			name:    "whitespace ID",
			id:      "   ",
			storage: storage,
		},
		{
			name:    "nil storage",
			id:      "node1",
			storage: nil,
		},
		{
			name:    "typed nil storage",
			id:      "node1",
			storage: (*WALStorage)(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := NewWithStorage(tt.id, tt.storage)

			if err == nil {
				t.Fatal("expected constructor error")
			}

			if node != nil {
				t.Fatal("constructor returned non-nil Raft on failure")
			}
		})
	}
}

func TestWALStorageConcurrentLoadsAndSaves(t *testing.T) {
	path := testStoragePath(t)
	storage := openTestWALStorage(t, path)
	defer storage.Close()

	if err := storage.Save(testHardState(1, "node1", 0), nil); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 32)

	// Run concurrent readers while one writer appends
	// sequential entries. This exercises locking and
	// defensive state copies.
	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 25; j++ {
				state, err := storage.Load()
				if err != nil {
					errCh <- err
					return
				}

				if err := validatePersistentState(state); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := uint64(1); i <= 20; i++ {
			entry := testSetEntry(
				i,
				1,
				fmt.Sprintf("key-%d", i),
				fmt.Sprintf("value-%d", i),
			)

			if err := storage.Save(nil, []LogEntry{entry}); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent operation failed: %v", err)
	}

	state, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Entries) != 20 {
		t.Fatalf("expected 20 entries, got %d", len(state.Entries))
	}
}
