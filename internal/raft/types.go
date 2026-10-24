package raft

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

type CommandType uint8

const (
	CommandNoop CommandType = iota
	CommandSet
	CommandDelete
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key,omitempty"`
	Value string      `json:"value,omitempty"`
}

type LogEntry struct {
	Index   uint64  `json:"index"`
	Term    uint64  `json:"term"`
	Command Command `json:"command"`
}
