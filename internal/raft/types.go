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
	Type  CommandType
	Key   string
	Value string
}

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command Command
}
