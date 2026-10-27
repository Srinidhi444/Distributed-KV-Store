# Distributed KV Store

A **Raft-based distributed key-value store built in Go**. It replicates writes across nodes, preserves committed data through failures, and continues serving requests while a majority of the cluster is available.

The project implements Raft's core mechanisms—**leader election, log replication, quorum commitment, durable recovery, and failover**—without an external consensus library. It is a learning-oriented implementation with fixed cluster membership, not a production-ready database.

> **Want to understand how Raft works in a distributed KV store?** Read my Medium article: [Building a Distributed Key-Value Store with Raft Consensus](https://medium.com/@kulkarnisrinidhi85/building-a-distributed-key-value-store-with-raft-consensus-9e0292bd43b5?sharedUserId=kulkarnisrinidhi85). It covers elections, log replication, commitment, recovery, linearizable reads, and failure scenarios in detail.

## Architecture

```text
                       HTTP client
                            |
                       Raft leader
                       /         \
                  Follower     Follower
                       \         /
                    Replicated log
                            |
                     Majority commit
                            |
                    In-memory KV state

             Each node has its own durable Raft WAL.
```

Clients send `PUT`, `GET`, and `DELETE` requests to the **current leader**. Writes enter the Raft log, replicate through `AppendEntries`, and are applied after commitment. Reads use a committed NOOP as a quorum-backed read barrier. On restart, nodes rebuild their in-memory KV state by replaying committed entries from the WAL.

## Features

- **Leader election:** randomized timeouts, RequestVote RPCs, persistent terms/votes, and majority voting.
- **Log replication:** AppendEntries, log consistency checks, conflict resolution, and follower catch-up.
- **Quorum commitment:** leader tracks `nextIndex` and `matchIndex` and commits entries according to Raft's rules.
- **Linearizable reads:** leader-only reads using committed NOOP barriers.
- **Persistence:** checksummed Raft WAL with synchronized writes, validation, and recovery.
- **Failure recovery:** leader re-election and log catch-up after node restarts.
- **HTTP API:** KV operations, health and status endpoints, and internal Raft RPCs.

## Project structure

```text
.
├── main.go                           # Node entry point and graceful shutdown
├── go.mod / go.sum                   # Go module and dependencies
├── start-cluster.ps1                 # Starts the local three-node cluster
├── stop-cluster.ps1                  # Stops the local cluster processes
└── internal/
    ├── api/
    │   ├── handler.go               # Leader-only KV HTTP handlers
    │   ├── request_gate.go          # Handles requests during shutdown
    │   └── *_test.go                # API tests
    ├── config/                       # CLI flags and config validation
    ├── node/
    │   ├── node.go                  # Wires Raft, HTTP, WAL, and KV state
    │   └── *_test.go                # Node and cluster tests
    ├── raft/
    │   ├── raft.go                  # Raft state and persistence-first transitions
    │   ├── types.go                 # Roles, commands, and log entries
    │   ├── rutime.go                # Runtime loop, timers, proposals, reads
    │   ├── events.go                # Raft event types
    │   ├── event_validation.go      # Incoming event validation
    │   ├── effects.go               # Persistence and network effects
    │   ├── effect_queue.go          # Dispatches outbound effects
    │   ├── election.go              # Election helpers
    │   ├── election_start.go        # Election initialization
    │   ├── election_protocol.go     # Vote processing and leader promotion
    │   ├── election_dispatch.go     # Dispatches Raft events
    │   ├── request_vote.go          # RequestVote rules
    │   ├── replication.go           # AppendEntries, catch-up, and commits
    │   ├── messages.go              # Consensus message types
    │   ├── election_http.go         # Raft HTTP RPCs
    │   ├── log_state.go             # Log consistency helpers
    │   ├── transition.go            # Planned state transitions
    │   ├── transition_validation.go # Transition safety checks
    │   ├── storage.go               # Durable storage interface
    │   ├── storage_impl.go          # Raft WAL storage adapter
    │   ├── wal.go                   # WAL lifecycle, locking, and sync
    │   ├── wal_record.go            # Record encoding and checksums
    │   ├── wal_recovery.go          # WAL replay and validation
    │   └── *_test.go                # Raft tests
    └── transport/                    # Peer-ping client, manager, and handler
```

## Getting started

**Requirements:** Go 1.22+ and PowerShell for the local cluster scripts.

### Start the cluster

From the repository root:

```powershell
go build -o distributed-kv.exe .
.\start-cluster.ps1
```

The script starts three local nodes on ports **8101, 8102, and 8103**, using separate node IDs and data directories.

Stop the local cluster with:

```powershell
.\stop-cluster.ps1
```

The stop script force-terminates the local node processes and is intended for development and failure testing. For graceful shutdown, use `Ctrl+C` when running a node directly in a terminal.

### CLI flags

| Flag | Description |
| --- | --- |
| `-id` | Unique node ID, such as `node1` |
| `-addr` | Node HTTP address, such as `127.0.0.1:8101` |
| `-data-dir` | Directory for the node's durable Raft data |
| `-peers` | Comma-separated peer mappings: `nodeID=host:port` |

Every node needs its own ID, address, and data directory. All nodes must use a consistent fixed cluster configuration.

### HTTP API

Use **Postman** or another HTTP client to send requests to the current leader. To identify it, check `GET /internal/raft/status` on each node and find the one reporting `role: leader`.

| Method | Endpoint | Description |
| --- | --- | --- |
| `PUT` | `/kv/{key}` | Write a string value; JSON body: `{"value":"..."}` |
| `GET` | `/kv/{key}` | Linearizable read from the leader |
| `DELETE` | `/kv/{key}` | Replicated key deletion |
| `GET` | `/health` | HTTP health probe, not a quorum check |
| `GET` | `/internal/raft/status` | Node ID, role, and term |
| `POST` | `/internal/raft/request-vote` | Internal Raft election RPC |
| `POST` | `/internal/raft/append-entries` | Internal replication and heartbeat RPC |
| `POST` | `/internal/ping` | Peer connectivity check |

Followers do not forward KV requests to the leader and return `503 Service Unavailable`. A timed-out write may still commit later, so the outcome can be uncertain.

## Testing

```powershell
go test ./... -count=1
go vet ./...
```

For race detection on Windows with MSYS2 UCRT64 GCC installed:

```powershell
$env:CGO_ENABLED = "1"
$env:CC = "C:\msys64\ucrt64\bin\gcc.exe"
$env:Path = "C:\msys64\ucrt64\bin;" + $env:Path
go test -race ./... -count=2 -timeout 120s
```

The tests cover elections, replication, WAL recovery, leader failover, and follower catch-up. The cluster also supports manual fault testing by stopping individual nodes or a quorum.

## Limitations

- **Fixed membership:** no dynamic cluster reconfiguration or joint consensus.
- **No snapshots or log compaction:** the WAL grows as operations accumulate.
- **Read overhead:** every linearizable read currently commits a NOOP entry.
- **Leader-only API:** no automatic leader discovery, forwarding, or client retry protocol.
- **Development-only security:** no authentication or TLS for internal Raft RPCs.
- **Basic KV model:** string keys and values, without transactions, TTLs, or secondary indexes.
- **Uncertain timeouts:** a timed-out write might commit after the client stops waiting.

For a deeper explanation of these tradeoffs and the Raft concepts behind the implementation, see the [Medium article](https://medium.com/@kulkarnisrinidhi85/building-a-distributed-key-value-store-with-raft-consensus-9e0292bd43b5?sharedUserId=kulkarnisrinidhi85).
(padhlo yar)
