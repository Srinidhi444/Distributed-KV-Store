# Distributed KV Store

A **Raft-based distributed key-value store built in Go**. It replicates writes across a cluster, maintains a consistent view of committed data, and can continue operating when a minority of nodes fail.

The goal is to implement and understand the core mechanisms of a distributed database—**leader election, replicated logs, quorum commits, durable recovery, and failover**—without relying on an external Raft library.

> **Project scope:** A working, learning-oriented Raft implementation with fixed cluster membership, not a production-ready database.

## Architecture

```text
                    HTTP client
                         |
                    Raft leader
                    /         \
               Follower     Follower
                    \         /
             Replicated Raft log
                         |
                Majority commit
                         |
             In-memory KV state machine

        Each node has its own durable Raft WAL.
```

Clients send `PUT`, `GET`, and `DELETE` requests to the **current leader**. Writes are appended to the leader's log, replicated through `AppendEntries`, and applied to the KV state machine only after commitment. Reads use a committed NOOP entry as a quorum-backed read barrier. Following a restart, each node rebuilds its in-memory state by replaying committed log entries from its WAL.

## Implemented features

- **Leader election:** randomized election timeouts, RequestVote RPCs, term tracking, and majority voting.
- **Log replication:** AppendEntries, consistency checks, conflict resolution, follower catch-up, and `nextIndex`/`matchIndex` tracking.
- **Quorum commits:** commands are acknowledged only after majority replication and application.
- **Linearizable reads:** leader-only reads using a committed NOOP as a read barrier.
- **Persistence and recovery:** checksummed, synchronized Raft WAL; durable term, vote, log, and commit state; replay on restart.
- **Failure handling:** automatic leader re-election, continued operation with a majority, and catch-up when a failed node returns.
- **HTTP interface:** KV operations, health checks, Raft status, and internal consensus RPCs.
- **Testing:** unit tests and multi-node integration tests for elections, replication, WAL recovery, failover, and restarts.

## Project structure

```text
.
├── main.go                          # CLI entry point and graceful shutdown
├── go.mod / go.sum                  # Go module and dependencies
├── internal/
│   ├── api/
│   │   ├── handler.go               # Leader-only KV HTTP handlers and errors
│   │   ├── request_gate.go          # Request handling during shutdown
│   │   └── *_test.go                # API tests
│   ├── config/                      # CLI flags and configuration validation
│   ├── node/
│   │   ├── node.go                  # Wires Raft, HTTP, WAL, state machine, transport
│   │   └── *_test.go                # Node and cluster integration tests
│   ├── raft/
│   │   ├── raft.go                  # Core Raft state
│   │   ├── types.go                 # Roles, commands, log entries
│   │   ├── rutime.go                # Runtime event loop, timers, proposals, reads¹
│   │   ├── events.go                # Raft event types
│   │   ├── event_validation.go      # Validates incoming events
│   │   ├── effects.go               # Persistence/network/application effects
│   │   ├── effect_queue.go          # Dispatches outbound RPC effects
│   │   ├── election.go              # Election and voting helpers
│   │   ├── election_start.go        # Starts elections
│   │   ├── election_protocol.go     # Vote handling and leader promotion
│   │   ├── election_dispatch.go     # Routes election and replication events
│   │   ├── request_vote.go          # RequestVote logic
│   │   ├── replication.go           # AppendEntries, log catch-up, commit logic
│   │   ├── messages.go              # Consensus RPC message types
│   │   ├── election_http.go         # HTTP transport for Raft RPCs
│   │   ├── log_state.go             # Log/state consistency helpers
│   │   ├── transition.go            # State transition representation
│   │   ├── transition_validation.go # Transition safety validation
│   │   ├── storage.go               # Durable storage contract
│   │   ├── storage_impl.go          # Raft WAL-backed storage
│   │   ├── wal.go                   # WAL file lifecycle and locking
│   │   ├── wal_record.go            # Framing and checksums
│   │   ├── wal_recovery.go          # WAL replay and validation
│   │   └── *_test.go                # Raft unit and integration tests
│   └── transport/                   # Peer-ping client, manager, and handler
├── start-cluster.ps1                # Optional Windows cluster launcher
└── stop-cluster.ps1                 # Optional local stop helper
```

¹ The runtime file is named `rutime.go` in the earlier working tree; it may be renamed to `runtime.go`. Only one of these files should exist.

**Storage model:** The active application uses `internal/raft` for persistent consensus state and a KV state machine in `internal/node`. The earlier standalone `internal/store` and `internal/persistence` packages are not part of this architecture.

## Getting started

**Requirements:** Go 1.22+; PowerShell for the commands below.

### Build

```powershell
go test ./...
go build -o distributed-kv.exe .
```

### Start a three-node cluster

Run each command from the repository root in **a separate PowerShell terminal**. Use distinct node IDs, ports, and data directories.

**Node 1:**

```powershell
.\distributed-kv.exe -id node1 -addr 127.0.0.1:8101 -data-dir ./manual-data/node1 -peers "node2=127.0.0.1:8102,node3=127.0.0.1:8103"
```

**Node 2:**

```powershell
.\distributed-kv.exe -id node2 -addr 127.0.0.1:8102 -data-dir ./manual-data/node2 -peers "node1=127.0.0.1:8101,node3=127.0.0.1:8103"
```

**Node 3:**

```powershell
.\distributed-kv.exe -id node3 -addr 127.0.0.1:8103 -data-dir ./manual-data/node3 -peers "node1=127.0.0.1:8101,node2=127.0.0.1:8102"
```

If the optional scripts have been created, `./start-cluster.ps1` can launch the local cluster. Stop individually launched processes with **Ctrl+C** for graceful shutdown. A force-stop helper is intended only for local testing.

### CLI flags

| Flag | Purpose | Example |
| --- | --- | --- |
| `-id` | Unique node identifier | `node1` |
| `-addr` | Node HTTP listen address | `127.0.0.1:8101` |
| `-data-dir` | Persistent data directory for this node | `./manual-data/node1` |
| `-peers` | Other members, comma-separated as `nodeID=host:port` | `node2=127.0.0.1:8102,node3=127.0.0.1:8103` |

The nodes must be configured as members of the **same fixed cluster**. Membership changes at runtime are not supported.

### Find the leader

```powershell
foreach ($port in 8101,8102,8103) {
    $s = Invoke-RestMethod "http://127.0.0.1:$port/internal/raft/status"
    Write-Host "$port $($s.node_id) $($s.role) term=$($s.term)"
}
```

Use the port of whichever node reports `role=leader` in the examples below. The leader may change after a failure.

### Use the API

```powershell
$base = "http://127.0.0.1:8102"   # Replace with the current leader URL

# Set a value
Invoke-RestMethod -Method Put -Uri "$base/kv/name" -ContentType "application/json" -Body '{"value":"raft"}'

# Get a value
Invoke-RestMethod -Method Get -Uri "$base/kv/name"

# Delete a value
Invoke-RestMethod -Method Delete -Uri "$base/kv/name"
```

| Method | Route | Description |
| --- | --- | --- |
| `PUT` | `/kv/{key}` | Write a string value via Raft |
| `GET` | `/kv/{key}` | Linearizable leader read |
| `DELETE` | `/kv/{key}` | Replicated delete |
| `GET` | `/health` | HTTP health probe (not a quorum check) |
| `GET` | `/internal/raft/status` | Node ID, role, and term |
| `POST` | `/internal/raft/request-vote` | Internal Raft voting RPC |
| `POST` | `/internal/raft/append-entries` | Internal replication/heartbeat RPC |
| `POST` | `/internal/ping` | Peer connectivity probe |

Followers return **503 Service Unavailable** for client KV operations; they do not forward requests to the leader. A timed-out write can have an **uncertain outcome** and might commit later.

## Testing

```powershell
go test ./... -count=1
go vet ./...
```

For race detection on Windows with an installed MSYS2 UCRT64 GCC toolchain:

```powershell
$env:CGO_ENABLED = "1"
$env:CC = "C:\msys64\ucrt64\bin\gcc.exe"
$env:Path = "C:\msys64\ucrt64\bin;" + $env:Path
go test -race ./... -count=2 -timeout 120s
```

Useful manual tests include **leader failover**, **follower catch-up after restart**, **data recovery from WAL**, and **quorum loss** (a lone node cannot confirm new writes in a three-node cluster).

## Limitations

- **Fixed membership:** no dynamic node joining, removal, or cluster reconfiguration.
- **No snapshots or log compaction:** WAL growth is unbounded over long-running workloads.
- **Read overhead:** each linearizable read currently commits a NOOP entry.
- **Leader-only requests:** no automatic redirection, discovery, or client retry protocol.
- **Development-network security:** no authentication or TLS for internal consensus RPCs.
- **Simple KV model:** string keys and values, without transactions, TTLs, or secondary indexes.
- **No automatic migration** from the earlier standalone KV WAL format.
- **Ambiguous timeouts:** a request that times out may still be committed later.

This project is deliberately scoped around **Raft consensus, durable replication, and fault recovery**, rather than database features or production operations.
