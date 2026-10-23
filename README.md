# kvstore

A key-value database written from scratch in Go, built in stages: from a
single-node storage engine up to a replicated cluster using the Raft
consensus algorithm.

## Status

**Milestone 1: storage engine.** Done.

```
go test -race ./...         # run the tests
go run ./cmd/kv -db my.kv   # interactive shell
```

```
> set name ada
OK
> get name
ada
> del name
OK
```

## Design

The storage engine follows the [Bitcask](https://riak.com/assets/bitcask-intro.pdf)
model.

- **Append-only log.** Every `Put` and `Delete` appends a record to one data
  file. Nothing is ever modified in place, so a crash can only damage the
  last record.
- **In-memory index.** A hash map points each live key to the offset of its
  latest record. A `Get` is one map lookup plus one disk read.
- **Deletes are tombstones.** A delete appends a marker record; replaying
  the log removes the key from the index.
- **Checksums and crash recovery.** Each record carries a CRC32. On startup
  the log is replayed to rebuild the index. If the replay finds a truncated
  or corrupt record (a torn write from a crash), the file is cut back to the
  last good record.

Record format (little-endian):

```
+---------+------------+------------+---------+-----------+
| crc (4) | keyLen (4) | valLen (4) | key ... | value ... |
+---------+------------+------------+---------+-----------+
```

`valLen = 0xFFFFFFFF` marks a tombstone.

### Trade-offs

- All keys must fit in memory (values stay on disk).
- The log grows forever: overwritten and deleted values still take up
  space. Compaction (milestone 2) fixes this.
- By default writes aren't fsynced, so a power loss can lose the most recent
  writes. `Options{SyncWrites: true}` (or `-sync` in the CLI) makes every
  write durable, at a large throughput cost.

### Benchmarks

Apple M1 Pro, 100-byte values, no fsync:

| Operation | Time/op |
|-----------|---------|
| Put       | ~2.7 µs |
| Get       | ~0.7 µs |

Run with `go test -run=^$ -bench=. ./storage`.

## Roadmap

1. **Storage engine.** Append-only log, in-memory index, checksums, crash
   recovery. ✅
2. **Compaction.** Split the log into size-capped segment files, merge old
   segments in the background to reclaim space, and add hint files for fast
   startup.
3. **Network server.** TCP server speaking the Redis protocol (RESP), so
   `redis-cli` and existing client libraries work against it. Load-test it.
4. **Replication with Raft.** Three-node cluster with leader election, log
   replication and linearizable reads, implemented from the
   [Raft paper](https://raft.github.io/raft.pdf).
5. **Fault testing.** Kill nodes and simulate network partitions under load,
   check that no acknowledged write is lost, and write up the results.
