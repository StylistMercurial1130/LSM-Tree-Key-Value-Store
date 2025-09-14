# LSM Storage Engine

A Key-Value Store backed by a Log-Structured Merge (LSM) Tree for disk persistence, developed in Go.

## Features

- **Efficient Write Path:** Uses a memory-resident AVL tree as a memtable for fast inserts and updates.
- **Disk Persistence:** Periodically flushes memtable contents to disk as sorted tables (SSTables).
- **Bloom Filters:** Accelerates key lookups and reduces unnecessary disk reads.
- **Multi-Level Storage:** Organizes SSTables into levels, supporting compaction and merging.
- **Concurrency:** Thread-safe operations using mutexes.
- **CLI Interface:** Simple command-line interface for `get`, `put`, and `delete` operations.
- **Extensible:** Modular design with clear separation between memory, disk, and engine logic.

## Project Structure

```
lsm_storage_engine/
├── disk/           # Disk layer: SSTables, bloom filters, levels, compaction
├── engine/         # Storage engine interface and implementation
├── mem/            # In-memory AVL tree and memtable
├── types/          # Common types: Record, Entry, errors, bitvector
├── main.go         # CLI entry point
└── README.md       # Project documentation
```

## Getting Started

### Prerequisites

- Go 1.20 or newer

### Build

```sh
go build -o lsm_storage_engine main.go
```

### Run

```sh
./lsm_storage_engine
```

### CLI Usage

Supported commands:

- `put <key> <value>`: Insert or update a key-value pair.
- `get <key>`: Retrieve the value for a key.
- `delete <key>`: Remove a key from the store.
- `exit`: Quit the CLI.

Example session:

```
LSM Storage Engine CLI
Commands: get <key>, put <key> <value>, delete <key>, exit
> put foo bar
Put Key: foo, Value: bar
> get foo
Key: foo, Value: bar
> delete foo
Deleted Key: foo
> get foo
Key not found
> exit
Exiting CLI.
```

## Implementation Details

- **Memtable:**  
  - Implemented as an AVL tree (`mem/avl.go`).
  - Flushed to disk when size threshold is reached.

- **Disk Layer:**  
  - SSTables stored in `./data` directory.
  - Bloom filters and index blocks for fast lookup.
  - Multi-level structure with compaction (`disk/diskmanager.go`, `disk/level.go`).

- **Types:**  
  - `Record` and `Entry` types for key-value pairs.
  - Custom error types for robust error handling.

- **Engine:**  
  - `StorageEngine` interface in [`engine/storage_engine.go`](engine/storage_engine.go).
  - Asynchronous API: `Get`, `Put`, and `Delete` return results via channels.

## Testing

Unit tests are provided for core components:

```sh
go test ./disk
go test ./mem
go test ./types
```

## Extending

- Add new compaction strategies in the disk layer.
- Support range queries or batch operations.
- Integrate with network protocols for distributed storage.