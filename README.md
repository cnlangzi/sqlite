# SQLite with Buffered Batch Writes

A high-performance SQLite driver wrapper providing transparent read/write separation and buffered batch writes.

## Features

- **Read/Write Separation**: Dedicated reader and writer connections for optimal concurrency
- **Buffered Batch Writes**: Aggregates multiple writes and flushes based on size threshold or time interval
- **Automatic Memory Tuning**: Dynamically configures SQLite cache sizes based on available system memory
- **WAL Mode**: Enabled by default for file databases, enabling concurrent reads during writes
- **Savepoint Support**: Nested transaction support via SQLite savepoints
- **Thread-Safe Writer**: Serialized writes through a single goroutine via channel-based coordination

## Installation

```bash
go get github.com/cnlangzi/sqlite
```

## Quick Start

```go
import "github.com/cnlangzi/sqlite"

db, err := sqlite.Open(context.Background(), "mydb.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Write operations (buffered)
_, err = db.Exec("INSERT INTO users (name) VALUES (?)", "alice")
if err != nil {
    log.Fatal(err)
}

// Read operations (concurrent)
row := db.QueryRow("SELECT name FROM users WHERE id = ?", 1)
var name string
row.Scan(&name)
```

## Configuration

### Buffer Configuration

```go
db, err := sqlite.Open(ctx, "mydb.db")
if err != nil {
    log.Fatal(err)
}

// Customize buffer behavior
db.Writer.(*sqlite.Writer).Configure(sqlite.Buffer{
    BufferSize:    200,              // Number of statements before flush (default: 100)
    FlushInterval: 200 * time.Millisecond, // Maximum wait before flush (default: 100ms)
})
```

### In-Memory Database

```go
// Use ":memory:" for an in-memory database
db, err := sqlite.Open(ctx, ":memory:")
```

## Architecture

### Read/Write Separation

The `DB` wrapper maintains two separate `*sql.DB` connections:

| Connection | Purpose | Configuration |
|------------|---------|---------------|
| `Writer` | All write operations | Single connection, serialized writes |
| `Reader` | All read operations | Connection pool scaled to `2 * runtime.NumCPU()` |

### Buffered Batch Writes

The `Writer` transparently batches writes through a dedicated goroutine:

1. Writes are sent via a channel to a single flush goroutine
2. The goroutine collects writes into an in-memory transaction
3. Flush occurs when:
   - Buffer reaches `BufferSize` threshold
   - `FlushInterval` elapses since last commit
   - `Writer.Commit()` is called
   - `Writer.Close()` is called

### Automatic Memory Tuning

For file databases, cache sizes are automatically configured based on physical memory:

| Parameter | Target | Min | Max | Fallback |
|-----------|--------|-----|-----|----------|
| Writer cache_size | 5% RAM | 32 MB | 512 MB | 100 MB |
| Reader cache_size | 12.5% RAM | 64 MB | 2 GB | 256 MB |
| Reader mmap_size | 50% RAM | 256 MB | 128 GB | 1 GB |

## API Reference

### Open

```go
func Open(ctx context.Context, dsn string) (*DB, error)
```

Opens a SQLite database. DSN can be a file path or `:memory:` for in-memory mode.

### DB

```go
type DB struct {
    Writer *Writer  // Buffered write operations
    Reader *sql.DB  // Direct read operations (connection pool)
}
```

### Writer

```go
// Execute a write statement
func (w *Writer) Exec(query string, args ...any) (sql.Result, error)

// Execute with context
func (w *Writer) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)

// Flush pending writes immediately
func (w *Writer) Commit() error

// Begin a buffered transaction
func (w *Writer) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)

// Close the writer (flushes and waits for completion)
func (w *Writer) Close() error
```

### Tx (Buffered Transaction)

```go
// Buffer a statement (does not execute immediately)
func (btx *Tx) Exec(query string, args ...any) (sql.Result, error)

// Execute all buffered statements atomically
func (btx *Tx) Commit() error

// Discard all buffered statements
func (btx *Tx) Rollback() error
```

### Savepoint

For fine-grained transaction control within a batch:

```go
sp, err := sqlite.NewSavepoint(tx)
if err != nil {
    log.Fatal(err)
}
// ... do work ...
sp.Release() // or sp.Rollback() to undo
```

## Performance Characteristics

- **Writes**: Serialized through a single goroutine; batching reduces transaction overhead
- **Reads**: Concurrent via connection pool; WAL mode allows reads during writes
- **Memory**: In-memory mode shares a single connection; file mode uses separate reader/writer connections
- **Durability**: Writes are committed to WAL on flush interval; call `Commit()` for immediate persistence

## SQLite Compilation Notes

This package uses [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) and requires CGO. Ensure you have a C compiler installed:

```bash
# macOS
xcode-select --install

# Ubuntu/Debian
sudo apt-get install build-essential
```