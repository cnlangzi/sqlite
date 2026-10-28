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

`Open` / `New` build the writer from `DefaultBufferConfig()`, which reads its
defaults from the environment:

| Environment variable | Meaning | Default |
|----------------------|---------|---------|
| `SQLITE_BUFFER_SIZE` | Statements buffered per transaction before a commit | `100` |
| `SQLITE_BUFFER_FLUSH_INTERVAL` | Maximum time between commits | `100ms` |
| `SQLITE_BUFFER_QUEUE_CAPACITY` | Tasks that may wait in the writer's task queue | `100` |

To use a non-default configuration, build the writer yourself with
`NewWriter`:

```go
db, err := sql.Open("sqlite3", "mydb.db")
if err != nil {
    log.Fatal(err)
}

writer := sqlite.NewWriter(db, sqlite.BufferConfig{
    Size:          200,                   // Statements per transaction (default: 100)
    FlushInterval: 200 * time.Millisecond, // Maximum wait between commits (default: 100ms)
    QueueCapacity: 500,                   // Tasks that may wait in the task queue (default: 100)
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

1. Writes are sent to the writer's task queue (a buffered channel) and picked
   up by a single flush goroutine
2. The goroutine collects writes into an in-memory transaction
3. Flush occurs when:
   - Buffer reaches the `Size` threshold
   - `FlushInterval` elapses since last commit
   - `Flush()` is called
   - `Close()` is called

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

### Error Semantics

| Error | Meaning | Caller action |
|-------|---------|---------------|
| `sqlite.ErrClosed` | The writer is closed, or was closed while the task was queued | Terminal — stop writing |
| `sqlite.ErrQueueFull` | The writer's task queue is full; the task was **not** executed | Transient — retry once the queue drains |

`ErrQueueFull` is back-pressure: the flush goroutine is behind, for example
because it is blocked on a slow statement or on the SQLite write lock. The
rejected task did not run, so retrying it is safe — but only while the writer
is alive: a writer that is shutting down reports `ErrClosed`, which is
terminal.

The two sentinels describe different conditions and are never returned for the
same situation, so a caller can always tell "retry this write" apart from "the
writer is dead".

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

### Batch (Atomic Closure)

`Batch` wraps an arbitrary closure in a single atomic transaction. The
framework owns `Begin`/`Commit`/`Rollback`; the closure is the only thing
the caller writes. The closure receives a real `*sql.Tx`, so existing
helpers like `NewSavepoint` compose naturally.

```go
// Atomic, closure-based batch.
func (db *DB) Batch(fn func(*sql.Tx) error) error
```

Semantics:

| `fn` returns | Transaction | `Batch` returns |
|--------------|-------------|-----------------|
| `nil`        | committed   | `nil`           |
| non-nil error| rolled back | the error       |
| panic        | rolled back | `fmt.Errorf("sqlite: batch panic: %v", r)` (panic is recovered) |

`Batch` is mutually exclusive with `Exec`/`ExecContext`/`BeginTx` for its
entire lifetime via the Writer's `txMu`. The lock is released on every exit
path (normal return, error, panic, closed writer).

The caller must **not** invoke `tx.Commit` or `tx.Rollback` from inside
`fn` — `Batch` owns the transaction lifecycle. If `fn` does call them,
`Batch` will surface `sql.ErrTxDone` from its own commit/rollback step.
The caller must also **not** invoke `db.Exec` / `db.ExecContext` /
`db.BeginTx` / `db.Batch` / `db.Flush` from inside `fn` — reentry would
deadlock the writer (the flush goroutine that is running `fn` is also the
only goroutine that drains the task channel, and `txMu` is not
reentrant). Execute statements via the `*sql.Tx` passed to `fn` instead.
No per-statement `sql.Result` is returned; use `BeginTx` when that is
required.

When to prefer `Batch` over `BeginTx`:

| Concern                       | `Batch`                          | `BeginTx`                              |
|------------------------------|----------------------------------|----------------------------------------|
| Channel ops (N statements)   | 1                                | N + 2                                  |
| Per-statement `sql.Result`   | not exposed                     | exposed                                |
| User-facing tx handle         | none (closure-only)             | `*Tx` with `Exec`/`Commit`/`Rollback`  |
| Nested `NewSavepoint`        | works (closure gets `*sql.Tx`)  | works                                  |
| Mutually exclusive with writes| yes                             | yes                                    |

Example — bulk insert:

```go
err := db.Batch(func(tx *sql.Tx) error {
    for _, u := range users {
        if _, err := tx.Exec(
            "INSERT INTO users(name, age) VALUES(?, ?)",
            u.Name, u.Age,
        ); err != nil {
            return err
        }
    }
    return nil
})
```

Example — read then write inside one transaction:

```go
err := db.Batch(func(tx *sql.Tx) error {
    var current int
    if err := tx.QueryRow(
        "SELECT balance FROM accounts WHERE id = ?", id,
    ).Scan(&current); err != nil {
        return err
    }
    if current < amount {
        return errors.New("insufficient balance")
    }
    _, err := tx.Exec(
        "UPDATE accounts SET balance = balance - ? WHERE id = ?",
        amount, id,
    )
    return err
})
```

Example — nested savepoint:

```go
err := db.Batch(func(tx *sql.Tx) error {
    sp, err := sqlite.NewSavepoint(tx)
    if err != nil {
        return err
    }
    if _, err := tx.Exec("INSERT INTO audit (...) VALUES (...)"); err != nil {
        sp.Rollback() // undo only this insert
        return err
    }
    sp.Release()
    return nil
})
```

## Structured Logging

The package uses `log/slog` for structured logging at key points in the writer/buffer pipeline. Configure the slog level and handler as needed.

### Log Schema

| Level | Message | Fields | Description |
|-------|---------|--------|-------------|
| Debug | `task received` | `buffer` | Task received by flush goroutine |
| Debug | `buffered` | `buffer`, `size_limit` | Statement added to buffer |
| Debug | `tx started` | `buffer` | New transaction begun |
| Debug | `commit triggered` | `trigger`, `buffer` | Commit initiated (trigger: size/interval/manual/close) |
| Info | `committed` | `trigger`, `count`, `elapsed` | Successful commit with duration |
| Error | `commit failed` | `trigger`, `buffer`, `err` | Commit failure |
| Debug | `flush requested` | - | Manual Flush() called |
| Warn | `task blocked` | `queued`, `capacity` | Task queue full, task rejected |
| Info | `close signal received` | - | Close signal received |
| Info | `drained on close` | `remaining_buffer` | Final commit on close |
| Debug | `tx committed` | `count` | Buffered Tx committed successfully |
| Debug | `tx rolled back` | `count` | Buffered Tx rolled back |

### Example Log Output

```json
{"time":"2024-01-15T10:30:00Z","level":"DEBUG","msg":"task received","buffer":5}
{"time":"2024-01-15T10:30:00Z","level":"DEBUG","msg":"buffered","buffer":6,"size_limit":100}
{"time":"2024-01-15T10:30:01Z","level":"DEBUG","msg":"commit triggered","trigger":"size","buffer":100}
{"time":"2024-01-15T10:30:01Z","level":"INFO","msg":"committed","trigger":"size","count":100,"elapsed":"1.234ms"}
{"time":"2024-01-15T10:30:05Z","level":"INFO","msg":"close signal received"}
{"time":"2024-01-15T10:30:05Z","level":"INFO","msg":"drained on close","remaining_buffer":0}
```

### Log Levels

- **Debug**: Routine flow (task received, buffered, tx started, commit triggered, flush requested, tx committed/rolled back)
- **Info**: Meaningful business events (committed, close signal received, drained on close)
- **Warn**: Backpressure (task blocked) — surfaced to callers as `ErrQueueFull`
- **Error**: Failures (commit failed)

## Performance Characteristics

- **Writes**: Serialized through a single goroutine; batching reduces transaction overhead
- **Reads**: Concurrent via connection pool; WAL mode allows reads during writes
- **Memory**: In-memory mode shares a single connection; file mode uses separate reader/writer connections
- **Durability**: Writes are committed to WAL on flush interval; call `Commit()` for immediate persistence

## Development

### Makefile Targets

This project uses a Makefile for common development tasks:

```bash
# Run all tests with race detection
make test

# Run the linter
make lint

# Install the pre-commit hook (runs test + lint before each commit)
make hooks-install
```

### Pre-commit Hook

After cloning the repository, install the pre-commit hook to automatically run tests and linting before each commit:

```bash
make hooks-install
```

The hook blocks commits if `make test` or `make lint` fail, ensuring the codebase stays clean and tests pass.

### CI

Pull requests to `main` trigger a GitHub Actions workflow that runs:
1. `go vet` — static analysis
2. `go build` — verify compilation
3. `make test` — run tests with race detection
4. `make lint` — run golangci-lint

The workflow must pass before a PR can be merged.

