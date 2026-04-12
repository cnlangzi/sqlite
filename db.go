package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var ErrTxNotready = errors.New("sqlite: transaction not ready")

// DB wraps a SQLite database with logical read/write separation.
// All writes go through the buffered Writer; all reads go directly to the Reader.
// This separation allows concurrent reads while writes are being buffered and
// committed by the background flush goroutine.
type DB struct {
	Writer *Writer
	Reader *sql.DB
	ctx    context.Context
}

// isInMemory reports whether the given DSN refers to an in-memory database.
// Returns true for DSNs starting with ":memory:".
func isInMemory(dsn string) bool {
	return strings.HasPrefix(dsn, ":memory:")
}

// New opens a SQLite database from the given DSN. If DSN is empty, it
// defaults to ":memory:" (an in-memory database). For file paths, the file
// is created if it does not exist. This is an alias for Open; prefer Open
// for clarity in application code.
func New(ctx context.Context, dsn string) (*DB, error) {
	if dsn == "" {
		dsn = ":memory:"
	}

	if isInMemory(dsn) {
		return openMemory(ctx, dsn)
	}

	return openFile(ctx, dsn)
}

// Open opens a SQLite database from the given DSN. If DSN is empty, it
// defaults to ":memory:" (an in-memory database). For file paths, the file
// is created if it does not exist. This is the preferred constructor.
func Open(ctx context.Context, dsn string) (*DB, error) {
	return New(ctx, dsn)
}

// Close shuts down the writer flush goroutine (committing any pending writes)
// and closes both the Writer and Reader database connections. Always call
// Close when you are done with the DB to avoid leaking goroutines or connections.
func (db *DB) Close() error {
	var errs []error

	if err := db.Writer.Close(); err != nil {
		errs = append(errs, err)
	}

	if err := db.Reader.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Exec delegates to Writer.Exec, executing a write statement (INSERT,
// UPDATE, DELETE, CREATE TABLE, etc.) asynchronously with buffering.
// See Writer.Exec for details on buffering behavior.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.Writer.Exec(query, args...)
}

// ExecContext delegates to Writer.ExecContext, executing a write statement
// with context cancellation support. See Writer.ExecContext for details.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.Writer.ExecContext(ctx, query, args...)
}

// Query delegates to Reader.Query, executing a read-only query on the
// dedicated reader connection pool. Returns *sql.Rows that must be closed.
// Reads are served concurrently with writes and do not block the Writer.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.Reader.Query(query, args...)
}

// QueryContext delegates to Reader.QueryContext, executing a read-only query
// with context cancellation support. Returns *sql.Rows that must be closed.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.Reader.QueryContext(ctx, query, args...)
}

// QueryRow delegates to Reader.QueryRow, executing a read-only query that
// returns at most one row. This is more efficient than Query when you only
// need a single row. Returns *sql.Row (not an error) even if no row is found.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.Reader.QueryRow(query, args...)
}

// QueryRowContext delegates to Reader.QueryRowContext, executing a read-only
// query with context cancellation support and returning at most one row.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.Reader.QueryRowContext(ctx, query, args...)
}
