package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var ErrTxNotready = errors.New("sqlite: transaction not ready")

// DB wraps SQLite database with read/write separation.
// Writer uses BatchWriter for buffered batch writes, Reader for reads.
type DB struct {
	Writer *Writer
	Reader *sql.DB
	ctx    context.Context
}

// isInMemory reports whether the given DSN refers to an in-memory database.
func isInMemory(dsn string) bool {
	return strings.HasPrefix(dsn, ":memory:")
}

// New creates a SQLite DB from a DSN, automatically detecting in-memory or file mode.
// Use ":memory:" for an in-memory database, or a file path for a persistent database.
func New(ctx context.Context, dsn string) (*DB, error) {
	if dsn == "" {
		dsn = ":memory:"
	}

	if isInMemory(dsn) {
		return openMemory(ctx, dsn)
	}

	return openFile(ctx, dsn)
}

// Open is an alias for New.
func Open(ctx context.Context, dsn string) (*DB, error) {
	return New(ctx, dsn)
}

// Close closes both Writer and Reader connections.
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

// Exec executes a write operation on Writer.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.Writer.Exec(query, args...)
}

// ExecContext executes a write operation on Writer with context.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.Writer.ExecContext(ctx, query, args...)
}

// Query executes a read operation on Reader.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.Reader.Query(query, args...)
}

// QueryContext executes a read operation on Reader with context.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.Reader.QueryContext(ctx, query, args...)
}

// QueryRow executes a read operation on Reader and returns a single row.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.Reader.QueryRow(query, args...)
}

// QueryRowContext executes a read operation on Reader with context and returns a single row.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.Reader.QueryRowContext(ctx, query, args...)
}
