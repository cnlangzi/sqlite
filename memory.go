package sqlite

import (
	"context"
	"database/sql"
	"time"
)

// openMemory opens an in-memory SQLite DB.
// For in-memory databases, Writer and Reader share the same *sql.DB connection
// because in-memory SQLite databases are not shared across separate connections.
// Using separate connections would result in each connection seeing a different database.
func openMemory(ctx context.Context, dsn string) (*DB, error) {
	db, err := openMemoryDB(dsn)
	if err != nil {
		return nil, err
	}

	return &DB{
		Writer: NewWriter(db, BufferConfig{Size: 100, FlushInterval: 100 * time.Millisecond}),
		Reader: db,
		ctx:    ctx,
	}, nil
}

// openMemoryDB opens a single-connection *sql.DB for an in-memory SQLite database.
// The single connection constraint is required because in-memory databases
// exist only within their owning connection; a second connection would see
// a completely different (empty) database.
func openMemoryDB(dsn string) (*sql.DB, error) {
	d, err := sql.Open("sqlite3", memoryDSN(dsn))
	if err != nil {
		return nil, err
	}

	// Single connection: in-memory DBs are not shared across connections.
	d.SetMaxOpenConns(1)
	d.SetMaxIdleConns(1)
	d.SetConnMaxLifetime(0)

	if err := d.Ping(); err != nil {
		return nil, err
	}

	return d, nil
}

// memoryDSN appends the SQLite mode=memory parameter to the given DSN,
// ensuring the database is created as an in-memory database rather than
// a temporary disk-based database.
func memoryDSN(dsn string) string {
	return dsn + "?mode=memory"
}
