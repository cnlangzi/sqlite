package sqlite

import (
	"context"
	"database/sql"
	"time"
)

// openMemory opens an in-memory SQLite DB.
// Writer and Reader share the same *sql.DB connection to avoid transaction isolation issues.
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

func memoryDSN(dsn string) string {
	return dsn + "?mode=memory"
}
