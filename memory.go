package sqlite

import (
	"context"
	"database/sql"
	"net/url"
)

// openMemory opens an in-memory SQLite DB.
// For in-memory databases, Writer and Reader share the same *sql.DB connection
// because in-memory SQLite databases are not shared across separate connections.
// Using separate connections would result in each connection seeing a different database.
//
// Optional Option arguments customize the single shared connection (see
// WithReadPRAGMA / WithWritePRAGMA). Per-side replace semantics: when a
// side's helper was invoked, only that side's caller list runs; otherwise
// that side's defaults run. The two lists are concatenated in defaults-
// first, callers-last order so any caller PRAGMA of the same name wins
// regardless of which side it was set on.
func openMemory(ctx context.Context, dsn string, opts []Option) (*DB, error) {
	cfg := applyOptions(opts)

	db, err := openMemoryDB(ctx, dsn, cfg)
	if err != nil {
		return nil, err
	}

	return &DB{
		Writer: NewWriter(db, DefaultBufferConfig()),
		Reader: db,
		ctx:    ctx,
	}, nil
}

// openMemoryDB opens a single-connection *sql.DB for an in-memory SQLite
// database. The single connection constraint is required because
// in-memory databases exist only within their owning connection; a
// second connection would see a completely different (empty) database.
//
// Per-side replace semantics on the shared connection:
//   - Writer slot: cfg.writerPragmas if cfg.writerProvided, otherwise
//     defaultWriterPragmas().
//   - Reader slot: cfg.readerPragmas if cfg.readerProvided, otherwise
//     defaultReaderPragmas().
//
// The two lists are concatenated in defaults-first, callers-last order
// so a caller PRAGMA of the same name overrides the corresponding
// default (last-write-wins at the connector's Connect time).
func openMemoryDB(ctx context.Context, dsn string, cfg *options) (*sql.DB, error) {
	pragmas := []string{}
	if !cfg.writerProvided {
		pragmas = append(pragmas, defaultWriterPragmas()...)
	}
	if !cfg.readerProvided {
		pragmas = append(pragmas, defaultReaderPragmas()...)
	}
	if cfg.writerProvided {
		pragmas = append(pragmas, cfg.writerPragmas...)
	}
	if cfg.readerProvided {
		pragmas = append(pragmas, cfg.readerPragmas...)
	}

	d := sql.OpenDB(newPragmaConnector(memoryDSN(dsn), pragmas))

	// Single connection: in-memory DBs are not shared across connections.
	d.SetMaxOpenConns(1)
	d.SetMaxIdleConns(1)
	d.SetConnMaxLifetime(0)

	if err := d.Ping(); err != nil {
		_ = d.Close()
		return nil, err
	}

	return d, nil
}

// memoryDSN appends mode=memory to the given DSN so go-sqlite3 creates
// an in-memory database. PRAGMAs are applied by the pragmaConnector on
// every Connect — see writerDSN for the rationale.
func memoryDSN(dsn string) string {
	params := url.Values{}
	params.Add("mode", "memory")
	return dsn + "?" + params.Encode()
}
