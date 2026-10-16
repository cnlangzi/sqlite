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
// WithReadPRAGMA / WithWritePRAGMA). Replace semantics apply per-side:
// the writer slot uses cfg.writerPragmas if cfg.writerProvided, otherwise
// defaultWriterPragmas(); the reader slot behaves analogously. The two
// PRAGMA lists are concatenated and applied to the single shared
// connection in declaration order.
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
// cfg governs the PRAGMA list applied to the single shared connection.
// Per-side replace semantics: the writer slot uses cfg.writerPragmas
// when cfg.writerProvided is true, otherwise defaultWriterPragmas();
// the reader slot behaves analogously. The two lists are concatenated
// (writer first, reader after) since order matters for same-name
// PRAGMAs (last-wins).
func openMemoryDB(ctx context.Context, dsn string, cfg *options) (*sql.DB, error) {
	d, err := sql.Open("sqlite3", memoryDSN(dsn))
	if err != nil {
		return nil, err
	}

	// Single connection: in-memory DBs are not shared across connections.
	d.SetMaxOpenConns(1)
	d.SetMaxIdleConns(1)
	d.SetConnMaxLifetime(0)

	if err := d.Ping(); err != nil {
		_ = d.Close()
		return nil, err
	}

	var pragmas []string
	if cfg.writerProvided {
		pragmas = append(pragmas, cfg.writerPragmas...)
	} else {
		pragmas = append(pragmas, defaultWriterPragmas()...)
	}
	if cfg.readerProvided {
		pragmas = append(pragmas, cfg.readerPragmas...)
	} else {
		pragmas = append(pragmas, defaultReaderPragmas()...)
	}
	if err := applyPragmas(ctx, d, pragmas); err != nil {
		_ = d.Close()
		return nil, err
	}

	return d, nil
}

// memoryDSN appends mode=memory to the given DSN so go-sqlite3 creates
// an in-memory database. PRAGMAs are applied separately via applyPragmas
// after the connection opens — see writerDSN for the rationale.
func memoryDSN(dsn string) string {
	params := url.Values{}
	params.Add("mode", "memory")
	return dsn + "?" + params.Encode()
}
