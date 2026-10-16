package sqlite

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/mattn/go-sqlite3"
)

// Option customizes how Open / New construct the reader and writer SQLite
// connections. Options are passed as variadic arguments:
//
//	db, err := sqlite.Open(ctx, dsn, sqlite.WithReadPRAGMA("..."),
//	    sqlite.WithWritePRAGMA("..."))
//
// WithReadPRAGMA / WithWritePRAGMA use REPLACE semantics per-side: invoking
// either one on a side skips that side's built-in defaults entirely. Only
// the caller-supplied PRAGMAs are applied. Defaults still apply when the
// corresponding helper is not called.
type Option func(*options)

// options accumulates the configuration produced by each Option. It is
// unexported because callers should only mutate it through the public
// WithXXX helpers, never directly.
type options struct {
	// readerPragmas are the caller-supplied PRAGMA specs for the reader
	// connection. readerProvided gates whether they apply at all: when
	// false, defaultReaderPragmas() is used; when true, the caller
	// list replaces the defaults entirely.
	readerPragmas  []string
	readerProvided bool

	// writerPragmas are the caller-supplied PRAGMA specs for the writer
	// connection. See readerPragmas for the provided/replace semantics.
	writerPragmas  []string
	writerProvided bool
}

// WithReadPRAGMA replaces the package's built-in reader-side PRAGMAs with
// the supplied list. Each argument is a single PRAGMA spec in the form
// accepted by SQLite (e.g. "name = value").
//
// Replace semantics: the reader connection will run ONLY the supplied
// PRAGMAs — the package defaults are skipped. To keep a default while
// adding your own, spread DefaultReadPragmas into the call:
//
//	sqlite.WithReadPRAGMA(
//	    "secure_delete = ON",                 // user addition
//	    sqlite.DefaultReadPragmas()...,        // adopt the package baseline
//	)
//
// Calling WithReadPRAGMA() with no arguments is valid: it explicitly
// clears the reader connection of all PRAGMAs (both built-ins and
// caller-supplied).
func WithReadPRAGMA(pragmas ...string) Option {
	return func(o *options) {
		o.readerPragmas = append(o.readerPragmas, pragmas...)
		o.readerProvided = true
	}
}

// WithWritePRAGMA replaces the package's built-in writer-side PRAGMAs
// with the supplied list. See WithReadPRAGMA for the replace semantics
// and the no-arguments "clear all" behavior.
func WithWritePRAGMA(pragmas ...string) Option {
	return func(o *options) {
		o.writerPragmas = append(o.writerPragmas, pragmas...)
		o.writerProvided = true
	}
}

// DefaultReadPragmas returns a snapshot of the built-in PRAGMA spec list
// for the reader connection, evaluated at call time (cache_size and
// threads depend on the current host). The list is exactly what Open
// applies when no WithReadPRAGMA was given. Useful as a baseline that
// callers can spread through WithReadPRAGMA to recover the defaults
// after replacement.
func DefaultReadPragmas() []string {
	return defaultReaderPragmas()
}

// DefaultWritePragmas returns a snapshot of the built-in PRAGMA spec list
// for the writer connection. See DefaultReadPragmas for usage notes.
func DefaultWritePragmas() []string {
	return defaultWriterPragmas()
}

// applyOptions runs each option against a freshly allocated options
// struct and returns it. nil options are silently ignored, so callers
// do not have to guard against accidental nil entries.
func applyOptions(opts []Option) *options {
	o := &options{}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return o
}

// pragmaConnector wraps mattn's SQLiteDriver to apply a list of PRAGMA
// statements to every newly opened connection, including connections
// that database/sql creates after discarding an idle one (ErrBadConn
// recovery, idle-conn eviction, etc.). Without this wrapper, only the
// conns created during Open would carry the PRAGMAs, so a later
// replacement connection would silently regress to driver defaults.
//
// The PRAGMAs are run after mattn finishes parsing the DSN, so DSN-
// level settings (mode=rwc/ro) take effect first; the caller PRAGMAs
// applied here then override anything the driver or DSN defaults set.
type pragmaConnector struct {
	driver  *sqlite3.SQLiteDriver
	dsn     string
	pragmas []string
}

// newPragmaConnector builds a Connector that opens the given DSN and
// applies the supplied PRAGMA list to every newly opened connection.
// The pragma slice is copied so subsequent caller mutation cannot
// affect future Connect calls. A nil/empty pragmas slice still produces
// a working Connector (Connect will simply apply no PRAGMAs).
func newPragmaConnector(dsn string, pragmas []string) driver.Connector {
	return &pragmaConnector{
		driver:  &sqlite3.SQLiteDriver{},
		dsn:     dsn,
		pragmas: append([]string(nil), pragmas...),
	}
}

// Connect opens a new SQLite connection via the wrapped driver and
// applies the configured PRAGMA list in declaration order. Errors close
// the partially-opened connection so the caller does not have to clean
// up. nil/empty pragma lists fast-path: the connection is returned as
// soon as the driver opens it.
func (c *pragmaConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	if len(c.pragmas) == 0 {
		return conn, nil
	}
	exec, ok := conn.(driver.ExecerContext)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("pragmaConnector: driver.Conn does not implement driver.ExecerContext")
	}
	for _, p := range c.pragmas {
		if _, err := exec.ExecContext(ctx, "PRAGMA "+p, nil); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("pragmaConnector: apply PRAGMA %q: %w", p, err)
		}
	}
	return conn, nil
}

// Driver returns the underlying mattn SQLiteDriver. Required by the
// driver.Connector interface.
func (c *pragmaConnector) Driver() driver.Driver {
	return c.driver
}
