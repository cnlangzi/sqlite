package sqlite

import (
	"fmt"
	"runtime"
)

// Option customizes how Open / New construct the reader and writer SQLite
// connections. Options are passed as variadic arguments:
//
//	db, err := sqlite.Open(ctx, dsn, sqlite.WithReadPRAGMA("..."),
//	    sqlite.WithWritePRAGMA("..."))
//
// Use WithReadPRAGMA / WithWritePRAGMA to override the built-in PRAGMA
// list applied to each connection. These helpers use REPLACE semantics:
// invoking either one on a side (reader or writer) skips that side's
// built-in defaults entirely. Only the caller-supplied PRAGMAs are
// applied. Defaults still apply when the corresponding helper is not
// called.
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
// Replace semantics:
//
//	sqlite.WithReadPRAGMA("secure_delete = ON", "busy_timeout = 5000")
//
// means the reader connection will run ONLY these two PRAGMAs at
// open-time; the package defaults (busy_timeout, temp_store, cache_size,
// threads) are skipped. To keep a built-in while changing another, copy
// it explicitly:
//
//	sqlite.WithReadPRAGMA(
//	    "busy_timeout = 5000",         // keep the built-in busy_timeout
//	    "temp_store = MEMORY",         // keep the built-in temp_store
//	    "secure_delete = ON",          // add the user tweak
//	    sqlite.DefaultReadPragmas()[3:], // adopt any built-in not listed above
//	)
//
// Multiple PRAGMAs in one call:
//
//	sqlite.WithReadPRAGMA("secure_delete = ON", "busy_timeout = 5000")
//
// A pre-built slice can be spread:
//
//	pragmas := []string{"secure_delete = ON"}
//	sqlite.WithReadPRAGMA(pragmas...)
//
// To write multiple logical groups at once, call WithReadPRAGMA several
// times — each invocation appends to the prior PRAGMA list (declaration
// order, last-wins for same-name PRAGMAs):
//
//	security := sqlite.WithReadPRAGMA("secure_delete = ON")
//	perf := sqlite.WithReadPRAGMA("temp_store = MEMORY", "cache_size = -64000")
//	db, err := sqlite.Open(ctx, dsn, security, perf)
//
// Final list = [secure_delete=ON, temp_store=MEMORY, cache_size=-64000].
// This list replaces the built-in defaults; nothing else runs.
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
// with the supplied list. See WithReadPRAGMA for the replace semantics,
// the multi-call composition pattern, and the no-arguments "clear all"
// behavior.
func WithWritePRAGMA(pragmas ...string) Option {
	return func(o *options) {
		o.writerPragmas = append(o.writerPragmas, pragmas...)
		o.writerProvided = true
	}
}

// DefaultReadPragmas returns a snapshot of the built-in PRAGMA spec list
// for the reader connection, evaluated at call time (cache_size and
// threads depend on the current host). Useful as a baseline that callers
// can spread through WithReadPRAGMA to keep the defaults alongside their
// own additions:
//
//	sqlite.WithReadPRAGMA(
//	    "secure_delete = ON",                  // the user tweak
//	    sqlite.DefaultReadPragmas()...,        // adopt the package baseline
//	)
func DefaultReadPragmas() []string {
	return []string{
		"temp_store = MEMORY",
		fmt.Sprintf("cache_size = -%d", readerCacheSizeKB()),
		fmt.Sprintf("threads = %d", runtime.NumCPU()),
	}
}

// DefaultWritePragmas returns a snapshot of the built-in PRAGMA spec list
// for the writer connection. See DefaultReadPragmas for usage notes.
func DefaultWritePragmas() []string {
	return []string{
		"journal_mode = WAL",
		"synchronous = NORMAL",
		"busy_timeout = 5000",
		"temp_store = MEMORY",
		fmt.Sprintf("cache_size = -%d", writerCacheSizeKB()),
	}
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
