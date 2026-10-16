package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// openFile opens a persistent SQLite database file with separate reader and
// writer connections. The file is created with mode 0666 if it does not exist.
// The writer uses WAL journal mode for improved concurrency; the reader is
// configured for read-only, concurrent access.
//
// Optional Option arguments customize the reader and writer connections
// (see WithReadPRAGMA / WithWritePRAGMA).
func openFile(ctx context.Context, dsn string, opts []Option) (*DB, error) {
	cfg := applyOptions(opts)

	_, err := os.Stat(dsn)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err = os.WriteFile(dsn, nil, 0666); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	writerDB, err := openWriterDB(ctx, dsn, cfg)
	if err != nil {
		return nil, err
	}

	readerDB, err := openReaderDB(ctx, dsn, cfg)
	if err != nil {
		_ = writerDB.Close()
		return nil, err
	}

	writer := NewWriter(writerDB, DefaultBufferConfig())

	return &DB{
		Writer: writer,
		Reader: readerDB,
		ctx:    ctx,
	}, nil
}

// openWriterDB opens a single-connection *sql.DB for the writer.
// Only one connection is used because SQLite requires serialized write access;
// a single connection naturally serializes all writes without additional locking.
//
// cfg governs whether the package's built-in writer defaults apply: when
// cfg.writerProvided is true (the caller invoked WithWritePRAGMA at least
// once), only cfg.writerPragmas run; otherwise defaultWriterPragmas() is
// used. Replace semantics — see WithWritePRAGMA for details.
func openWriterDB(ctx context.Context, dsn string, cfg *options) (*sql.DB, error) {
	d, err := sql.Open("sqlite3", writerDSN(dsn))
	if err != nil {
		return nil, err
	}

	// Single connection serializes all writes.
	d.SetMaxOpenConns(1)
	d.SetMaxIdleConns(1)
	d.SetConnMaxLifetime(0)

	if err := d.Ping(); err != nil {
		_ = d.Close()
		return nil, err
	}

	pragmas := cfg.writerPragmas
	if !cfg.writerProvided {
		pragmas = defaultWriterPragmas()
	}
	if err := applyPragmas(ctx, d, pragmas); err != nil {
		_ = d.Close()
		return nil, err
	}

	return d, nil
}

// openReaderDB opens a connection pool *sql.DB optimized for concurrent reads.
// The pool size scales with CPU count to maximize read throughput.
//
// cfg governs whether the package's built-in reader defaults apply: when
// cfg.readerProvided is true, only cfg.readerPragmas run; otherwise
// defaultReaderPragmas() is used. Because the reader uses a connection
// pool, the chosen PRAGMA list is applied to every pooled connection via
// applyPragmasToPool.
func openReaderDB(ctx context.Context, dsn string, cfg *options) (*sql.DB, error) {
	d, err := sql.Open("sqlite3", readerDSN(dsn))
	if err != nil {
		return nil, err
	}

	// Scale reader pool with CPU count.
	conns := max(10, runtime.NumCPU()*2)
	d.SetMaxOpenConns(conns)
	d.SetMaxIdleConns(conns)
	d.SetConnMaxLifetime(0)

	if err := d.Ping(); err != nil {
		_ = d.Close()
		return nil, err
	}

	pragmas := cfg.readerPragmas
	if !cfg.readerProvided {
		pragmas = defaultReaderPragmas()
	}
	if err := applyPragmasToPool(ctx, d, conns, pragmas); err != nil {
		_ = d.Close()
		return nil, err
	}

	return d, nil
}

// writerDSN constructs a SQLite DSN for the writer connection with
// mode=rwc (read-write-create). Only connection-open settings that
// cannot be expressed as a PRAGMA statement live here; every actual
// PRAGMA (journal_mode, synchronous, busy_timeout, temp_store,
// cache_size) is applied via applyPragmas so caller PRAGMAs can
// override the built-ins via last-wins execution order.
func writerDSN(file string) string {
	params := url.Values{}
	params.Add("mode", "rwc")
	return file + "?" + params.Encode()
}

// readerDSN constructs a SQLite DSN for the reader connection. mode=ro
// is the only DSN-level setting because it is the connection's open
// mode (not a PRAGMA); every PRAGMA is applied separately via
// applyPragmas, see writerDSN for the rationale.
func readerDSN(file string) string {
	params := url.Values{}
	params.Add("mode", "ro")
	return file + "?" + params.Encode()
}

// defaultWriterPragmas returns the built-in writer PRAGMA spec list
// evaluated at call time (cache_size depends on host memory). Kept private
// (vs. DefaultWritePragmas) because openFile needs a fresh snapshot each
// time; callers wanting a snapshot should use DefaultWritePragmas directly.
//
// Order matters: caller-supplied PRAGMAs run AFTER these defaults, so a
// same-name caller PRAGMA wins via SQLite's last-wins execution. The
// list also places SQL action before predicate-style PRAGMAs (cache_size)
// because journal/synchronous/lock changes have to commit before the
// cache is sized.
func defaultWriterPragmas() []string {
	return []string{
		"journal_mode = WAL",
		"synchronous = NORMAL",
		"busy_timeout = 5000",
		"temp_store = MEMORY",
		fmt.Sprintf("cache_size = -%d", writerCacheSizeKB()),
	}
}

// defaultReaderPragmas returns the built-in reader PRAGMA spec list. See
// defaultWriterPragmas for the private vs. exported rationale.
func defaultReaderPragmas() []string {
	return []string{
		"busy_timeout = 5000",
		"temp_store = MEMORY",
		fmt.Sprintf("cache_size = -%d", readerCacheSizeKB()),
		fmt.Sprintf("threads = %d", runtime.NumCPU()),
	}
}

// applyPragmas runs the given PRAGMA statements against a single-connection
// *sql.DB (writer path). Calling database/sql.DB.Conn pins one connection
// from the pool; with SetMaxOpenConns(1) on the writer this is always the
// only connection. An empty pragmas slice is a no-op.
func applyPragmas(ctx context.Context, db *sql.DB, pragmas []string) error {
	if len(pragmas) == 0 {
		return nil
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for PRAGMAs: %w", err)
	}
	defer func() { _ = conn.Close() }()

	return execPragmas(ctx, conn, pragmas)
}

// applyPragmasToPool eagerly opens n connections in the pool and applies
// the given PRAGMAs to each, so every pooled connection carries them. With
// SetMaxOpenConns(n) == SetMaxIdleConns(n), database/sql does not grow
// the pool beyond n in steady state, so this approach covers all
// connections the pool will use. An empty pragmas slice is a no-op.
func applyPragmasToPool(ctx context.Context, db *sql.DB, n int, pragmas []string) error {
	if len(pragmas) == 0 {
		return nil
	}
	conns := make([]*sql.Conn, 0, n)
	defer func() {
		for _, c := range conns {
			_ = c.Close() // returns the conn to the pool, does not close the underlying SQLite handle
		}
	}()
	for i := 0; i < n; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("acquire connection %d/%d for PRAGMAs: %w", i+1, n, err)
		}
		conns = append(conns, c)
		if err := execPragmas(ctx, c, pragmas); err != nil {
			return err
		}
	}
	return nil
}

// execPragmas runs each PRAGMA statement via ExecContext on the given
// pinned connection. PRAGMAs are SQL statements so they can be issued as
// regular DDL/DML, but the caller is expected to provide the SQL fragment
// after the "PRAGMA" keyword (e.g. "cache_size = -64000").
func execPragmas(ctx context.Context, conn *sql.Conn, pragmas []string) error {
	for _, p := range pragmas {
		if _, err := conn.ExecContext(ctx, "PRAGMA "+p); err != nil {
			return fmt.Errorf("apply PRAGMA %q: %w", p, err)
		}
	}
	return nil
}

// availableMemoryKB returns the currently available memory of the system in KiB
// by reading MemAvailable from /proc/meminfo on Linux. MemAvailable estimates
// how much memory can be allocated for new processes without swapping, accounting
// for reclaimable page cache and slabs. Returns 0 if the value cannot be
// determined (e.g., on macOS or if /proc is inaccessible), which triggers
// fallback defaults.
func availableMemoryKB() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.SplitN(string(data), "\n", 20) {
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					return kb
				}
			}
		}
	}
	return 0
}

// writerCacheSizeKB returns the SQLite cache_size in KiB for writer connections.
// It targets 5% of available memory, clamped to the range [32 MB, 512 MB],
// with a 100 MB fallback when available memory cannot be determined.
func writerCacheSizeKB() int64 {
	const (
		minKB     = 32 * 1024  // 32 MB
		maxKB     = 512 * 1024 // 512 MB
		defaultKB = 100 * 1024 // 100 MB fallback
	)
	mem := availableMemoryKB()
	if mem == 0 {
		return defaultKB
	}
	target := mem / 20 // 5%
	if target < minKB {
		return minKB
	}
	if target > maxKB {
		return maxKB
	}
	return target
}

// readerCacheSizeKB returns the SQLite cache_size in KiB for reader connections.
// It targets 12.5% of available memory, clamped to the range [64 MB, 2 GB],
// with a 256 MB fallback when available memory cannot be determined.
func readerCacheSizeKB() int64 {
	const (
		minKB     = 64 * 1024       // 64 MB
		maxKB     = 2 * 1024 * 1024 // 2 GB
		defaultKB = 256 * 1024      // 256 MB fallback
	)
	mem := availableMemoryKB()
	if mem == 0 {
		return defaultKB
	}
	target := mem / 8 // 12.5%
	if target < minKB {
		return minKB
	}
	if target > maxKB {
		return maxKB
	}
	return target
}
