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
// cfg governs which PRAGMA list runs at every Connect (per
// newPragmaConnector): cfg.writerPragmas if cfg.writerProvided is true,
// otherwise defaultWriterPragmas(). Replace semantics — see
// WithWritePRAGMA for details.
func openWriterDB(ctx context.Context, dsn string, cfg *options) (*sql.DB, error) {
	pragmas := cfg.writerPragmas
	if !cfg.writerProvided {
		pragmas = defaultWriterPragmas()
	}
	d := sql.OpenDB(newPragmaConnector(writerDSN(dsn), pragmas))

	// Single connection serializes all writes.
	d.SetMaxOpenConns(1)
	d.SetMaxIdleConns(1)
	d.SetConnMaxLifetime(0)

	if err := d.Ping(); err != nil {
		_ = d.Close()
		return nil, err
	}

	return d, nil
}

// openReaderDB opens a connection pool *sql.DB optimized for concurrent reads.
// The pool size scales with CPU count to maximize read throughput.
//
// cfg governs which PRAGMA list runs at every Connect (per
// newPragmaConnector): cfg.readerPragmas if cfg.readerProvided is true,
// otherwise defaultReaderPragmas(). The pragmaConnector guarantees every
// pool connection — initial and replacement — gets the chosen PRAGMAs.
func openReaderDB(ctx context.Context, dsn string, cfg *options) (*sql.DB, error) {
	pragmas := cfg.readerPragmas
	if !cfg.readerProvided {
		pragmas = defaultReaderPragmas()
	}
	d := sql.OpenDB(newPragmaConnector(readerDSN(dsn), pragmas))

	// Scale reader pool with CPU count.
	conns := max(10, runtime.NumCPU()*2)
	d.SetMaxOpenConns(conns)
	d.SetMaxIdleConns(conns)
	d.SetConnMaxLifetime(0)

	if err := d.Ping(); err != nil {
		_ = d.Close()
		return nil, err
	}

	return d, nil
}

// writerDSN constructs a SQLite DSN for the writer connection with
// mode=rwc (read-write-create). Only connection-open settings that
// cannot be expressed as a PRAGMA statement live here; every actual
// PRAGMA (journal_mode, synchronous, busy_timeout, temp_store,
// cache_size) is applied via the pragmaConnector on every Connect.
func writerDSN(file string) string {
	params := url.Values{}
	params.Add("mode", "rwc")
	return file + "?" + params.Encode()
}

// readerDSN constructs a SQLite DSN for the reader connection.
// mode=ro is the only DSN-level setting because it is the connection's
// open mode (not a PRAGMA); every PRAGMA is applied via the
// pragmaConnector, see writerDSN for the rationale.
func readerDSN(file string) string {
	params := url.Values{}
	params.Add("mode", "ro")
	return file + "?" + params.Encode()
}

// defaultWriterPragmas returns the built-in writer PRAGMA spec list
// evaluated at call time (cache_size depends on host memory). The list
// is the single source of truth that DefaultWritePragmas publicly
// surfaces; both openFile and call sites read from this helper.
//
// Ordering matters: action-style PRAGMAs (journal_mode, synchronous,
// busy_timeout) run before predicate-style PRAGMAs (cache_size) so the
// connection state is stable before the cache is sized.
func defaultWriterPragmas() []string {
	return []string{
		"journal_mode = WAL",
		"synchronous = NORMAL",
		"busy_timeout = 5000",
		"temp_store = MEMORY",
		fmt.Sprintf("cache_size = -%d", writerCacheSizeKB()),
	}
}

// defaultReaderPragmas returns the built-in reader PRAGMA spec list.
// DefaultReadPragmas publicly surfaces the same content.
func defaultReaderPragmas() []string {
	return []string{
		"busy_timeout = 5000",
		"temp_store = MEMORY",
		fmt.Sprintf("cache_size = -%d", readerCacheSizeKB()),
		fmt.Sprintf("threads = %d", runtime.NumCPU()),
	}
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
