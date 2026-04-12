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
	"time"
)

// openFile opens a persistent SQLite database file with separate reader and
// writer connections. The file is created with mode 0666 if it does not exist.
// The writer uses WAL journal mode for improved concurrency; the reader is
// configured for read-only, concurrent access.
func openFile(ctx context.Context, dsn string) (*DB, error) {
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

	writerDB, err := openWriterDB(dsn)
	if err != nil {
		return nil, err
	}

	readerDB, err := openReaderDB(dsn)
	if err != nil {
		return nil, err
	}

	return &DB{
		Writer: NewWriter(writerDB, BufferConfig{Size: 100, FlushInterval: 100 * time.Millisecond}),
		Reader: readerDB,
		ctx:    ctx,
	}, nil
}

// openWriterDB opens a single-connection *sql.DB for the writer.
// Only one connection is used because SQLite requires serialized write access;
// a single connection naturally serializes all writes without additional locking.
func openWriterDB(dsn string) (*sql.DB, error) {
	d, err := sql.Open("sqlite3", writerDSN(dsn))
	if err != nil {
		return nil, err
	}

	// Single connection serializes all writes.
	d.SetMaxOpenConns(1)
	d.SetMaxIdleConns(1)
	d.SetConnMaxLifetime(0)

	if err := d.Ping(); err != nil {
		return nil, err
	}

	return d, nil
}

// openReaderDB opens a connection pool *sql.DB optimized for concurrent reads.
// The pool size scales with CPU count to maximize read throughput.
func openReaderDB(dsn string) (*sql.DB, error) {
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
		return nil, err
	}

	return d, nil
}

// writerDSN constructs a SQLite DSN for the writer connection with
// mode=rwc (read-write-create), WAL journal mode, and pragmas tuned for
// write performance and reliability. The negative cache_size value indicates
// KiB rather than pages.
func writerDSN(file string) string {
	params := url.Values{}
	params.Add("mode", "rwc")
	params.Add("_journal_mode", "WAL")
	params.Add("_synchronous", "NORMAL")
	params.Add("_busy_timeout", "5000")
	params.Add("_pragma", "temp_store(MEMORY)")
	params.Add("_pragma", fmt.Sprintf("cache_size(-%d)", writerCacheSizeKB()))
	return file + "?" + params.Encode()
}

// readerDSN constructs a SQLite DSN for the reader connection with mode=ro
// (read-only) and pragmas tuned for read performance, including private cache
// to avoid cache synchronization overhead, mmap for zero-copy reads, and
// thread count matching CPU count for parallel queries.
func readerDSN(file string) string {
	params := url.Values{}
	params.Add("mode", "ro")
	params.Add("cache", "private")
	params.Add("_busy_timeout", "5000")
	params.Add("_query_only", "true")
	params.Add("_pragma", fmt.Sprintf("mmap_size(%d)", mmapSizeBytes()))
	params.Add("_pragma", "temp_store(MEMORY)")
	params.Add("_pragma", fmt.Sprintf("cache_size(-%d)", readerCacheSizeKB()))
	params.Add("_pragma", fmt.Sprintf("threads(%d)", runtime.NumCPU()))
	return file + "?" + params.Encode()
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

// mmapSizeBytes returns the SQLite mmap_size in bytes for reader connections.
// It targets 50% of available memory, clamped to the range [256 MB, 128 GB],
// with a 1 GB fallback when available memory cannot be determined. Large mmap
// allows multiple reader connections to share the OS page cache (zero-copy on Linux).
func mmapSizeBytes() int64 {
	const (
		minBytes     = 256 << 20      // 256 MB
		maxBytes     = 128 << 30      // 128 GB
		defaultBytes = int64(1 << 30) // 1 GB fallback
	)
	mem := availableMemoryKB()
	if mem == 0 {
		return defaultBytes
	}
	target := mem * 512 // KiB * 512 = 50% of RAM in bytes
	if target < minBytes {
		return minBytes
	}
	if target > maxBytes {
		return maxBytes
	}
	return target
}
