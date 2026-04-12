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

// openFile opens a persistent SQLite DB with read/write separation.
// The file is created if it does not exist.
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

// physicalMemoryKB returns total physical memory in KiB, or 0 if unavailable.
func physicalMemoryKB() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.SplitN(string(data), "\n", 20) {
		if strings.HasPrefix(line, "MemTotal:") {
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

// writerCacheSizeKB returns the SQLite cache_size for the writer in KiB.
// Targets 5% of physical RAM, clamped to [32 MB, 512 MB], with a 100 MB fallback.
func writerCacheSizeKB() int64 {
	const (
		minKB     = 32 * 1024  // 32 MB
		maxKB     = 512 * 1024 // 512 MB
		defaultKB = 100 * 1024 // 100 MB fallback
	)
	mem := physicalMemoryKB()
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

// readerCacheSizeKB returns the SQLite cache_size for readers in KiB.
// Targets 12.5% of physical RAM, clamped to [64 MB, 2 GB], with a 256 MB fallback.
func readerCacheSizeKB() int64 {
	const (
		minKB     = 64 * 1024       // 64 MB
		maxKB     = 2 * 1024 * 1024 // 2 GB
		defaultKB = 256 * 1024      // 256 MB fallback
	)
	mem := physicalMemoryKB()
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

// mmapSizeBytes returns the mmap_size for readers in bytes.
// Targets 50% of physical RAM, clamped to [256 MB, 128 GB], with a 1 GB fallback.
// Large mmap lets multiple reader connections share the OS page cache (zero-copy on Linux).
func mmapSizeBytes() int64 {
	const (
		minBytes     = 256 << 20      // 256 MB
		maxBytes     = 128 << 30      // 128 GB
		defaultBytes = int64(1 << 30) // 1 GB fallback
	)
	mem := physicalMemoryKB()
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
