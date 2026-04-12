package sqlite

import (
	"os"
	"strconv"
	"time"
)

// BufferConfig holds batch writer configuration.
// The writer buffers incoming statements and flushes them in batches
// to reduce transaction overhead and improve throughput.
type BufferConfig struct {
	// Size is the number of statements to buffer before triggering an
	// automatic flush. A transaction is committed when this threshold
	// is reached. Default: 100.
	Size int

	// FlushInterval is the maximum duration between commits. The writer
	// flushes pending statements if this interval elapses since the last
	// commit, even if Size has not been reached. Default: 100ms.
	FlushInterval time.Duration
}

// Validate sets default values for any zero fields and ensures the
// configuration is usable. Environment variables are consulted when a field
// is zero: SQLITE_BUFFER_SIZE and SQLITE_BUFFER_FLUSH_INTERVAL.
// It is called automatically by NewWriter; you only need to invoke it if
// you wish to check a config before use.
func (c *BufferConfig) Validate() {
	if c.Size <= 0 {
		if v := os.Getenv("SQLITE_BUFFER_SIZE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				c.Size = n
			}
		}
	}
	if c.Size <= 0 {
		c.Size = 100
	}

	if c.FlushInterval <= 0 {
		if v := os.Getenv("SQLITE_BUFFER_FLUSH_INTERVAL"); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				c.FlushInterval = d
			}
		}
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 500 * time.Millisecond
	}
}
