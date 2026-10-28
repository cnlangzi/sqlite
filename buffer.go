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

	// QueueCapacity is the number of tasks that may wait in the writer's
	// task queue for the flush goroutine. When the queue is full, further
	// tasks are rejected with ErrQueueFull (transient back-pressure)
	// instead of blocking the caller. Default: 100.
	QueueCapacity int
}

// DefaultBufferConfig returns a BufferConfig with defaults applied,
// reading from the SQLITE_BUFFER_SIZE, SQLITE_BUFFER_FLUSH_INTERVAL and
// SQLITE_BUFFER_QUEUE_CAPACITY env vars when set. Call this at Open time to
// respect environment configuration.
//
// An env value that cannot be parsed, or that is not positive, is ignored
// and the corresponding default is kept.
func DefaultBufferConfig() BufferConfig {
	cfg := BufferConfig{Size: 100, FlushInterval: 100 * time.Millisecond, QueueCapacity: 100}

	for _, e := range []struct {
		env  string
		dest *int
	}{
		{"SQLITE_BUFFER_SIZE", &cfg.Size},
		{"SQLITE_BUFFER_QUEUE_CAPACITY", &cfg.QueueCapacity},
	} {
		if v := os.Getenv(e.env); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				*e.dest = n
			}
		}
	}

	if v := os.Getenv("SQLITE_BUFFER_FLUSH_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.FlushInterval = d
		}
	}

	return cfg
}

// Validate sets default values for any zero fields and ensures the
// configuration is usable. It is called automatically by NewWriter;
// you only need to invoke it if you wish to check a config before use.
func (c *BufferConfig) Validate() {
	if c.Size <= 0 {
		c.Size = 100
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 100 * time.Millisecond
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 100
	}
}
