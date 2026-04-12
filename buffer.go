package sqlite

import (
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
// configuration is usable. It is called automatically by NewWriter;
// you only need to invoke it if you wish to check a config before use.
func (c *BufferConfig) Validate() {
	if c.Size <= 0 {
		c.Size = 100
	}

	if c.FlushInterval <= 0 {
		c.FlushInterval = 500 * time.Millisecond
	}
}
