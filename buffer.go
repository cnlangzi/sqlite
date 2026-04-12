package sqlite

import (
	"time"
)

// BufferConfig holds batch writer configuration.
type BufferConfig struct {
	// Size is the number of statements to buffer before flushing.
	// Default: 100
	Size int

	// FlushInterval is the maximum time to wait before flushing.
	// Default: 100ms
	FlushInterval time.Duration
}

// Validate sets defaults and validates the configuration.
func (c *BufferConfig) Validate() {
	if c.Size <= 0 {
		c.Size = 100
	}

	if c.FlushInterval <= 0 {
		c.FlushInterval = 500 * time.Millisecond
	}
}
